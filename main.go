package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	resty "github.com/go-resty/resty/v2"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	cron "github.com/robfig/cron/v3"

	"mytruyen-worker/task"
)

func addLog(client *resty.Client) {
	client.OnAfterResponse(func(c *resty.Client, r *resty.Response) error {
		log.Printf("%s %s - %d", r.Request.Method, r.Request.URL, r.StatusCode())
		return nil
	})
}

func getMeTruyenAuthToken(client *resty.Client) (string, error) {
	var MytruyenLoginPayload = map[string]any{
		"email":       os.Getenv("METRUYEN_EMAIL"),
		"password":    os.Getenv("METRUYEN_PASSWORD"),
		"remember":    1,
		"device_name": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
	}

	var result struct {
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}

	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(MytruyenLoginPayload).
		SetResult(&result).
		Post("/auth/login")
	if err != nil {
		return "", fmt.Errorf("Login request failed: %v", err)
	}
	if resp.IsError() {
		return "", fmt.Errorf("Login request failed with status: %s", resp.Status())
	}

	return result.Data.Token, nil
}

func getMyTruyenAuthToken(client *resty.Client) (string, error) {
	var MytruyenLoginPayload = map[string]any{
		"email":    os.Getenv("MYTRUYEN_EMAIL"),
		"password": os.Getenv("MYTRUYEN_PASSWORD"),
	}

	var result struct {
		Data struct {
			Token string `json:"access_token"`
		} `json:"data"`
	}

	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(MytruyenLoginPayload).
		SetResult(&result).
		Post("/auth/login")
	if err != nil {
		return "", fmt.Errorf("Login request failed: %v", err)
	}
	if resp.IsError() {
		return "", fmt.Errorf("Login request failed with status: %s", resp.Status())
	}

	return result.Data.Token, nil
}

func consumer(ctx context.Context) error {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Error loading .env file")
	}

	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to open a channel:", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		os.Getenv("RABBITMQ_QUEUE_CRAWL"),
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatal("Failed to declare queue:", err)
	}

	couroutineCount, err := strconv.Atoi(os.Getenv("CRAWL_COURUTINE_COUNT"))
	if err != nil {
		log.Fatal("Failed to parse CRAWL_COURUTINE_COUNT:", err)
	}
	log.Printf("Crawl couroutine count: %d", couroutineCount)
	err = ch.Qos(couroutineCount, 0, false)
	if err != nil {
		log.Fatal("Failed to set QoS:", err)
	}

	msgs, err := ch.Consume(
		q.Name,            // queue
		"mytruyen-worker", // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		log.Fatal("Failed to register a consumer:", err)
	}

	MeTruyencvClient := resty.New()
	MeTruyencvClient.SetBaseURL(os.Getenv("METRUYEN_BACKEND"))
	addLog(MeTruyencvClient)
	MeTruyencvToken, err := getMeTruyenAuthToken(MeTruyencvClient)
	if err != nil {
		log.Fatalf("Failed to get MeTruyen auth token: %v", err)
	}
	fmt.Printf("MeTruyencv auth token: %s\n", MeTruyencvToken)
	MeTruyencvClient.SetAuthToken(MeTruyencvToken)

	MyTruyenClient := resty.New()
	MyTruyenClient.SetBaseURL(os.Getenv("MYTRUYEN_BACKEND"))
	addLog(MyTruyenClient)
	MyTruyenToken, err := getMyTruyenAuthToken(MyTruyenClient)
	if err != nil {
		log.Fatalf("Failed to get MyTruyen auth token: %v", err)
	}
	fmt.Printf("MyTruyen auth token: %s\n", MyTruyenToken)
	MyTruyenClient.SetAuthToken(MyTruyenToken)

	c := cron.New()
	_, err = c.AddFunc("*/1 * * * *", func() {
		log.Println("Running scheduled task: CheckNewChaptersHandler")
		success := task.CheckNewChaptersHandler(MeTruyencvClient, MyTruyenClient)
		if success {
			log.Println("Scheduled task CheckNewChaptersHandler completed successfully.")
		} else {
			log.Println("Scheduled task CheckNewChaptersHandler failed.")
		}
	})
	if err != nil {
		log.Fatalf("Failed to schedule CheckNewChaptersHandler: %v", err)
	}
	c.Start()
	defer c.Stop()

	closeChan := make(chan *amqp.Error, 1)
	conn.NotifyClose(closeChan)

	var wg sync.WaitGroup

	for i := 0; i < couroutineCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for d := range msgs {
				var crawlRequest struct {
					Type   string `json:"type"`
					BookID int    `json:"book_id"`
				}
				err := json.Unmarshal(d.Body, &crawlRequest)
				if err != nil {
					log.Printf("Error parsing message: %v", err)
					_ = d.Nack(false, false)
					continue
				}

				log.Printf("[Worker %d] Received a crawl request: Type=%s, BookID=%d",
					workerID,
					crawlRequest.Type,
					crawlRequest.BookID,
				)

				var success bool
				switch crawlRequest.Type {
				case "crawl_genres":
					success = task.GenresHandler(MeTruyencvClient, MyTruyenClient)
					if !success {
						log.Printf("[Worker %d] Failed to crawl genres", workerID)
					} else {
						log.Printf("[Worker %d] Successfully crawled genres", workerID)
					}

				case "crawl_tags":
					success = task.TagsHandler(MeTruyencvClient, MyTruyenClient)
					if !success {
						log.Printf("[Worker %d] Failed to crawl tags", workerID)
					} else {
						log.Printf("[Worker %d] Successfully crawled tags", workerID)
					}

				case "crawl_book_statuses":
					success = task.BookStatusHandler(MeTruyencvClient, MyTruyenClient)
					if !success {
						log.Printf("[Worker %d] Failed to crawl book statuses", workerID)
					} else {
						log.Printf("[Worker %d] Successfully crawled book statuses", workerID)
					}

				case "crawl_all_books":
					success = task.AllBookHandler(MeTruyencvClient, MyTruyenClient, q.Name)
					if !success {
						log.Printf("[Worker %d] Failed to crawl all books", workerID)
					} else {
						log.Printf("[Worker %d] Successfully enqueued all books crawling", workerID)
					}

				case "crawl_book":
					success = task.BookHandler(MeTruyencvClient, MyTruyenClient, crawlRequest.BookID)
					if !success {
						log.Printf("[Worker %d] Failed to crawl book ID %d", workerID, crawlRequest.BookID)
					} else {
						log.Printf("[Worker %d] Successfully crawled book ID %d", workerID, crawlRequest.BookID)
					}

				case "crawl_chapters":
					success = task.ChaptersHandler(MeTruyencvClient, MyTruyenClient, crawlRequest.BookID)
					if !success {
						log.Printf("[Worker %d] Failed to crawl chapters", workerID)
					} else {
						log.Printf("[Worker %d] Successfully crawled chapters", workerID)
					}

				// case "check_new_chapters":
				// 	success = task.CheckNewChaptersHandler(MeTruyencvClient, MyTruyenClient, ch, q.Name)
				// 	if !success {
				// 		log.Printf("[Worker %d] Failed to check new chapters", workerID)
				// 	} else {
				// 		log.Printf("[Worker %d] Successfully checked new chapters", workerID)
				// 	}

				// case "refresh_mytruyen_token":
				// 	token, err := getMyTruyenAuthToken(MyTruyenClient)
				// 	if err != nil {
				// 		log.Printf("[Worker %d] Failed to refresh MyTruyen auth token: %v", workerID, err)
				// 		success = false
				// 	} else {
				// 		log.Printf("[Worker %d] Successfully refreshed MyTruyen auth token.", workerID)
				// 		MyTruyenClient.SetAuthToken(token)
				// 		success = true
				// 	}

				// case "add_all_books_to_meili":
				// 	success = task.MeiliHandler(MyTruyencvClient, MeiliClient)
				// 	if !success {
				// 		log.Printf("[Worker %d] Failed to add all books to Meilisearch", workerID)
				// 	} else {
				// 		log.Printf("[Worker %d] Successfully added all books to Meilisearch", workerID)
				// 	}

				default:
					log.Printf("[Worker %d] Unknown crawl request type: %s", workerID, crawlRequest.Type)
					success = false
				}

				if success {
					if err := d.Ack(false); err != nil {
						log.Printf("[Worker %d] Ack failed: %v", workerID, err)
					}
				} else {
					if err := d.Nack(false, true); err != nil {
						log.Printf("[Worker %d] Nack failed: %v", workerID, err)
					}
				}
			}
		}(i)
	}
	select {
	case errClose := <-closeChan:
		log.Printf("RabbitMQ closed: %v", errClose)
		wg.Wait()
		return fmt.Errorf("rabbitmq disconnected")

	case <-ctx.Done():
		log.Println("Shutdown requested")

		ch.Cancel("mytruyen-worker", false)

		wg.Wait()

		ch.Close()

		return nil
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)

		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Exiting...")
			return
		default:
		}

		err := consumer(ctx)

		if ctx.Err() != nil {
			log.Println("Shutdown complete")
			return
		}

		log.Printf("Consumer stopped: %v", err)

		time.Sleep(5 * time.Second)
	}
}
