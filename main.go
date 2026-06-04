package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	resty "github.com/go-resty/resty/v2"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"

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

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
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

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
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

	MyTruyencvClient := resty.New()
	MyTruyencvClient.SetBaseURL(os.Getenv("MYTRUYEN_BACKEND"))
	addLog(MyTruyencvClient)
	MyTruyencvToken, err := getMyTruyenAuthToken(MyTruyencvClient)
	if err != nil {
		log.Fatalf("Failed to get MyTruyen auth token: %v", err)
	}
	fmt.Printf("MyTruyen auth token: %s\n", MyTruyencvToken)
	MyTruyencvClient.SetAuthToken(MyTruyencvToken)

	meiliURL := os.Getenv("MEILI_URL")
	if !strings.HasPrefix(meiliURL, "http://") && !strings.HasPrefix(meiliURL, "https://") {
		meiliURL = "http://" + meiliURL
	}
	if !strings.Contains(meiliURL, ":") {
		meiliURL = meiliURL + ":7700"
	}
	MeiliClient := resty.New()
	MeiliClient.SetBaseURL(meiliURL)
	MeiliClient.SetHeader("Authorization", "Bearer "+os.Getenv("MEILI_MASTER_KEY"))

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var crawlRequest struct {
				Type   string `json:"type"`
				BookID string `json:"book_id"`
				Page   int    `json:"page"`
				Limit  int    `json:"limit"`
			}
			err := json.Unmarshal(d.Body, &crawlRequest)
			if err != nil {
				log.Printf("Error parsing message: %v", err)
				continue
			}

			log.Printf("Received a crawl request: Type=%s, BookID=%s, Page=%d, Limit=%d",
				crawlRequest.Type, crawlRequest.BookID, crawlRequest.Page, crawlRequest.Limit)

			var success bool
			switch crawlRequest.Type {
			case "crawl_genres":
				success = task.GenresHandler(MeTruyencvClient, MyTruyencvClient)
				if !success {
					log.Println("Failed to crawl genres")
				} else {
					log.Println("Successfully crawled genres")
				}

			case "crawl_tags":
				success = task.TagsHandler(MeTruyencvClient, MyTruyencvClient)
				if !success {
					log.Println("Failed to crawl tags")
				} else {
					log.Println("Successfully crawled tags")
				}

			case "crawl_book_statuses":
				success = task.BookStatusHandler(MeTruyencvClient, MyTruyencvClient)
				if !success {
					log.Println("Failed to crawl book statuses")
				} else {
					log.Println("Successfully crawled book statuses")
				}

			case "crawl_all_books":
				success = task.AllBookHandler(MeTruyencvClient, ch, q.Name)
				if !success {
					log.Println("Failed to crawl all books")
				} else {
					log.Println("Successfully enqueued all books crawling")
				}

			case "crawl_book":
				success = task.BookHandler(MeTruyencvClient, MyTruyencvClient, ch, q.Name, crawlRequest.Page, crawlRequest.Limit)
				if !success {
					log.Println("Failed to crawl books page")
				} else {
					log.Printf("Successfully crawled books page %d", crawlRequest.Page)
				}

			case "crawl_chapters":
				success = task.ChaptersHandler(MeTruyencvClient, MyTruyencvClient, crawlRequest.BookID)
				if !success {
					log.Println("Failed to crawl chapters")
				} else {
					log.Println("Successfully crawled chapters")
				}

			case "check_new_chapters":
				success = task.CheckNewChaptersHandler(MeTruyencvClient, MyTruyencvClient, ch, q.Name)
				if !success {
					log.Println("Failed to check new chapters")
				} else {
					log.Println("Successfully checked new chapters")
				}

			case "refresh_mytruyen_token":
				token, err := getMyTruyenAuthToken(MyTruyencvClient)
				if err != nil {
					log.Printf("Failed to refresh MyTruyen auth token: %v", err)
					success = false
				} else {
					log.Println("Successfully refreshed MyTruyen auth token.")
					MyTruyencvClient.SetAuthToken(token)
					success = true
				}

			case "add_all_books_to_meili":
				success = task.MeiliHandler(MyTruyencvClient, MeiliClient)
				if !success {
					log.Println("Failed to add all books to Meilisearch")
				} else {
					log.Println("Successfully added all books to Meilisearch")
				}

			default:
				log.Printf("Unknown crawl request type: %s", crawlRequest.Type)
				success = false
			}

			if success {
				d.Ack(false)
			} else {
				// Reject and requeue the message on failure
				d.Nack(false, true)
			}
		}
	}()

	<-forever
}
