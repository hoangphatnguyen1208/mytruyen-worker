package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/meilisearch/meilisearch-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Hàm helper để log lỗi và crash app nếu có lỗi nghiêm trọng lúc khởi động
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system env")
	}

	ConcurrencyLimitStr := os.Getenv("CONCURRENCY_LIMIT")
	ConcurrencyLimit, err := strconv.Atoi(ConcurrencyLimitStr)
	if err != nil {
		log.Panicf("Lỗi khi chuyển đổi CONCURRENCY_LIMIT sang số nguyên: %v", err)
	}

	// =========================================================================
	// 1. KHỞI TẠO MEILISEARCH CLIENT
	// =========================================================================
	meiliClient := meilisearch.New(
		os.Getenv("MEILI_URL"),
		meilisearch.WithAPIKey(os.Getenv("MEILI_MASTER_KEY")),
	)

	indexName := os.Getenv("MEILI_INDEX_NAME")

	_, err = meiliClient.GetIndex(indexName)

	if err != nil {
		// 2. Nếu chưa có → tạo
		_, err = meiliClient.CreateIndex(&meilisearch.IndexConfig{
			Uid:        indexName,
			PrimaryKey: "id",
		})
		if err != nil {
			panic(err)
		}
	}

	index := meiliClient.Index(indexName)

	// =========================================================================
	// 2. KHỞI TẠO RABBITMQ CONNECTION & CHANNEL
	// =========================================================================
	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	failOnError(err, "Không thể kết nối tới RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Không thể mở Channel RabbitMQ")

	q, err := ch.QueueDeclare(
		os.Getenv("RABBITMQ_QUEUE_NAME"), // Tên queue
		true,                             // Durable (Lưu ổ cứng)
		false,                            // Delete when unused
		false,                            // Exclusive
		false,                            // No-wait
		nil,                              // Arguments
	)
	failOnError(err, "Không thể khai báo Queue")

	err = ch.Qos(ConcurrencyLimit*2, 0, false)
	failOnError(err, "Không thể thiết lập QoS")

	// =========================================================================
	// 3. KHỞI TẠO CONSUMER
	// =========================================================================
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer tag
		false,  // Auto-ack = FALSE (Rất quan trọng, phải tự Ack)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Lỗi khi đăng ký Consumer")

	// =========================================================================
	// 4. CHẠY WORKER POOL
	// =========================================================================
	var wg sync.WaitGroup
	log.Printf("Bắt đầu khởi tạo %d workers...", ConcurrencyLimit)

	for i := 1; i <= ConcurrencyLimit; i++ {
		wg.Add(1)

		// Khởi chạy Goroutine (Worker)
		go func(workerID int) {
			defer wg.Done() // Báo cáo hoàn thành khi Worker bị tắt

			// Worker liên tục nhặt task từ channel 'msgs'
			for d := range msgs {
				log.Printf("[Worker %d] Đã nhận 1 task...", workerID)

				// 4.1. Decode JSON
				var document map[string]any
				if err := json.Unmarshal(d.Body, &document); err != nil {
					log.Printf("[Worker %d] Lỗi format JSON. Vứt bỏ task. Lỗi: %v", workerID, err)
					d.Nack(false, false) // Nack và không requeue
					continue
				}

				docType, ok := document["type"].(string)
				if !ok {
					log.Printf("[Worker %d] Lỗi field 'type' (missing hoặc sai kiểu)", workerID)
					d.Nack(false, false)
					continue
				}

				docData, ok := document["data"].(map[string]any)
				if !ok {
					log.Printf("[Worker %d] Lỗi field 'data' (missing hoặc sai kiểu)", workerID)
					d.Nack(false, false)
					continue
				}

				var taskInfo *meilisearch.TaskInfo
				var err error
				switch docType {
				case "post":
					taskInfo, err = index.AddDocuments(
						[]map[string]any{docData}, nil)
					if err != nil {
						log.Printf("[Worker %d] Lỗi gọi API Meilisearch. Requeue task. Lỗi: %v", workerID, err)
						d.Nack(false, true) // Nack và đưa lại vào hàng đợi để thử lại
						continue
					}
				case "patch":
					taskInfo, err = index.UpdateDocuments(
						[]map[string]any{docData}, nil)
					if err != nil {
						log.Printf("[Worker %d] Lỗi gọi API Meilisearch. Requeue task. Lỗi: %v", workerID, err)
						d.Nack(false, true) // Nack và đưa lại vào hàng đợi để thử lại
						continue
					}
				case "delete":
					id, ok := docData["id"].(string)
					if !ok {
						log.Printf("[Worker %d] Lỗi field 'id' (missing hoặc sai kiểu)", workerID)
						d.Nack(false, false)
						continue
					}
					taskInfo, err = index.DeleteDocument(id, nil)
					if err != nil {
						log.Printf("[Worker %d] Lỗi gọi API Meilisearch. Requeue task. Lỗi: %v", workerID, err)
						d.Nack(false, true) // Nack và đưa lại vào hàng đợi để thử lại
						continue
					}

				default:
					log.Printf("[Worker %d] Unknown docType: %s", workerID, docType)
					d.Nack(false, false) // bỏ luôn hoặc gửi DLQ
					continue
				}

				// 4.3. Thành công -> Xác nhận (Ack)
				log.Printf("[Worker %d] Push Meili thành công! TaskUID: %d", workerID, taskInfo.TaskUID)
				d.Ack(false)
			}

			log.Printf("[Worker %d] Đã dừng vòng lặp nhận task.", workerID)
		}(i) // Truyền i vào làm ID cho worker
	}

	log.Printf(" [*] Hệ thống đang chạy với %d luồng song song.", ConcurrencyLimit)
	log.Printf(" [*] Nhấn CTRL+C để tắt server an toàn (Graceful Shutdown).")

	// =========================================================================
	// 5. CƠ CHẾ GRACEFUL SHUTDOWN (CHỜ TÍN HIỆU TỪ HỆ ĐIỀU HÀNH)
	// =========================================================================
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("\n[!] Nhận lệnh tắt server. Bắt đầu tiến trình Graceful Shutdown...")

	log.Println("Đóng kết nối RabbitMQ Channel...")
	ch.Close()

	log.Println("Đang chờ các workers hoàn tất task hiện tại...")
	wg.Wait()

	log.Println("Tất cả data đã xử lý an toàn. Server tắt thành công!")
}
