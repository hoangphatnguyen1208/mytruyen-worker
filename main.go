package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/meilisearch/meilisearch-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Khai báo số lượng luồng (workers) chạy song song
const ConcurrencyLimit = 5

// Hàm helper để log lỗi và crash app nếu có lỗi nghiêm trọng lúc khởi động
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	// =========================================================================
	// 1. KHỞI TẠO MEILISEARCH CLIENT
	// =========================================================================
	meiliClient := meilisearch.NewClient(meilisearch.ClientConfig{
		Host:   "http://localhost:7700",
		APIKey: "YOUR_MASTER_KEY", // Thay bằng Master Key thực tế
	})
	// Trỏ tới index chứa truyện
	index := meiliClient.Index("truyen")

	// =========================================================================
	// 2. KHỞI TẠO RABBITMQ CONNECTION & CHANNEL
	// =========================================================================
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Không thể kết nối tới RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Không thể mở Channel RabbitMQ")
	// LƯU Ý: Không dùng defer ch.Close() ở đây nữa vì ta sẽ tự tay đóng nó lúc Shutdown

	// Khai báo Queue (Đảm bảo queue tồn tại)
	q, err := ch.QueueDeclare(
		"meilisearch_sync_queue", // Tên queue
		true,                     // Durable (Lưu ổ cứng)
		false,                    // Delete when unused
		false,                    // Exclusive
		false,                    // No-wait
		nil,                      // Arguments
	)
	failOnError(err, "Không thể khai báo Queue")

	// Cấu hình QoS (Prefetch): Lấy sẵn 10 task về RAM để chia cho 5 workers
	err = ch.Qos(10, 0, false)
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
				var document map[string]interface{}
				if err := json.Unmarshal(d.Body, &document); err != nil {
					log.Printf("[Worker %d] Lỗi format JSON. Vứt bỏ task. Lỗi: %v", workerID, err)
					d.Nack(false, false) // Nack và không requeue
					continue
				}

				// 4.2. Push dữ liệu lên Meilisearch
				taskInfo, err := index.AddDocuments([]map[string]interface{}{document})
				if err != nil {
					log.Printf("[Worker %d] Lỗi gọi API Meilisearch. Requeue task. Lỗi: %v", workerID, err)
					d.Nack(false, true) // Nack và đưa lại vào hàng đợi để thử lại
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
	// Tạo 1 channel để hứng tín hiệu từ OS (Hệ điều hành)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Hàm main đứng ở đây và BỊ CHẶN cho đến khi có người bấm Ctrl+C hoặc OS kill process
	<-quit
	log.Println("\n[!] Nhận lệnh tắt server. Bắt đầu tiến trình Graceful Shutdown...")

	// Bước 1: Đóng RabbitMQ Channel
	// -> Việc này ngăn RabbitMQ gửi thêm message mới xuống.
	// -> Đồng thời làm cho vòng lặp `for d := range msgs` ở các workers KẾT THÚC.
	log.Println("Đóng kết nối RabbitMQ Channel...")
	ch.Close()

	// Bước 2: Chờ các workers làm nốt việc dang dở
	log.Println("Đang chờ các workers hoàn tất task hiện tại...")
	wg.Wait()

	// Khi wg.Wait() chạy xong nghĩa là tất cả các lệnh defer wg.Done() đã được gọi
	log.Println("Tất cả data đã xử lý an toàn. Server tắt thành công!")
}