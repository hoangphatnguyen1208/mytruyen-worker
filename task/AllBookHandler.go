package task

import (
	"encoding/json"
	"log"

	resty "github.com/go-resty/resty/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func AllBookHandler(meTruyencvClient *resty.Client, ch *amqp.Channel, queueName string) bool {

	var result struct {
		Pagination struct {
			Last int `json:"Last"`
		} `json:"pagination"`
	}

	resp, err := meTruyencvClient.R().
		SetQueryParams(map[string]string{
			"limit": "20",
			"page":  "1",
			"sort":  "-created_at",
			"state": "published",
		}).
		SetResult(&result).
		Get("books")

	if err != nil {
		log.Printf("Error fetching all books for pagination: %v", err)
		return false
	}
	if resp.IsError() {
		log.Printf("Error response from MeTruyen when fetching pagination: %s", resp.Status())
		return false
	}

	lastPage := result.Pagination.Last

	log.Printf("Pagination info: lastPage=%d", lastPage)

	for i := 1; i <= lastPage; i++ {
		payload := map[string]any{
			"type":  "crawl_book",
			"page":  i,
			"limit": 20,
		}
		body, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error marshalling crawl_book payload: %v", err)
			continue
		}

		err = ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		if err != nil {
			log.Printf("Failed to publish crawl_book task for page %d: %v", i, err)
			return false
		}
	}

	return true
}