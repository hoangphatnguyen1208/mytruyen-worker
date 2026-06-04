package task

import (
	"log"

	resty "github.com/go-resty/resty/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CheckNewChaptersHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client, ch *amqp.Channel, queueName string) bool {
	log.Println("Checking for new chapters...")

	var result struct {
		Data []map[string]any `json:"data"`
	}

	resp, err := meTruyencvClient.R().
		SetQueryParams(map[string]string{
			"include": "author,genres,tags,creator",
			"limit":   "20",
			"page":    "1",
			"sort":    "-new_chap_at",
			"state":   "published",
		}).
		SetResult(&result).
		Get("books")

	if err != nil {
		log.Printf("Error checking new chapters from MeTruyen: %v", err)
		return false
	}
	if resp.IsError() {
		log.Printf("Error response from MeTruyen when checking new chapters: %s", resp.Status())
		return false
	}

	return HandleBooks(result.Data, meTruyencvClient, myTruyenClient, ch, queueName)
}
