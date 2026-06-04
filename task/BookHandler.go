package task

import (
	"fmt"
	"log"

	resty "github.com/go-resty/resty/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func BookHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client, ch *amqp.Channel, queueName string, page, limit int) bool {
	if limit <= 0 {
		limit = 20
	}
	if page <= 0 {
		page = 1
	}

	var result struct {
		Data []map[string]any `json:"data"`
	}

	resp, err := meTruyencvClient.R().
		SetQueryParams(map[string]string{
			"include": "author,genres,tags,creator",
			"limit":   fmt.Sprintf("%d", limit),
			"page":    fmt.Sprintf("%d", page),
			"sort":    "-created_at",
		}).
		SetResult(&result).
		Get("books")

	if err != nil {
		log.Printf("Error fetching book list from MeTruyen for page %d: %v", page, err)
		return false
	}
	if resp.IsError() {
		log.Printf("Error response from MeTruyen when fetching book list for page %d: %s", page, resp.Status())
		return false
	}

	return HandleBooks(result.Data, meTruyencvClient, myTruyenClient, ch, queueName)
}
