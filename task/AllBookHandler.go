package task

import (
	"fmt"
	"log"

	resty "github.com/go-resty/resty/v2"
)

func AllBookHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client, queueName string) bool {

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

	payloads := make([]map[string]any, 0)

	for i := 1; i <= lastPage; i++ {
		var res struct {
			Data []struct {
				ID int `json:"id"`
			} `json:"data"`
		}

		resp, err := meTruyencvClient.R().
			SetQueryParams(map[string]string{
				"limit": "20",
				"page":  fmt.Sprintf("%d", i),
				"sort":  "-created_at",
				"state": "published",
			}).
			SetResult(&res).
			Get("books")
		
		if err != nil {
			log.Printf("Error fetching books for page %d: %v", i, err)
			return false
		}
		if resp.IsError() {
			log.Printf("Error response from MeTruyen when fetching books for page %d: %s", i, resp.Status())
			return false
		}
		for _, book := range res.Data {
			payload := map[string]any{
				"book_id": book.ID,
			}
			payloads = append(payloads, payload)
		}
	}
    
	for _, payload := range payloads {
		mytruyen_resp, err := myTruyenClient.R().
			SetBody(payload).
			Post("rabbitmq/book")
		if err != nil {
			log.Printf("Error enqueueing book: %v", err)
			return false
		}
		if mytruyen_resp.IsError() {
			log.Printf("Error response from MyTruyen when enqueueing book: %s", mytruyen_resp.Body())
			return false
		}
	}
	return true
}