package task

import (
	"fmt"
	"log"

	resty "github.com/go-resty/resty/v2"
)

func AllBookHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client, queueName string, workerID int) bool {

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
		log.Printf("[Worker %d] Error fetching all books for pagination: %v", workerID, err)
		return false
	}
	if resp.IsError() {
		log.Printf("[Worker %d] Error response from MeTruyen when fetching pagination: %s", workerID, resp.Status())
		return false
	}

	lastPage := result.Pagination.Last

	log.Printf("[Worker %d] Pagination info: lastPage=%d", workerID, lastPage)

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
			log.Printf("[Worker %d] Error fetching books for page %d: %v", workerID, i, err)
			return false
		}
		if resp.IsError() {
			log.Printf("[Worker %d] Error response from MeTruyen when fetching books for page %d: %s", workerID, i, resp.Status())
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
			log.Printf("[Worker %d] Error enqueueing book: %v", workerID, err)
			return false
		}
		if mytruyen_resp.IsError() {
			log.Printf("[Worker %d] Error response from MyTruyen when enqueueing book: %s", workerID, mytruyen_resp.Body())
			return false
		}
	}
	return true
}