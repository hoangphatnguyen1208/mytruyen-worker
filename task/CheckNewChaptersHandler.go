package task

import (
	"log"
	"fmt"

	resty "github.com/go-resty/resty/v2"
)

func CheckNewChaptersHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client) bool {
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

	books := result.Data
	if len(books) == 0 {
		log.Println("No books with new chapters found.")
		return true
	}

	for _, book := range books {
		bookID, ok := book["id"].(float64)
		if !ok {
			log.Printf("Invalid book ID format: %v", book["id"])
			continue
		}
		bookIDInt := int(bookID)

		log.Printf("Found book with new chapters: ID=%d, Name=%s", bookIDInt, book["name"])

		type response struct {
			Data map[string]any `json:"data"`
		}
		var meTruyenResponse response
		metruyencvResp, err := meTruyencvClient.R().
			SetResult(&meTruyenResponse).
			Get(fmt.Sprintf("books/%d", bookIDInt))
		if err != nil {
			log.Printf("Error fetching book details for book ID %d: %v", bookIDInt, err)
		}
		if metruyencvResp.IsError() {
			log.Printf("Error response from MeTruyen when fetching book details for book ID %d: %s", bookIDInt, metruyencvResp.Status())
		}
		
		
		var mytruyenResponse response
		mytruyenResp, err := myTruyenClient.R().
			SetResult(&mytruyenResponse).
			Get(fmt.Sprintf("books/id/%d", bookIDInt))
		if err != nil {
			log.Printf("Error checking book in MyTruyen for book ID %d: %v", bookIDInt, err)
		}
		if mytruyenResp.StatusCode() == 404 {
			log.Printf("Book ID %d not found in MyTruyen.", bookIDInt)
		}

		if mytruyenResponse.Data["chapter_count"] != nil && meTruyenResponse.Data["chapter_count"] == mytruyenResponse.Data["chapter_count"] {
			log.Printf("Book ID %d has same chapter count.", bookIDInt)
			continue
		}

		// Post chapters task to queue
		mytruyen_resp, err := myTruyenClient.R().
			SetBody(map[string]int{
				"book_id": bookIDInt,
			}).
			Post("rabbitmq/book")

		if err != nil {
			log.Printf("Error posting chapters task for book %d: %v", bookIDInt, err)
			continue
		}
		if mytruyen_resp.IsError() {
			log.Printf("Error response from MyTruyen when posting chapters task for book %d: %s", bookIDInt, mytruyen_resp.Status())
			continue
		}

		log.Printf("Posted chapters task for book ID %d to queue successfully.", bookIDInt)
	}

	return true
}
