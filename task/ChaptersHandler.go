package task

import (
	"fmt"
	"log"

	resty "github.com/go-resty/resty/v2"
)

func ChaptersHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client, bookID int, workerID int) bool {
	log.Printf("[Worker %d] Crawling chapters for book '%d'", workerID, bookID)

	var result struct {
		Data []map[string]any `json:"data"`
	}

	resp, err := meTruyencvClient.R().
		SetQueryParam("filter[book_id]", fmt.Sprintf("%d", bookID)).
		SetResult(&result).
		Get("chapters/")

	if err != nil {
		log.Printf("[Worker %d] Error fetching chapters from MeTruyen for book %d: %v", workerID, bookID, err)
		return false
	}
	if resp.IsError() {
		log.Printf("[Worker %d] Error response from MeTruyen when fetching chapters for book %d: %s", workerID, bookID, resp.Status())
		return false
	}

	chapters := result.Data
	n := len(chapters)
	if n == 0 {
		log.Printf("[Worker %d] No chapters found for book %d on MeTruyen", workerID, bookID)
		return true
	}

	// Use binary search to find max chapter index in MyTruyen
	start := -1
	l := 0
	r := n - 1

	for l <= r {
		mid := (l + r) / 2
		chapter := chapters[mid]
		
		indexVal := GetValAsInt(chapter["index"], 0)

		chapterResp, err := myTruyenClient.R().
			Get(fmt.Sprintf("chapters/id/%d/%d", bookID, indexVal))

		if err == nil && chapterResp.StatusCode() == 200 {
			start = mid
			l = mid + 1
		} else {
			r = mid - 1
		}
	}

	log.Printf("[Worker %d] Max existing chapter index for book '%d' is %d", workerID, bookID, start)

	for i := start + 1; i < n; i++ {
		chapter := chapters[i]
		name, _ := chapter["name"].(string)
		indexVal := GetValAsInt(chapter["index"], 0)
		wordCountVal := GetValAsInt(chapter["word_count"], 0)

		payload := map[string]any{
			"name":       name,
			"index":      indexVal,
			"word_count": wordCountVal,
			"published":  true,
		}

		resp, err = myTruyenClient.R().
			SetBody(payload).
			Post(fmt.Sprintf("chapters/id/%d", bookID))

		if err != nil {
			log.Printf("[Worker %d] Error posting chapter %d for book %d: %v", workerID, indexVal, bookID, err)
			return false
		} else if resp.StatusCode() >= 500 {
			log.Printf("[Worker %d] Failed to post chapter %d for book %d, status: %s, body: %s", workerID, indexVal, bookID, resp.Status(), resp.String())
			return false
		}
	}

	return true
}
