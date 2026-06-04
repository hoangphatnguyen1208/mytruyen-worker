package task

import (
	"log"

	resty "github.com/go-resty/resty/v2"
	"github.com/gosimple/slug"
)

func BookStatusHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client) bool {
	var result struct {
		Data struct {
			Filter struct {
				Status struct {
					Data map[string]string `json:"data"`
				} `json:"status"`
			} `json:"filter"`
		} `json:"data"`
	}

	resp, err := meTruyencvClient.R().
		SetResult(&result).
		Get("books/options?v=1")

	if err != nil {
		log.Printf("Error fetching statuses from MeTruyen: %v", err)
		return false
	}
	if resp.IsError() {
		log.Printf("Error response from MeTruyen when fetching statuses: %s", resp.Status())
		return false
	}

	for _, statusName := range result.Data.Filter.Status.Data {
		resp, err := myTruyenClient.R().
			SetBody(map[string]any{
				"name":        statusName,
				"slug":        slug.Make(statusName),
				"description": "",
			}).
			Post("/book-statuses")

		if err != nil {
			log.Printf("Error creating status in MyTruyen: %v", err)
			return false
		}
		if resp.StatusCode() >= 500 {
			log.Printf("Error response from MyTruyen when creating status: %s", resp.Status())
			return false
		}
	}
	return true
}