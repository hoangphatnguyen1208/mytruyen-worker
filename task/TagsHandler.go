package task

import (
	"log"

	resty "github.com/go-resty/resty/v2"
	"github.com/gosimple/slug"
)

func TagsHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client, workerID int) bool {
	var result struct {
		Data struct {
			Filter struct {
				Tags map[string]struct {
					Name string            `json:"name"`
					Data map[string]string `json:"data"`
				} `json:"tags"`
			} `json:"filter"`
		} `json:"data"`
	}

	resp, err := meTruyencvClient.R().
		SetResult(&result).
		Get("books/options?v=1")

	if err != nil {
		log.Printf("[Worker %d] Error fetching tags from MeTruyen: %v", workerID, err)
		return false
	}
	if resp.IsError() {
		log.Printf("[Worker %d] Error response from MeTruyen when fetching tags: %s", workerID, resp.Status())
		return false
	}

	for _, tagGroup := range result.Data.Filter.Tags {
		for _, tagName := range tagGroup.Data {
			resp, err := myTruyenClient.R().
				SetBody(map[string]any{
					"name":        tagName,
					"slug":        slug.Make(tagName),
					"type":        tagGroup.Name,
					"description": "",
				}).
				Post("/tags")

			if err != nil {
				log.Printf("[Worker %d] Error creating tag in MyTruyen: %v", workerID, err)
				return false
			}
			if resp.StatusCode() >= 500 {
				log.Printf("[Worker %d] Error response from MyTruyen when creating tag: %s", workerID, resp.Status())
				return false
			}
		}
	}
	return true
}