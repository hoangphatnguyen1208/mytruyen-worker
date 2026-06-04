package task

import (
	"log"

	resty "github.com/go-resty/resty/v2"
	"github.com/gosimple/slug"
)

func GenresHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client) (bool) {
	var result struct {
		Data struct {
			Filter struct {
				Genres map[string]struct {
					Data map[string]string `json:"data"`
				} `json:"genres"`
			} `json:"filter"`
		} `json:"data"`
	}

	resp, err := meTruyencvClient.R().
		SetResult(&result).
		Get("books/options?v=1")

	if err != nil {
		log.Printf("Error fetching genres from MeTruyen: %v", err)
		return false
	}
	if resp.IsError() {
		log.Printf("Error response from MeTruyen when fetching genres: %s", resp.Status())
		return false
	}

	for _, genreGroup := range result.Data.Filter.Genres {
		for _, genreName := range genreGroup.Data {
			resp, err := myTruyenClient.R().
				SetBody(map[string]any{
					"name": genreName,
					"slug": slug.Make(genreName),
					"description": "",
				}).
				Post("/genres")
			
			if err != nil {
				log.Printf("Error creating genre in MyTruyen: %v", err)
				return false
			}
			if resp.StatusCode() >= 500 {
				log.Printf("Error response from MyTruyen when creating genre: %s", resp.Status())
				return false
			}
		}
	}
	return true
}