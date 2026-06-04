package task

import (
	"fmt"
	"log"

	resty "github.com/go-resty/resty/v2"
)

func MeiliHandler(myTruyenClient *resty.Client, meiliClient *resty.Client) bool {
	log.Println("Starting to add all books to Meilisearch...")

	var result struct {
		Pagination struct {
			TotalPages int `json:"total_pages"`
		} `json:"pagination"`
	}

	resp, err := myTruyenClient.R().
		SetQueryParams(map[string]string{
			"limit": "30",
			"page":  "1",
			"sort":  "-created_at",
		}).
		SetResult(&result).
		Get("books")

	if err != nil {
		log.Printf("Error fetching books from MyTruyen: %v", err)
		return false
	}
	if resp.IsError() {
		log.Printf("Error response from MyTruyen when fetching pagination: %s", resp.Status())
		return false
	}

	totalPages := result.Pagination.TotalPages
	if totalPages <= 0 {
		totalPages = 1
	}

	for page := 1; page <= totalPages; page++ {
		var pageResult struct {
			Data []map[string]any `json:"data"`
		}

		resp, err := myTruyenClient.R().
			SetQueryParams(map[string]string{
				"limit": "30",
				"page":  fmt.Sprintf("%d", page),
				"sort":  "-created_at",
			}).
			SetResult(&pageResult).
			Get("books")

		if err != nil {
			log.Printf("Error fetching books page %d from MyTruyen: %v", page, err)
			continue
		}
		if resp.IsError() {
			log.Printf("Error response from MyTruyen for books page %d: %s", page, resp.Status())
			continue
		}

		for _, book := range pageResult.Data {
			id := book["id"]
			name := book["name"]
			
			authorName := ""
			if author, ok := book["author"].(map[string]any); ok {
				authorName, _ = author["name"].(string)
			}

			doc := map[string]any{
				"id":     id,
				"name":   name,
				"author": authorName,
			}

			// Add to Meilisearch: POST /indexes/books/documents
			meiliResp, err := meiliClient.R().
				SetBody([]any{doc}).
				Post("indexes/books/documents")

			if err != nil {
				log.Printf("Error adding book ID %v to Meilisearch: %v", id, err)
			} else if meiliResp.IsError() {
				log.Printf("Failed to add book ID %v to Meilisearch, status: %s, body: %s", id, meiliResp.Status(), meiliResp.String())
			} else {
				log.Printf("Added book ID %v for search indexing.", id)
			}
		}
	}

	return true
}
