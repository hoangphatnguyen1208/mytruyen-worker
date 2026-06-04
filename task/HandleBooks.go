package task

import (
	"encoding/json"
	"fmt"
	"log"

	resty "github.com/go-resty/resty/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func HandleBooks(books []map[string]any, meTruyencvClient *resty.Client, myTruyenClient *resty.Client, ch *amqp.Channel, queueName string) bool {
	var arqJobs []string

	for _, bookData := range books {
		bookID := GetIDString(bookData["id"])
		if bookID == "" {
			log.Printf("Book data does not contain a valid ID: %v", bookData)
			continue
		}

		bookSlug, _ := bookData["slug"].(string)
		bookName, _ := bookData["name"].(string)

		// Check chapters count/status on MeTruyen
		meTruyenResponse, err := meTruyencvClient.R().
			SetQueryParam("filter[book_id]", bookID).
			Get("/chapters")
		if err != nil {
			log.Printf("Error checking chapters on MeTruyen for book %s: %v", bookID, err)
			continue
		}
		if meTruyenResponse.StatusCode() == 404 {
			log.Printf("Book '%s' not found on Metruyencv. Skipping.", bookSlug)
			continue
		}

		arqJobs = append(arqJobs, bookID)

		// Check if book exists in MyTruyen
		bookResponse, err := myTruyenClient.R().
			Get(fmt.Sprintf("books/id/%s", bookID))

		// Helper to fetch value or default
		getVal := func(m map[string]any, key string, def any) any {
			if val, ok := m[key]; ok && val != nil {
				return val
			}
			return def
		}

		if err == nil && bookResponse.StatusCode() == 200 {
			log.Printf("Book '%s' already exists in MyTruyen. Updating...", bookName)
			updatePayload := map[string]any{
				"name":             getVal(bookData, "name", ""),
				"slug":             getVal(bookData, "slug", ""),
				"kind":             getVal(bookData, "kind", 1),
				"sex":              getVal(bookData, "sex", 1),
				"status_id":        getVal(bookData, "status", 1),
				"chapter_per_week": getVal(bookData, "chapter_per_week", 1),
				"published":        getVal(bookData, "published", true),
				"synopsis":         getVal(bookData, "synopsis", ""),
				"note":             getVal(bookData, "note", []any{}),
				"poster":           getVal(bookData, "poster", ""),
				"chapter_count":    getVal(bookData, "chapter_count", 0),
				"word_count":       getVal(bookData, "word_count", 0),
				"view_count":       getVal(bookData, "view_count", 0),
				"comment_count":    getVal(bookData, "comment_count", 0),
				"review_count":     getVal(bookData, "review_count", 0),
				"average_rating":   getVal(bookData, "review_score", 0),
				"bookmark_count":   getVal(bookData, "bookmark_count", 0),
			}
			_, err = myTruyenClient.R().
				SetBody(updatePayload).
				Patch(fmt.Sprintf("books/id/%s", bookID))
			if err != nil {
				log.Printf("Failed to update book %s: %v", bookID, err)
			}
			continue
		}

		// Handle author
		var authorObj map[string]any
		if a, ok := bookData["author"].(map[string]any); ok {
			authorObj = a
		} else if c, ok := bookData["creator"].(map[string]any); ok {
			authorObj = c
		}

		author, err := GetOrCreateAuthor(myTruyenClient, authorObj)
		if err != nil {
			log.Printf("Failed to get/create author for book %s: %v", bookID, err)
			continue
		}
		var authorID any
		if author != nil {
			authorID = author["id"]
		}

		// Handle genres
		var genreIDs []any
		if genresList, ok := bookData["genres"].([]any); ok {
			for _, genreItem := range genresList {
				if gMap, ok := genreItem.(map[string]any); ok {
					gid, err := GetOrCreateGenre(myTruyenClient, gMap)
					if err == nil && gid != nil {
						genreIDs = append(genreIDs, gid)
					}
				}
			}
		}

		// Handle tags
		var tagIDs []any
		if tagsList, ok := bookData["tags"].([]any); ok {
			for _, tagItem := range tagsList {
				if tMap, ok := tagItem.(map[string]any); ok {
					tid, err := GetOrCreateTag(myTruyenClient, tMap)
					if err == nil && tid != nil {
						tagIDs = append(tagIDs, tid)
					}
				}
			}
		}

		// Prepare payload for MyTruyen
		payload := map[string]any{
			"id":               bookID,
			"name":             getVal(bookData, "name", ""),
			"slug":             getVal(bookData, "slug", ""),
			"kind":             getVal(bookData, "kind", 1),
			"sex":              getVal(bookData, "sex", 1),
			"status_id":        getVal(bookData, "status", 1),
			"chapter_per_week": getVal(bookData, "chapter_per_week", 1),
			"published":        getVal(bookData, "published", true),
			"synopsis":         getVal(bookData, "synopsis", ""),
			"note":             getVal(bookData, "note", []any{}),
			"author_id":        authorID,
			"genre_ids":        genreIDs,
			"tag_ids":          tagIDs,
			"poster":           getVal(bookData, "poster", ""),
			"chapter_count":    getVal(bookData, "chapter_count", 0),
			"word_count":       getVal(bookData, "word_count", 0),
		}

		resp, err := myTruyenClient.R().
			SetBody(payload).
			Post("books")
		if err != nil {
			log.Printf("Failed to create book %s: %v", bookID, err)
		} else if resp.IsError() {
			log.Printf("Failed to create book %s, status: %s, body: %s", bookID, resp.Status(), resp.String())
		}
	}

	// Enqueue crawl_chapters jobs
	for _, bID := range arqJobs {
		payload := map[string]any{
			"type":    "crawl_chapters",
			"book_id": bID,
		}
		body, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Failed to marshal crawl_chapters payload: %v", err)
			continue
		}

		err = ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		)
		if err != nil {
			log.Printf("Failed to publish crawl_chapters for book %s: %v", bID, err)
		}
	}

	return true
}
