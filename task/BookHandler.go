package task

import (
	"fmt"
	"log"

	resty "github.com/go-resty/resty/v2"
)

func BookHandler(meTruyencvClient *resty.Client, myTruyenClient *resty.Client, bookID int) bool {
	var bookData struct {
		Data map[string]any `json:"data"`
	}

	resp, err := meTruyencvClient.R().
		SetResult(&bookData).
		Get(fmt.Sprintf("books/%d", bookID))

	if err != nil {
		log.Printf("Error fetching book from MeTruyen: %v", err)
		return false
	}
	if resp.IsError() {
		log.Printf("Error response from MeTruyen when fetching book %d: %s", bookID, resp.Status())
		return false
	}

	bookSlug, _ := bookData.Data["slug"].(string)
	bookName, _ := bookData.Data["name"].(string)

	// Check chapters count/status on MeTruyen
	meTruyenResponse, err := meTruyencvClient.R().
		SetQueryParam("filter[book_id]", fmt.Sprintf("%d", bookID)).
		Get("/chapters")
	if err != nil {
		log.Printf("Error checking chapters on MeTruyen for book %d: %v", bookID, err)
		return false
	}
	if meTruyenResponse.StatusCode() == 404 {
		log.Printf("Book '%s' not found on Metruyencv. Skipping.", bookSlug)
		return false
	}

	// Check if book exists in MyTruyen
	bookResponse, err := myTruyenClient.R().
		Get(fmt.Sprintf("books/id/%d", bookID))

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
			"name":             getVal(bookData.Data, "name", ""),
			"slug":             getVal(bookData.Data, "slug", ""),
			"kind":             getVal(bookData.Data, "kind", 1),
			"sex":              getVal(bookData.Data, "sex", 1),
			"status_id":        getVal(bookData.Data, "status", 1),
			"chapter_per_week": getVal(bookData.Data, "chapter_per_week", 1),
			"published":        getVal(bookData.Data, "published", true),
			"synopsis":         getVal(bookData.Data, "synopsis", ""),
			"note":             getVal(bookData.Data, "note", []any{}),
			"poster":           getVal(bookData.Data, "poster", ""),
			"chapter_count":    getVal(bookData.Data, "chapter_count", 0),
			"word_count":       getVal(bookData.Data, "word_count", 0),
			"view_count":       getVal(bookData.Data, "view_count", 0),
			"comment_count":    getVal(bookData.Data, "comment_count", 0),
			"review_count":     getVal(bookData.Data, "review_count", 0),
			"average_rating":   getVal(bookData.Data, "review_score", 0),
			"bookmark_count":   getVal(bookData.Data, "bookmark_count", 0),
		}
		_, err = myTruyenClient.R().
			SetBody(updatePayload).
			Patch(fmt.Sprintf("books/id/%d", bookID))
		if err != nil {
			log.Printf("Failed to update book %d: %v", bookID, err)
		}
		return true
	}

	// Handle author
	var authorObj map[string]any
	if a, ok := bookData.Data["author"].(map[string]any); ok {
		authorObj = a
	} else if c, ok := bookData.Data["creator"].(map[string]any); ok {
		authorObj = c
	}

	author, err := GetOrCreateAuthor(myTruyenClient, authorObj)
	if err != nil {
		log.Printf("Failed to get/create author for book %d: %v", bookID, err)
		return false
	}
	var authorID any
	if author != nil {
		authorID = author["id"]
	}

	// Handle genres
	genreIDs := []any{}
	if genresList, ok := bookData.Data["genres"].([]any); ok {
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
	tagIDs := []any{}
	if tagsList, ok := bookData.Data["tags"].([]any); ok {
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
		"name":             getVal(bookData.Data, "name", ""),
		"slug":             getVal(bookData.Data, "slug", ""),
		"kind":             getVal(bookData.Data, "kind", 1),
		"sex":              getVal(bookData.Data, "sex", 1),
		"status_id":        getVal(bookData.Data, "status", 1),
		"chapter_per_week": getVal(bookData.Data, "chapter_per_week", 1),
		"published":        getVal(bookData.Data, "published", true),
		"synopsis":         getVal(bookData.Data, "synopsis", ""),
		"note":             getVal(bookData.Data, "note", []any{}),
		"author_id":        authorID,
		"genre_ids":        genreIDs,
		"tag_ids":          tagIDs,
		"poster":           getVal(bookData.Data, "poster", ""),
		"chapter_count":    getVal(bookData.Data, "chapter_count", 0),
		"word_count":       getVal(bookData.Data, "word_count", 0),
	}

	mytruyen_resp, err := myTruyenClient.R().
		SetBody(payload).
		Post("books")
	if err != nil {
		log.Printf("Failed to create book %d: %v", bookID, err)
		return false
	} else if mytruyen_resp.IsError() {
		log.Printf("Failed to create book %d, status: %s, body: %s", bookID, mytruyen_resp.Status(), mytruyen_resp.String())
		return false
	}

	// Post chapters task to queue
	mytruyen_resp, err = myTruyenClient.R().
		SetBody(map[string]int{
			"book_id": bookID,
		}).
		Post("rabbitmq/chapters")
	if err != nil {
		log.Printf("Failed to enqueue chapters for book %d: %v", bookID, err)
			return false
	} else if mytruyen_resp.IsError() {
		log.Printf("Failed to enqueue chapters for book %d, status: %s, body: %s", bookID, mytruyen_resp.Status(), mytruyen_resp.String())
		return false
	}
	return true
}
