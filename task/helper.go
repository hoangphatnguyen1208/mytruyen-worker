package task

import (
	"fmt"
	"log"
	"net/url"

	resty "github.com/go-resty/resty/v2"
	"github.com/gosimple/slug"
)

func GetOrCreateAuthor(myTruyenClient *resty.Client, authorObj map[string]any) (map[string]any, error) {
	if authorObj == nil {
		log.Printf("Author object is nil.")
		return nil, nil
	}
	name, _ := authorObj["name"].(string)
	if name == "" {
		log.Printf("Author name is empty.")
		return nil, nil
	}


	// Fetch from MyTruyen
	var getResult struct {
		Data map[string]any `json:"data"`
	}

	encodedName := url.PathEscape(name)
	resp, err := myTruyenClient.R().
		SetResult(&getResult).
		Get(fmt.Sprintf("authors/%s", encodedName))

	if err != nil || resp.IsError() {
		// Not found or error, create the author
		localName, _ := authorObj["local_name"].(string)
		var postResult struct {
			Data map[string]any `json:"data"`
		}
		resp, err = myTruyenClient.R().
			SetBody(map[string]any{
				"name":       name,
				"local_name": localName,
			}).
			SetResult(&postResult).
			Post("authors")

		if err != nil {
			return nil, fmt.Errorf("failed to create author: %w", err)
		}
		if resp.IsError() {
			return nil, fmt.Errorf("failed to create author, status: %s, body: %s", resp.Status(), resp.String())
		}
		getResult.Data = postResult.Data
	}

	author := getResult.Data
	return author, nil
}

func GetOrCreateGenre(myTruyenClient *resty.Client, genreObj map[string]any) (any, error) {
	if genreObj == nil {
		return nil, nil
	}
	name, _ := genreObj["name"].(string)
	if name == "" {
		return nil, nil
	}
	genreSlug := slug.Make(name)

	var getResult struct {
		Data map[string]any `json:"data"`
	}
	resp, err := myTruyenClient.R().
		SetResult(&getResult).
		Get(fmt.Sprintf("genres/%s", genreSlug))

	if err != nil || resp.IsError() {
		var postResult struct {
			Data map[string]any `json:"data"`
		}
		resp, err = myTruyenClient.R().
			SetBody(map[string]any{
				"name":        name,
				"slug":        genreSlug,
				"description": "",
			}).
			SetResult(&postResult).
			Post("genres")

		if err != nil {
			return nil, fmt.Errorf("failed to create genre: %w", err)
		}
		if resp.IsError() {
			return nil, fmt.Errorf("failed to create genre, status: %s, body: %s", resp.Status(), resp.String())
		}
		getResult.Data = postResult.Data
	}

	genreData := getResult.Data
	if genreData != nil {
		id := genreData["id"]
		return id, nil
	}
	return nil, nil
}

func GetOrCreateTag(myTruyenClient *resty.Client, tagObj map[string]any) (any, error) {
	if tagObj == nil {
		return nil, nil
	}
	name, _ := tagObj["name"].(string)
	if name == "" {
		return nil, nil
	}
	tagSlug := slug.Make(name)

	var getResult struct {
		Data map[string]any `json:"data"`
	}
	resp, err := myTruyenClient.R().
		SetResult(&getResult).
		Get(fmt.Sprintf("tags/%s", tagSlug))

	if err != nil || resp.IsError() {
		tagType, _ := tagObj["type"].(string)
		if tagType == "" {
			tagType = "Khác"
		}
		var postResult struct {
			Data map[string]any `json:"data"`
		}
		resp, err = myTruyenClient.R().
			SetBody(map[string]any{
				"name":        name,
				"slug":        tagSlug,
				"type":        tagType,
				"description": "",
			}).
			SetResult(&postResult).
			Post("tags")

		if err != nil {
			return nil, fmt.Errorf("failed to create tag: %w", err)
		}
		if resp.IsError() {
			return nil, fmt.Errorf("failed to create tag, status: %s, body: %s", resp.Status(), resp.String())
		}
		getResult.Data = postResult.Data
	}

	tagData := getResult.Data
	if tagData != nil {
		id := tagData["id"]
		return id, nil
	}
	return nil, nil
}

func GetIDString(val any) string {
	if val == nil {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func GetValAsInt(val any, def int) int {
	if val == nil {
		return def
	}
	switch v := val.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		var i int
		if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
			return i
		}
	}
	return def
}
