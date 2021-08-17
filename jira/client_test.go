package jira

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func init() {
	missing := []string{}
	if os.Getenv("JIRA_URL") == "" {
		missing = append(missing, "JIRA_URL")
	}
	if os.Getenv("JIRA_USERNAME") == "" {
		missing = append(missing, "JIRA_USERNAME")
	}
	if os.Getenv("JIRA_PASSWORD") == "" {
		missing = append(missing, "JIRA_PASSWORD")
	}

	if len(missing) > 0 {
		log.Fatalf("Required configs are missing: %v", missing)
	}
}

func TestClient_CardsByID(t *testing.T) {
	c := NewClient()
	got, err := c.CardsByID(context.TODO(), "13375439")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Faults) > 0 {
		t.Fatalf("%#v", got.Faults)
	}
	if len(got.Cards) != 1 {
		t.Fatalf("%#v", got.Cards)
	}
	t.Logf("%#v", got.Cards[0])
}
func TestClient_CardCommentsByID(t *testing.T) {
	c := NewClient()
	got, err := c.CardCommentsByID(context.TODO(), "12979320", "13271796")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Cards) != 2 {
		t.Fatalf("%#v", got.Cards)
	}
	comments, ok := got.Cards["12979320"]
	if !ok {
		t.Fatal("missing expected comments")
	}
	if len(comments.Comments) == 0 {
		t.Fatal("missing expected comments")
	}
	t.Logf("%#v", comments.Comments)
}

func TestClient_Search(t *testing.T) {
	c := NewClient()
	got, err := c.SearchCards(context.TODO(), SearchCardsArgs{
		LastChangeTime: time.Unix(1584221752, 0),
		Quicksearch:    "Signal quality improvements June 2020",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Faults) > 0 {
		t.Fatalf("%#v", got.Faults)
	}
	if len(got.Cards) != 1 {
		t.Logf("Actual count: %d", len(got.Cards))
		t.Fatalf("%#v", got.Cards)
	}
	t.Logf("%#v", got.Cards[0])
}
