package jira

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	jira "github.com/andygrunwald/go-jira"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Client struct {
	URL      string
	Username string
	Password string
	Client   *jira.Client
}

func NewClient() *Client {
	newClient := Client{
		URL:      os.Getenv("JIRA_URL"),
		Username: os.Getenv("JIRA_USERNAME"),
		Password: os.Getenv("JIRA_PASSWORD"),
	}

	tp := jira.BasicAuthTransport{
		Username: newClient.Username,
		Password: newClient.Password,
	}

	log.Printf("#%v", newClient)

	client, err := jira.NewClient(tp.Client(), strings.TrimSpace(newClient.URL))
	if err != nil {
		log.Fatal(err)
	}
	newClient.Client = client

	return &newClient
}

func (c *Client) CardCommentsByID(ctx context.Context, cards ...string) (*CardCommentsList, error) {
	var err error
	if len(cards) == 0 {
		return &CardCommentsList{}, nil
	}

	cardList := &CardCommentsList{}
	cardList.Cards = make(map[IDString]CardCommentInfo)

	for _, cardID := range cards {
		if cardID == "" {
			return nil, fmt.Errorf("blank card ID found")
		}
		card, _, err := c.Client.Issue.Get(cardID, &jira.GetQueryOptions{})
		if err != nil {
			return nil, fmt.Errorf("can't retrive issue %s: %s", cardID, err.Error())
		}
		if card.Fields.Comments == nil {
			// No comments for this card
			continue
		}

		var cardComments []CardComment
		for _, comment := range card.Fields.Comments.Comments {
			//2020-03-30T09:40:23.000+0000
			created, err := time.Parse("2006-01-02T15:04:05.000-0700", comment.Created)
			if err != nil {
				log.Printf("Error converting time %s: %s", comment.Created, err.Error())
				continue
			}

			cardComments = append(cardComments, CardComment{
				ID: comment.ID,
				// TODO: Not 100% sure on this logic yet
				IsPrivate:    comment.Visibility.Type != "" || comment.Visibility.Value != "",
				Creator:      comment.Author.EmailAddress,
				CreationTime: v1.Time{created},
				Time:         v1.Time{created},
				Text:         comment.Body,
			})
		}

		cardList.Cards[IDString(cardID)] = CardCommentInfo{
			Comments: cardComments,
		}

	}

	return cardList, err
}

func (c *Client) SearchCards(ctx context.Context, args SearchCardsArgs) (*CardInfoList, error) {
	var cardList *CardInfoList

	issues, _, err := c.Client.Issue.Search(args.ToJQL(), &jira.SearchOptions{
		MaxResults: args.Limit,
		StartAt:    args.Offset,
	})
	if err != nil {
		return nil, err
	}

	cardList = &CardInfoList{}
	for _, i := range issues {
		issue, _, err := c.Client.Issue.Get(i.ID, &jira.GetQueryOptions{})
		if err != nil {
			log.Printf("Error retrieving full issue %s: %s", i.ID, err.Error())
		}

		createTime := v1.Time{time.Time(issue.Fields.Created)}
		updateTime := v1.Time{time.Time(issue.Fields.Updated)}

		// Assignee can be nil
		assignee := ""
		if issue.Fields.Assignee != nil {
			assignee = issue.Fields.Assignee.EmailAddress
		}

		cardList.Cards = append(cardList.Cards, CardInfo{
			ID:             issue.ID,
			Project:        issue.Fields.Project.Key,
			Status:         issue.Fields.Status.Description,
			Priority:       issue.Fields.Priority.Name,
			Summary:        issue.Fields.Summary,
			Creator:        issue.Fields.Creator.EmailAddress,
			AssignedTo:     assignee,
			CreationTime:   createTime,
			LastChangeTime: updateTime,
		})
	}

	return cardList, err
}

func (c *Client) CardsByID(ctx context.Context, cards ...string) (*CardInfoList, error) {
	return c.SearchCards(ctx, SearchCardsArgs{IDs: cards})
}

type ClientError struct {
	Err Error
}

func (e *ClientError) Error() string {
	return e.Err.Message
}
