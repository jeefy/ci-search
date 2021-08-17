package jira

import (
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type CardList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []Card
}

type Card struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Info CardInfo
}

type CardComments struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	Info        CardInfo
	RefreshTime time.Time
	Comments    []CardComment
}

type Error struct {
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

type CardInfoList struct {
	Cards  []CardInfo    `json:"cards"`
	Faults []interface{} `json:"faults"`
}

type IDString string

type CardCommentsList struct {
	Cards map[IDString]CardCommentInfo `json:"cards"`
}

type CardCommentInfo struct {
	Comments []CardComment `json:"comments"`
}

type CardComment struct {
	ID           string      `json:"id"`
	IsPrivate    bool        `json:"is_private"`
	Creator      string      `json:"creator"`
	CreationTime metav1.Time `json:"creation_time"`
	Time         metav1.Time `json:"time"`
	Text         string      `json:"text"`
}

type CardInfo struct {
	ID             string      `json:"id"`
	Project        string      `json:"project"`
	Status         string      `json:"status"`
	Resolution     string      `json:"resolution"`
	Priority       string      `json:"priority"`
	Summary        string      `json:"summary"`
	Keywords       []string    `json:"keywords"`
	Creator        string      `json:"creator"`
	Component      []string    `json:"component"`
	AssignedTo     string      `json:"assigned_to"`
	CreationTime   metav1.Time `json:"creation_time"`
	LastChangeTime metav1.Time `json:"last_change_time"`
	TargetRelease  []string    `json:"target_release"`
}

type SearchCardsArgs struct {
	LastChangeTime time.Time
	IDs            []string
	Projects       []string
	Quicksearch    string

	Limit  int
	Offset int
}

func (args SearchCardsArgs) ToJQL() string {
	// issueFunction in lastComment("before -7d") and created >= -7d
	jql := "(issuetype = BUG OR issuetype = STORY OR issuetype = TASK) "
	if len(args.Projects) > 0 {
		jql = fmt.Sprintf("%s AND project in (%s) ", jql, strings.Trim(strings.Replace(fmt.Sprint(args.IDs), " ", ",", -1), "[]"))
	} else {
		// Set a sane default
		jql = fmt.Sprintf("%s AND project in (SDCICD, OSD, SDA) ", jql)
	}
	if len(args.IDs) > 0 {
		jql = fmt.Sprintf("%s AND id in (%s)", jql, strings.Trim(strings.Replace(fmt.Sprint(args.IDs), " ", ",", -1), "[]"))
	}
	if args.Quicksearch != "" {
		jql = fmt.Sprintf("%s AND (text ~ \"%s\" OR comment ~ \"%s\") ", jql, args.Quicksearch, args.Quicksearch)
	}

	jql = fmt.Sprintf("%s AND (createdDate > \"%s\" OR updatedDate > \"%s\")", jql, args.LastChangeTime.Format("2006-01-02 15:04"), args.LastChangeTime.Format("2006-01-02 15:04"))

	fmt.Printf("JQL: %s\n", jql)

	return jql

}

func NewCard(info *CardInfo) *Card {
	return &Card{
		ObjectMeta: metav1.ObjectMeta{
			Name:              info.ID,
			UID:               types.UID(info.ID),
			CreationTimestamp: info.CreationTime,
			ResourceVersion:   timeToRV(info.LastChangeTime),
		},
		Info: *info,
	}
}

func NewCardComments(id string, info *CardCommentInfo) *CardComments {
	return setFieldsFromCardComments(&CardComments{
		ObjectMeta: metav1.ObjectMeta{
			Name: id,
			UID:  types.UID(id),
		},
		Comments: info.Comments,
	})
}

func setFieldsFromCardComments(card *CardComments) *CardComments {
	var oldest, newest time.Time
	for _, comment := range card.Comments {
		if oldest.IsZero() || comment.CreationTime.Time.Before(oldest) {
			oldest = comment.CreationTime.Time
		}
		if comment.Time.Time.After(newest) {
			newest = comment.CreationTime.Time
		}
	}
	card.CreationTimestamp.Time = oldest
	card.ResourceVersion = timeToRV(metav1.Time{Time: newest})
	return card
}

func timeToRV(t metav1.Time) string {
	s, _ := t.MarshalQueryParameter()
	return s
}

func NewCardList(cards *CardInfoList, includeFn func(*CardInfo) bool) *CardList {
	var change time.Time
	items := make([]Card, 0, len(cards.Cards))
	for _, info := range cards.Cards {
		if includeFn != nil && !includeFn(&info) {
			continue
		}
		if t := info.LastChangeTime.Time; change.Before(t) {
			change = t
		}
		items = append(items, Card{
			ObjectMeta: metav1.ObjectMeta{
				Name:              info.ID,
				UID:               types.UID(info.ID),
				CreationTimestamp: info.CreationTime,
				ResourceVersion:   timeToRV(info.LastChangeTime),
			},
			Info: info,
		})
	}
	list := &CardList{Items: items}
	if !change.IsZero() {
		list.ResourceVersion = timeToRV(metav1.Time{Time: change})
	}
	return list
}

func (b Card) DeepCopyObject() runtime.Object {
	copied := b
	copied.ObjectMeta = *b.ObjectMeta.DeepCopy()
	return &copied
}

func (b CardComments) DeepCopyObject() runtime.Object {
	copied := b
	copied.ObjectMeta = *b.ObjectMeta.DeepCopy()
	if b.Comments != nil {
		copied.Comments = make([]CardComment, len(b.Comments))
		copy(copied.Comments, b.Comments)
	}
	return &copied
}

func (b *CardList) DeepCopyObject() runtime.Object {
	copied := *b
	if b.Items != nil {
		copied.Items = make([]Card, len(b.Items))
		for i := range b.Items {
			copied.Items[i] = *b.Items[i].DeepCopyObject().(*Card)
		}
	}
	return &copied
}
