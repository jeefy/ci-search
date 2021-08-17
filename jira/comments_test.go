package jira

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func TestCommentStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "disk")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := NewClient()
	if len(c.URL) == 0 || len(c.Username) == 0 || len(c.Password) == 0 {
		t.Skip("Must specifify a JIRA_URL, JIRA_USERNAME, and JIRA_PASSWORD")
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	informer := NewInformer(c, 30*time.Second, 0, 1*time.Minute, func(metav1.ListOptions) SearchCardsArgs {
		return SearchCardsArgs{
			Quicksearch: "Signal quality improvements June 2020",
		}
	}, func(*CardInfo) bool { return true })
	lister := NewCardLister(informer.GetIndexer())
	diskStore := NewCommentDiskStore(dir, 10*time.Minute)
	store := NewCommentStore(c, 5*time.Minute, false, diskStore)

	go informer.Run(ctx.Done())
	go store.Run(ctx, informer)
	go diskStore.Run(ctx, lister, store, false)

	klog.Infof("waiting for caches to sync")
	cache.WaitForCacheSync(ctx.Done(), informer.HasSynced)

	for {
		cards, err := lister.List(labels.Everything())
		if err != nil {
			t.Fatal(err)
		}
		if len(cards) == 0 {
			klog.Infof("no cards")
			time.Sleep(time.Second)
			continue
		}

		var missing bool
		for _, card := range cards {
			comments, ok := store.Get(card.Info.ID)
			if !ok || comments.RefreshTime.IsZero() {
				klog.Infof("no comments for %s", card.Info.ID)
				missing = true
				continue
			}

			klog.Infof("Card %s had %d comments", card.Info.ID, len(comments.Comments))
		}
		if missing {
			time.Sleep(time.Second)
			continue
		}
		break
	}
}
