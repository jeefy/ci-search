package jira

import (
	"context"
	"log"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func TestListWatcher(t *testing.T) {
	c := NewClient()
	log.Println("Created new client")
	if len(c.URL) == 0 || len(c.Username) == 0 || len(c.Password) == 0 {
		t.Skip("Must specifify a JIRA_URL, JIRA_USERNAME, and JIRA_PASSWORD")
	}

	lw := &ListWatcher{
		client:   c,
		interval: 1 * time.Minute,
		argsFn: func(metav1.ListOptions) SearchCardsArgs {
			return SearchCardsArgs{
				Quicksearch: "Signal quality improvements June 2020",
			}
		},
	}
	log.Println("Created new listwatcher")
	obj, err := lw.List(metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Listed new listwatcher")
	list := obj.(*CardList)
	if len(list.Items) == 0 {
		t.Fatalf("%#v", list.Items)
	}
	t.Logf("%#v", list.Items)
	log.Println("Cast list results to CardList")

	w, err := lw.Watch(metav1.ListOptions{ResourceVersion: list.ResourceVersion})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Created new Watch")
	defer w.Stop()
	count := 0
	for event := range w.ResultChan() {
		log.Println("Result Chaaan~!")
		log.Printf("%v", event)
		t.Logf("%#v", event)
		count++
		if count > 2 {
			break
		}
	}
	log.Println("Donedonedone")
}

func TestInformer(t *testing.T) {
	c := NewClient()
	if len(c.URL) == 0 || len(c.Username) == 0 || len(c.Password) == 0 {
		t.Skip("Must specifify a JIRA_URL, JIRA_USERNAME, and JIRA_PASSWORD")
	}

	informer := NewInformer(c, 30*time.Second, 0, 1*time.Minute, func(metav1.ListOptions) SearchCardsArgs {
		return SearchCardsArgs{
			Quicksearch: "Signal quality improvements June 2020",
		}
	}, func(*CardInfo) bool { return true })
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if card, ok := obj.(*Card); ok {
				klog.Infof("ADD %s", card.Name)
			} else {
				klog.Infof("ADD %#v", obj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if card, ok := obj.(*Card); ok {
				klog.Infof("DELETE %s", card.Name)
			} else {
				klog.Infof("DELETE %#v", obj)
			}
		},
	})
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	go informer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		t.Fatalf("unable to sync")
	}

	time.Sleep(2 * time.Minute)
}
