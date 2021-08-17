package jira

import (
	"context"
	"reflect"
	"sync"
	"time"

	"golang.org/x/time/rate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

type CommentStore struct {
	store          cache.Store
	persistedStore PersistentCommentStore
	hasSynced      []cache.InformerSynced
	client         *Client
	includePrivate bool

	queue workqueue.Interface

	refreshInterval time.Duration
	maxBatch        int
	rateLimit       *rate.Limiter

	// lock keeps the comment list in sync with the card list
	lock sync.Mutex
}

type PersistentCommentStore interface {
	Sync(keys []string) ([]*CardComments, error)
	NotifyChanged(id string)
	DeleteCard(*Card) error
	CloseCard(*CardComments) error
}

func NewCommentStore(client *Client, refreshInterval time.Duration, includePrivate bool, persisted PersistentCommentStore) *CommentStore {
	s := &CommentStore{
		store:          cache.NewStore(cache.MetaNamespaceKeyFunc),
		persistedStore: persisted,
		client:         client,

		includePrivate: includePrivate,

		queue: workqueue.NewNamed("comment_store"),

		refreshInterval: refreshInterval,
		rateLimit:       rate.NewLimiter(rate.Every(15*time.Second), 3),
		maxBatch:        250,
	}
	return s
}

type Stats struct {
	Cards int
}

func (s *CommentStore) Stats() Stats {
	return Stats{
		Cards: len(s.store.ListKeys()),
	}
}

func (s *CommentStore) Get(id string) (*CardComments, bool) {
	item, ok, err := s.store.GetByKey(id)
	if err != nil || !ok {
		return nil, false
	}
	return item.(*CardComments), true
}

func (s *CommentStore) Run(ctx context.Context, informer cache.SharedInformer) error {
	defer klog.V(2).Infof("Comment worker exited")
	if s.refreshInterval == 0 {
		return nil
	}
	if s.persistedStore != nil {
		// load the full state into the store
		list, err := s.persistedStore.Sync(nil)
		if err != nil {
			klog.Errorf("Unable to load initial comment state: %v", err)
		}
		for _, card := range list {
			s.store.Add(card.DeepCopyObject())
		}
		klog.V(4).Infof("Loaded %d cards from disk", len(list))

		// wait for card cache to fill, then prune the list
		// done := ctx.Done()
		// if !cache.WaitForCacheSync(done, informer.HasSynced) {
		// 	return ctx.Err()
		// }
		// list, err = persisted.Sync(informer.GetStore().ListKeys())
		// if err != nil {
		// 	klog.Errorf("Unable to load initial comment state: %v", err)
		// }
		// klog.V(4).Infof("Prune disk to %d cards", len(list))
	}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    s.cardAdd,
		DeleteFunc: s.cardDelete,
		UpdateFunc: func(_, new interface{}) { s.cardUpdate(new) },
	})

	klog.V(5).Infof("Running comment store")

	// periodically put all cards that haven't been refreshed in the last interval
	// into the queue
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		now := time.Now()
		refreshAfter := now.Add(-s.refreshInterval)
		var count int
		for _, obj := range s.store.List() {
			comments := obj.(*CardComments)
			if comments.RefreshTime.Before(refreshAfter) {
				s.queue.Add(comments.Name)
				count++
			}
		}
		klog.V(5).Infof("Refreshed %d comments older than %s", count, s.refreshInterval.String())
	}, s.refreshInterval/4)

	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := s.run(ctx); err != nil {
			klog.Errorf("Error syncing comments: %v", err)
		}
	}, time.Second)

	return ctx.Err()
}

func (s *CommentStore) run(ctx context.Context) error {
	done := ctx.Done()
	for {
		l := s.queue.Len()
		if l == 0 {
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-done:
				return nil
			}
		}
		if err := s.rateLimit.Wait(ctx); err != nil {
			return err
		}

		if l > s.maxBatch {
			l = s.maxBatch
		}

		var cardIDs []string
		cardIDs = make([]string, 0, l)
		for l > 0 {
			k, done := s.queue.Get()
			if done {
				return ctx.Err()
			}
			s.queue.Done(k)
			cardIDs = append(cardIDs, k.(string))
			l--
		}

		now := time.Now()
		klog.V(7).Infof("Fetching %d comments", len(cardIDs))
		cardComments, err := s.client.CardCommentsByID(ctx, cardIDs...)
		if err != nil {
			klog.Warningf("comment store failed to retrieve comments: %v", err)
			continue
		}
		s.filterComments(cardComments)
		s.mergeCards(cardComments, now)
	}
}

func (s *CommentStore) filterComments(cardComments *CardCommentsList) {
	if s.includePrivate {
		return
	}
	for id, comments := range cardComments.Cards {
		if len(comments.Comments) == 0 {
			continue
		}
		copied := make([]CardComment, 0, len(comments.Comments))
		for _, comment := range comments.Comments {
			if comment.IsPrivate {
				continue
			}
			copied = append(copied, comment)
		}
		if len(copied) == len(comments.Comments) {
			continue
		}
		if len(copied) == 0 {
			comment := comments.Comments[0]
			comment.Text = "<private comment>"
			comment.Creator = "Unknown"
			comment.IsPrivate = false
			copied = append(copied, comment)
		}
		klog.V(7).Infof("Filtered %d/%d private comments from card %s", len(comments.Comments)-len(copied), len(comments.Comments), id)
		comments.Comments = copied
		cardComments.Cards[id] = comments
	}
}

func (s *CommentStore) mergeCards(cardComments *CardCommentsList, now time.Time) {
	var total int
	defer func() { klog.V(7).Infof("Updated %d comment records", total) }()
	s.lock.Lock()
	defer s.lock.Unlock()

	for id, comments := range cardComments.Cards {
		obj, ok, err := s.store.GetByKey(string(id))
		if !ok || err != nil {
			klog.V(5).Infof("Card %s is not in cache", id)
			continue
		}
		existing := obj.(*CardComments)
		if existing.RefreshTime.After(now) {
			klog.V(5).Infof("Card refresh time is in the future: %v >= %v", existing, now)
			continue
		}

		updated := NewCardComments(string(id), &comments)
		updated.Info = existing.Info
		updated.RefreshTime = now
		s.store.Update(updated)
		if s.persistedStore != nil {
			s.persistedStore.NotifyChanged(string(id))
		}
		total++
	}
}

func (s *CommentStore) cardAdd(obj interface{}) {
	card, ok := obj.(*Card)
	if !ok {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	obj, ok, err := s.store.GetByKey(card.Name)
	if err != nil {
		klog.Errorf("Unexpected error retrieving %q from store: %v", card.Name, err)
	}
	if ok {
		existing := obj.(*CardComments).DeepCopyObject().(*CardComments)
		existing.Info = card.Info
		if err := s.store.Update(existing); err != nil {
			klog.Errorf("Unable to merge added card from informer: %v", err)
			return
		}
	} else {
		if err := s.store.Add(&CardComments{
			ObjectMeta: metav1.ObjectMeta{Name: card.Name},
			Info:       card.Info,
		}); err != nil {
			klog.Errorf("Unable to add card from informer: %v", err)
			return
		}
	}
	s.queue.Add(card.Name)
}

func (s *CommentStore) cardUpdate(obj interface{}) {
	card, ok := obj.(*Card)
	if !ok {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	existing, ok := s.Get(card.Info.ID)
	if !ok {
		return
	}
	if reflect.DeepEqual(card.Info, existing.Info) {
		return
	}
	existing = existing.DeepCopyObject().(*CardComments)
	existing.Info = card.Info
	if err := s.store.Update(existing); err != nil {
		klog.Errorf("Unable to update card from informer: %v", err)
		return
	}
	if s.persistedStore != nil {
		s.persistedStore.NotifyChanged(card.Info.ID)
	}
}

func (s *CommentStore) cardDelete(obj interface{}) {
	var name string
	var err error
	switch t := obj.(type) {
	case cache.DeletedFinalStateUnknown:
		name = t.Key
	case *Card:
		name = t.Name
	default:
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	obj, ok, err := s.store.GetByKey(name)
	if err != nil {
		klog.Errorf("Unexpected error retrieving %q from store: %v", name, err)
		return
	}
	if !ok {
		klog.Errorf("Card %q not found in store", name)
		return
	}

	card, ok := obj.(*CardComments)
	if !ok {
		klog.Errorf("Key %q did not reference object of type CardComments: %#v", name, obj)
		return
	}
	if err := s.store.Delete(card); err != nil {
		klog.Errorf("Unable to delete card from informer: %v", err)
		return
	}
	if err := s.persistedStore.CloseCard(card); err != nil {
		klog.Errorf("Unable to close card in disk store: %v", err)
		return
	}
	s.queue.Add(name)
}
