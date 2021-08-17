package jira

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"

	"github.com/openshift/ci-search/walk"
)

type CommentDiskStore struct {
	base   string
	maxAge time.Duration

	queue workqueue.Interface
}

type CommentAccessor interface {
	Get(id string) (*CardComments, bool)
}

func NewCommentDiskStore(path string, maxAge time.Duration) *CommentDiskStore {
	return &CommentDiskStore{
		base:   path,
		maxAge: maxAge,
		queue:  workqueue.NewNamed("comment_disk"),
	}
}

func (s *CommentDiskStore) Run(ctx context.Context, lister *CardLister, store CommentAccessor, disableWrite bool) {
	defer klog.V(2).Infof("Comment disk worker exited")
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		for {
			obj, done := s.queue.Get()
			if done {
				return
			}
			if disableWrite {
				s.queue.Done(obj)
				return
			}
			comments, ok := store.Get(obj.(string))
			if !ok {
				s.queue.Done(obj)
				klog.V(5).Infof("No comments for %s", obj.(string))
				continue
			}
			s.queue.Done(obj)
			card, err := lister.Get(obj.(string))
			if err != nil {
				card = &Card{ObjectMeta: comments.ObjectMeta, Info: comments.Info}
			}
			if err := s.write(card, comments); err != nil {
				klog.Errorf("failed to write card: %v", err)
			}
		}
	}, time.Second)
}

func (s *CommentDiskStore) NotifyChanged(id string) {
	s.queue.Add(id)
}

func (s *CommentDiskStore) Sync(keys []string) ([]*CardComments, error) {
	var known sets.String
	if keys != nil {
		known = sets.NewString(keys...)
	}
	start := time.Now()
	mustExpire := s.maxAge != 0
	expiredAt := start.Add(-s.maxAge)
	tempExpiredAfter := start.Add(-15 * time.Minute)

	cards := make([]*CardComments, 0, 2048)

	err := walk.Walk(s.base, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}

		if mustExpire && expiredAt.After(info.ModTime()) {
			os.Remove(path)
			klog.V(5).Infof("File expired: %s", path)
			return nil
		}
		relPath, err := filepath.Rel(s.base, path)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath)
		if strings.HasPrefix(info.Name(), "z-card-") {
			if tempExpiredAfter.After(info.ModTime()) {
				os.Remove(path)
				klog.V(5).Infof("Temporary file expired: %s", path)
				return nil
			}
			return nil
		}
		if !strings.HasPrefix(info.Name(), "card-") {
			return nil
		}
		idString := info.Name()[5:]

		if known != nil && !known.Has(idString) {
			os.Remove(path)
			klog.V(5).Infof("Card is not in the known list: %s", path)
			return nil
		}

		comments, err := readCardComments(path)
		if err != nil {
			return fmt.Errorf("unable to read %q: %v", path, err)
		}
		if len(comments.Comments) == 0 {
			os.Remove(path)
			klog.V(5).Infof("Card has no comments: %s", path)
			return nil
		}
		if comments.Name != idString {
			return fmt.Errorf("file has path %q but ID is %s and name is %s", path, idString, comments.Name)
		}
		comments.CreationTimestamp.Time = comments.Comments[0].CreationTime.Time
		comments.RefreshTime = info.ModTime()
		cards = append(cards, comments)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cards, nil
}

func (s *CommentDiskStore) DeleteCard(card *Card) error {
	_, path := s.pathForCard(card)
	return os.Remove(path)
}

func (s *CommentDiskStore) CloseCard(card *CardComments) error {

	clone := card.DeepCopyObject().(*CardComments)
	clone.Info.Status = "CLOSED"
	if err := s.write(&Card{ObjectMeta: clone.ObjectMeta, Info: clone.Info}, clone); err != nil {
		return fmt.Errorf("could not mark card %s closed due to write error: %v", clone.Info.ID, err)
	}
	return nil
}
func (s *CommentDiskStore) pathForCard(card *Card) (string, string) {
	return filepath.Join(s.base, fmt.Sprintf("z-card-%s", card.Info.ID)),
		filepath.Join(s.base, fmt.Sprintf("card-%s", card.Info.ID))
}

func lineSafe(s string) string {
	return strings.TrimSpace(strings.Replace(s, "\n", " ", -1))
}
func arrayLineSafe(arr []string, delim string) string {
	inputs := make([]string, 0, len(arr))
	for _, s := range arr {
		inputs = append(inputs, lineSafe(s))
	}
	return strings.Join(inputs, delim)
}

func (s *CommentDiskStore) write(card *Card, comments *CardComments) error {
	path, finalPath := s.pathForCard(card)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(f)

	if _, err := fmt.Fprintf(
		w,
		"Card %s: %s\nStatus: %s %s\nSeverity: %s\nCreator: %s\nAssigned To: %s\nKeywords: %s\nComponent: %s\n---\n",
		card.Info.ID,
		lineSafe(card.Info.Summary),
		lineSafe(card.Info.Status),
		lineSafe(card.Info.Priority),
		lineSafe(card.Info.Resolution),
		lineSafe(card.Info.Creator),
		lineSafe(card.Info.AssignedTo),
		arrayLineSafe(card.Info.Keywords, ", "),
		arrayLineSafe(card.Info.TargetRelease, ", "),
	); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}

	for _, comment := range comments.Comments {
		escapedText := strings.ReplaceAll(strings.ReplaceAll(comment.Text, "\x00", " "), "\x1e", " ")
		if _, err := fmt.Fprintf(
			w,
			"Comment %s by %s at %s\n%s\n\x1e",
			comment.ID,
			strings.TrimSpace(comment.Creator),
			timeToRV(comment.CreationTime),
			escapedText,
		); err != nil {
			f.Close()
			os.Remove(path)
			return err
		}
	}

	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return err
	}
	if err := os.Chtimes(path, comments.RefreshTime, comments.RefreshTime); err != nil {
		os.Remove(path)
		return err
	}
	return os.Rename(path, finalPath)
}

var (
	reDiskCommentsLineHeader        = regexp.MustCompile(`^Card (\d+): (.*)$`)
	reDiskCommentsLineCommentHeader = regexp.MustCompile(`^Comment (\d+) by (.+) at (\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ)$`)
)

const (
	cardCommentDelimiter = "\x1e"
)

func readCardComments(path string) (*CardComments, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var card CardComments
	comments := make([]CardComment, 0, 4)

	// allow lines of up to 4MB
	delim := []byte(cardCommentDelimiter)
	br := bufio.NewReader(f)
	sr := bufio.NewScanner(br)
	sr.Buffer(make([]byte, 4*1024), 4*1024*1024)
	phase := 0
	sr.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		switch phase {
		case 0, 1:
			return bufio.ScanLines(data, atEOF)
		case 2:
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.Index(data, delim); i >= 0 {
				// We have a full comment
				return i + len(delim), data[0:i], nil
			}
			// If we're at EOF, we have a final, non-terminated line. Return it.
			if atEOF {
				return len(data), data, nil
			}
			// Request more data.
			return 0, nil, nil
		default:
			return 0, nil, fmt.Errorf("unrecognized phase %d", phase)
		}
	})

	// PHASE 0: Header
	if !sr.Scan() {
		return nil, fmt.Errorf("%s: first line missing or malformed: %v", path, sr.Err())
	}
	m := reDiskCommentsLineHeader.FindStringSubmatch(sr.Text())
	if m == nil {
		return nil, fmt.Errorf("%s: first line must be of the form 'Card NUMBER: DESCRIPTION'", path)
	}

	card.Name = m[1]
	card.Info.ID = m[1]
	card.Info.Summary = m[2]

	// strip the rest of the header
	var foundSeparator bool
ScanHeader:
	for sr.Scan() {
		text := sr.Text()
		switch {
		case strings.HasPrefix(text, "Status: "):
			parts := strings.SplitN(text, " ", 3)
			if len(parts) < 2 || len(parts[1]) == 0 {
				continue
			}
			card.Info.Status = parts[1]
			if len(parts) > 2 {
				card.Info.Resolution = parts[2]
			}
		case strings.HasPrefix(text, "Creator: "):
			parts := strings.SplitN(text, " ", 2)
			if len(parts) < 1 || len(parts[1]) == 0 {
				continue
			}
			card.Info.Creator = parts[1]
		case strings.HasPrefix(text, "Assigned To: "):
			parts := strings.SplitN(text, " ", 3)
			if len(parts) < 2 || len(parts[2]) == 0 {
				continue
			}
			card.Info.AssignedTo = parts[2]
		case strings.HasPrefix(text, "Keywords: "):
			parts := strings.SplitN(text, " ", 2)
			if len(parts) < 1 || len(parts[1]) == 0 {
				continue
			}
			card.Info.Keywords = strings.Split(parts[1], ", ")
			card.Info.TargetRelease = strings.Split(parts[2], ", ")
		case strings.HasPrefix(text, "Component: "):
			parts := strings.SplitN(text, " ", 2)
			if len(parts) < 1 || len(parts[1]) == 0 {
				continue
			}
			card.Info.Component = strings.Split(parts[1], ", ")
		case text == "---":
			foundSeparator = true
			break ScanHeader
		}
	}
	if err := sr.Err(); err != nil {
		return nil, fmt.Errorf("%s: unable to read stored card: %v", path, err)
	}
	if !foundSeparator {
		return nil, fmt.Errorf("%s: unable to read stored card: no body separator", path)
	}

	phase = 1
	var comment CardComment
	for sr.Scan() {
		switch phase {
		case 1:
			m := reDiskCommentsLineCommentHeader.FindStringSubmatch(sr.Text())
			if m == nil {
				return nil, fmt.Errorf("%s: comment header line %d must be of the form 'Comment ID by AUTHOR at DATE': %q", path, len(comments)+1, sr.Text())
			}

			comment.ID = m[1]
			comment.Creator = m[2]

			if err := comment.CreationTime.UnmarshalQueryParameter(m[3]); err != nil {
				return nil, fmt.Errorf("%s: comment header line must have an RFC3339 DATE: %v", path, err)
			}
			comment.Time = comment.CreationTime

			phase = 2

		case 2:
			comment.Text = strings.TrimSuffix(sr.Text(), "\n")
			comments = append(comments, comment)
			comment = CardComment{}

			phase = 1

		default:
			return nil, fmt.Errorf("%s: programmer error, unexpected phase %d", path, phase)
		}
	}
	if err := sr.Err(); err != nil {
		return nil, fmt.Errorf("%s: failed to parse comments: %v", path, err)
	}

	card.Comments = comments
	return setFieldsFromCardComments(&card), nil
}
