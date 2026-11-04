package core

import (
	"container/list"
	"crypto/sha256"
	"time"
)

type appliedMessage struct {
	key     [sha256.Size]byte
	expires time.Time
}

// messageDedupe belongs to the client's single incoming worker, not its socket,
// and therefore survives reconnects. Only fixed-size hashes are retained. Entries
// expire from their successful application time; duplicates do not extend it.
type messageDedupe struct {
	ttl     time.Duration
	limit   int
	entries map[[sha256.Size]byte]*list.Element
	ordered list.List
}

func newMessageDedupe(ttl time.Duration, limit int) *messageDedupe {
	if ttl <= 0 || limit <= 0 {
		return nil
	}
	return &messageDedupe{ttl: ttl, limit: limit, entries: make(map[[sha256.Size]byte]*list.Element)}
}

func (d *messageDedupe) removeOldest() {
	front := d.ordered.Front()
	delete(d.entries, front.Value.(appliedMessage).key)
	d.ordered.Remove(front)
}

func (d *messageDedupe) seen(key [sha256.Size]byte) bool {
	now := time.Now()
	for front := d.ordered.Front(); front != nil; front = d.ordered.Front() {
		if now.Before(front.Value.(appliedMessage).expires) {
			break
		}
		d.removeOldest()
	}
	_, ok := d.entries[key]
	return ok
}

func (d *messageDedupe) remember(key [sha256.Size]byte) {
	if d.seen(key) {
		return
	}
	if len(d.entries) >= d.limit {
		d.removeOldest()
	}
	d.entries[key] = d.ordered.PushBack(appliedMessage{key: key, expires: time.Now().Add(d.ttl)})
}
