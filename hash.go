package gohash

import (
	"github.com/spaolacci/murmur3"
	"log"
)

type Record struct {
	key *string
	value *string
	deleted bool
}

type Hash struct {
	size, capacity uint32
	data []Record
}

func (h *Hash) Init(capacity uint32) {
	h.size = 0
	h.capacity = capacity
	h.data = make([]Record, capacity)
}

func (h *Hash) Set(key, value *string) {
	record := h.getRecord(key, true)
	if record.key == nil {
		h.size++
	}
	record.key = key
	record.value = value
}

func (h *Hash) Get(key *string) *string {
	record := h.getRecord(key, false)
	if record == nil {
		return nil
	}
	return record.value
}

func (h *Hash) Unset(key *string) {
	record := h.getRecord(key, false)
	if record == nil {
		return
	}
	record.key = nil
	record.value = nil
	record.deleted = true
	h.size--
}

// Use quadratic probing to find records
func (h *Hash) getRecord(key *string, mayBeEmpty bool) *Record {
	log.Println("getRecord", key, mayBeEmpty)

	for offset := uint32(0); offset < h.capacity; offset++ {
		index := h.hash(key, offset * offset)
		log.Println("offset", offset, "index", index)
		record := &(h.data[index])

		if mayBeEmpty && (record.deleted || record.key == nil) {
			return record
		}
		if !mayBeEmpty && record.deleted {
			continue
		}
		if !mayBeEmpty && record.key == nil {
			return nil
		}

		if *key == *(record.key) {
			return record
		}
	}

	return nil
}

func (h *Hash) hash(key *string, offset uint32) uint32 {
	data := []byte(*key)
	return (murmur3.Sum32(data) + offset) % h.capacity
}