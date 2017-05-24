package main

import (
    "sync"
    "errors"
)

const SUBDB_COUNT = 32
const DEFAULT_VALUE int32 = 0

type subDatabase struct {
    items map[string]int32
    sync.RWMutex
}

type Database struct {
    sub []*subDatabase
}

func NewDatabse(ServerConfig ) *Database {
    database := new(Database)
    for i := 0; i < SUBDB_COUNT; i++ {
        database.sub[i] = &subDatabase{items: make(map[string]int32)}
    }
    return database
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (db *Database) getSubDatabase(key string) *subDatabase {
    return db.sub[uint(fnv32(key))%uint(SUBDB_COUNT)]
}

func (db *Database) Get(key string) (int32, error) {
    sub := db.getSubDatabase(key)
    sub.RLock()
    val, ok := subdb.items[key]
    if !ok {
        val = 0
    }
    subdb.RUnlock()
    return val, nil
}

func (db *Database) Set(key string, val int32) (int32, error) {
    subdb := db.getSubDatabase(key)
    subdb.Lock()
    _ = db.assertValueExist(subdb, key, false)
    subdb.items[key] = val
    subdb.Unlock()
    return val, nil
}

func (db *Database) Increase(key string, delta int32) (int32, error) {
    subdb := db.getSubDatabase(key)
    subdb.Lock()
    _ = db.assertValueExist(subdb, key, false)
    subdb.items[key] += delta
    subdb.Unlock()
    return 0, nil
}

func (db *Database) Decrease(key string, delta int32) (int32, error) {
    subdb := db.getSubDatabase(key)
    subdb.Lock()
    defer subdb.Unlock()

    originVal := db.assertValueExist(subdb, key, false)
    if originVal < delta {
        return originVal, errors.New("val < delta")
    }
    subdb.items[key] -= delta
    return 0, nil
}

func (db *Database) Transfer(fromKey string, toKey string, delta int32) (int32, error) {
    fromdb := db.getSubDatabase(fromKey)
    todb := db.getSubDatabase(toKey)

    fromdb.Lock()
    defer fromdb.Unlock()
    if fromdb != todb {
        todb.Lock()
        defer todb.Unlock()
    }

    originVal := db.assertValueExist(fromdb, fromKey, false)
    if originVal < delta {
        return originVal, errors.New("val < delta")
    }
    fromdb.items[fromKey] -= delta

    _ = db.assertValueExist(todb, toKey, false)
    todb.items[toKey] += delta

    return 0, nil
}

