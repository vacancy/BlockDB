package main

import (
    "sync"
    "errors"
    pb "../protobuf/go"
)

const SUBDB_COUNT = 32

type subDatabase struct {
    items map[string]int32
    sync.RWMutex
}

type Database struct {
    sub []*subDatabase
    config *ServerConfig
    logger *Logger
}

func NewDatabse(conf *ServerConfig, logger *Logger) *Database {
    database := new(Database)
    database.sub = make([]*subDatabase, SUBDB_COUNT)
    for i := 0; i < SUBDB_COUNT; i++ {
        database.sub[i] = &subDatabase{items: make(map[string]int32)}
    }
    database.config = conf
    database.logger = logger
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

func checkKey(key string) bool {
    // TODO:: FUCK
    return true
}

func (db *Database) getSubDatabase(key string) *subDatabase {
    return db.sub[uint(fnv32(key))%uint(SUBDB_COUNT)]
}

func (db *Database) Get(key string) (int32, error) {
    if !checkKey(key) {
        return -1, errors.New("invalid key/val")
    }

    subdb := db.getSubDatabase(key)
    subdb.RLock()
    defer subdb.RUnlock()

    val, ok := subdb.items[key]
    if !ok {
        val = 0
    }
    return val, nil
}

func (db *Database) Set(key string, val int32) (int32, *LogRequest, error) {
    if !checkKey(key) || val < 0 {
        return -1, nil, errors.New("invalid key/val")
    }

    subdb := db.getSubDatabase(key)
    subdb.Lock()
    defer subdb.Unlock()

    subdb.items[key] = val
    req := db.logger.Log(&pb.Transaction{Type: 2, UserID: key, Value: val})
    return val, req, nil
}

func (db *Database) Increase(key string, delta int32) (int32, *LogRequest, error) {
    if !checkKey(key) || delta < 0 {
        return -1, nil, errors.New("invalid key/val")
    }

    subdb := db.getSubDatabase(key)
    subdb.Lock()
    defer subdb.Unlock()

    _, ok := subdb.items[key]
    if ok {
        subdb.items[key] += delta
    } else {
        subdb.items[key] = delta
    }
    req := db.logger.Log(&pb.Transaction{Type: 3, UserID: key, Value: delta})
    return 0, req, nil
}

func (db *Database) Decrease(key string, delta int32) (int32, *LogRequest, error) {
    if !checkKey(key) || delta < 0 {
        return -1, nil, errors.New("invalid key/val")
    }

    subdb := db.getSubDatabase(key)
    subdb.Lock()
    defer subdb.Unlock()

    oval, ok := subdb.items[key]
    if !ok || oval < delta {
        return -1, nil, errors.New("oval < delta")
    }
    req := db.logger.Log(&pb.Transaction{Type: 4, UserID: key, Value: delta})
    subdb.items[key] -= delta
    return 0, req, nil
}

func (db *Database) Transfer(fromKey string, toKey string, delta int32) (int32, *LogRequest, error) {
    if !checkKey(fromKey) || !checkKey(toKey) || delta < 0 {
        return -1, nil, errors.New("invalid key/val")
    }

    fromDB := db.getSubDatabase(fromKey)
    toDB := db.getSubDatabase(toKey)

    fromDB.Lock()
    defer fromDB.Unlock()
    if fromDB != toDB {
        toDB.Lock()
        defer toDB.Unlock()
    }

    oval, okf := fromDB.items[fromKey]

    if !okf || oval < delta {
        return -1, nil, errors.New("val < delta")
    }
    fromDB.items[fromKey] -= delta

    _, okt := toDB.items[toKey]
    if okt {
        toDB.items[toKey] += delta
    } else {
        toDB.items[toKey] = delta
    }

    req := db.logger.Log(&pb.Transaction{Type: 5, FromID: fromKey, ToID: toKey, Value: delta})
    return 0, req, nil
}

