package main

import (
    "sync"
    "errors"
    "github.com/golang/protobuf/proto"
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
    sync.Mutex
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
    db.Lock()
    defer db.Unlock()
    return db.sub[uint(fnv32(key))%uint(SUBDB_COUNT)]
}

func (db *Database) SetAtomicKey(key string, val int32, t int) error {
    subdb := db.getSubDatabase(key)
    return SetAtomic(subdb, key, val, t)
}

func SetAtomic(subdb *subDatabase, key string, val int32, t int) error {
    if t == 1 {
        subdb.items[key] = val
    } else {
        oval, ok := subdb.items[key]
        if t == 2 {
            if ok {
                subdb.items[key] += val
            } else {
                subdb.items[key] = val
            }
        } else {
            if !ok || oval < val {
                return errors.New("oval < delta")
            }
            subdb.items[key] -= val
        }
    }
    return nil
}

func (db *Database) TransferAtomicKey(fromKey string, toKey string, delta int32) error {
    fromDB := db.getSubDatabase(fromKey)
    toDB := db.getSubDatabase(toKey)
    return TransferAtomic(fromDB, toDB, fromKey, toKey, delta)
}

func TransferAtomic(fromDB *subDatabase, toDB *subDatabase, fromKey string, toKey string, delta int32) error {
    oval, okf := fromDB.items[fromKey]

    if !okf || oval < delta {
        return errors.New("val < delta")
    }
    fromDB.items[fromKey] -= delta

    _, okt := toDB.items[toKey]
    if okt {
        toDB.items[toKey] += delta
    } else {
        toDB.items[toKey] = delta
    }

    return nil
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

    SetAtomic(subdb, key, val, 1)
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

    SetAtomic(subdb, key, delta, 2)
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

    err := SetAtomic(subdb, key, delta, 3)
    if err != nil {
        return -1, nil, err
    }
    req := db.logger.Log(&pb.Transaction{Type: 4, UserID: key, Value: delta})
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

    err := TransferAtomic(fromDB, toDB, fromKey, toKey, delta)
    if err != nil {
        return -1, nil, err
    }
    req := db.logger.Log(&pb.Transaction{Type: 5, FromID: fromKey, ToID: toKey, Value: delta})
    return 0, req, nil
}

func (db *Database) DumpSnapshot() ([]byte, error) {
    db.Lock()
    for _, subdb := range db.sub {
        subdb.RLock()
    }
    db.Unlock()

    snapshot := new(pb.ServerSnapshot)
    snapshot.Data = make(map[string]int32)
    for _, subdb := range db.sub {
        for k, v := range subdb.items {
            snapshot.Data[k] = v
        }
    }

    for _, subdb := range db.sub {
        subdb.RUnlock()
    }

    bytes, err := proto.Marshal(snapshot)
    return bytes, err
}

func (db *Database) LoadSnapshot(data []byte) error {
    snapshot := new(pb.ServerSnapshot)
    err := proto.Unmarshal(data, snapshot)
    if err != nil {
        return err
    }
    db.Lock()
    for _, subdb := range db.sub {
        subdb.Lock()
    }
    db.Unlock()

    for k, v := range snapshot.Data {
        db.getSubDatabase(k).items[k] = v
    }

    for _, subdb := range db.sub {
        subdb.Unlock()
    }

    return nil
}

