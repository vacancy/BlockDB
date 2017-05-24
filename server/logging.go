package main;

import (
    "fmt"
    pb "../protobuf/go"
    "github.com/golang/protobuf/jsonpb"
    "github.com/golang/protobuf/proto"
)

type FutureLogResponse chan bool 
type LogRequest struct {
    Transaction *pb.Transaction
    LogResponse FutureLogResponse
}

type Logger struct {
    Buffer [2][]*LogRequest
    BufferLength [2]int
    CurrentBuffer int
    Channel chan *LogRequest

    jsonIndex int
    jsonMarshaler jsonpb.Marshaler
    lastSnapshot int
    bufferSaved chan bool
    server *Server
    config *ServerConfig
}

func NewLogger(server *Server) (*Logger) {
    conf := server.Config
    logger := new(Logger)
    for i := 0; i < 2; i++ {
        logger.Buffer[i] = make([]*LogRequest, conf.BlockSize)
        logger.BufferLength[i] = 0
    }

    logger.CurrentBuffer = 0
    logger.Channel = make(chan *LogRequest)
    logger.server = server
    logger.config = conf
    logger.jsonIndex = 0
    logger.jsonMarshaler = jsonpb.Marshaler{EnumsAsInts: false}
    logger.lastSnapshot = 0
    logger.ResetInternal()
    logger.bufferSaved = make(chan bool, 1)
    logger.bufferSaved <- true

    return logger
}

func NewLogRequest(t *pb.Transaction) (*LogRequest) {
    req := new(LogRequest)
    req.Transaction = t
    req.LogResponse = make(FutureLogResponse, 1)

    return req
}

func (req *LogRequest) Success() {
    req.LogResponse <- true
}

func (req *LogRequest) Wait() bool {
    succ := <-req.LogResponse
    close(req.LogResponse)

    return succ
}

func (l *Logger) Log(t *pb.Transaction) *LogRequest {
    req := NewLogRequest(t)
    l.Channel <- req
    return req
}

func (l *Logger) GetJsonIndex() int {
    return l.jsonIndex
}

func (l *Logger) GetBufferLength() int {
    return l.BufferLength[l.CurrentBuffer]
}

func (l *Logger) Save(current int) {
    l.jsonIndex += 1

    block := new(pb.Block)
    block.BlockID = int32(l.jsonIndex)
    block.PrevHash = "00000000"
    block.Nonce = "00000000"
    block.Transactions = make([]*pb.Transaction, l.config.BlockSize)
    for i := 0; i < l.config.BlockSize; i++ {
        block.Transactions[i] = l.Buffer[current][i].Transaction
    }
    str, err := l.jsonMarshaler.MarshalToString(block)
    if err != nil {
        panic(err)
    }
    fmt.Println(str)


    if l.jsonIndex % l.config.SnapshotBlockSize == 0 {
        data, err := l.server.Database.DumpSnapshot()
        if err == nil {
            fmt.Println("snapshot", data)
            l.lastSnapshot = l.jsonIndex
        }
    }

    l.ResetInternal()
    l.bufferSaved <- true
}

func (l *Logger) ResetInternal() {
    state := &pb.ServerState{JsonIndex: int32(l.jsonIndex), LastSnapshot: int32(l.lastSnapshot)}
    stateBytes, err := proto.Marshal(state)
    if err != nil {
        panic(err)
    }
    fmt.Println("ResetInternal", stateBytes)
}

func (l *Logger) AppendInternal(reqs []*LogRequest) {
    for _, req := range reqs {
        bytes, err := proto.Marshal(req.Transaction)
        if err != nil {
            panic(err)
        }
        fmt.Println("AppendInternal", bytes)
    }
}

func (l *Logger) Recover() {
    // Load l.jsonIndex, l.lastSnapshot, transactions
    transactions := make([]*pb.Transaction, 0)

    if l.lastSnapshot != 0 {
        // Load snapshot
    }

    for i := l.lastSnapshot + 1; i <= l.jsonIndex; i++ {
        block := new(pb.Block)
        for j := 0; j < l.config.BlockSize; j++ {
            l.server.RecoverAtomic(block.Transactions[j])
        }
    }

    for _, t := range transactions {
        l.server.RecoverAtomic(t)
    }
}

func (l *Logger) Mainloop() {
    for {
        start := l.BufferLength[l.CurrentBuffer]
        end := start
        for i := 0; i < l.config.LogBatchSize; i++ {
            if start != end && len(l.Channel) == 0 {
                break
            }
            this := <-l.Channel
            l.Buffer[l.CurrentBuffer][end] = this
            end += 1
            l.BufferLength[l.CurrentBuffer] = end
            if end == l.config.BlockSize {
                break
            }
        }

        l.AppendInternal(l.Buffer[l.CurrentBuffer][start:end])
        for i := start; i < end; i++ {
            l.Buffer[l.CurrentBuffer][i].Success()
        }

        if end == l.config.BlockSize {
            _ = <-l.bufferSaved
            go l.Save(l.CurrentBuffer)
            l.CurrentBuffer = 1 - l.CurrentBuffer
            l.BufferLength[l.CurrentBuffer] = 0
        }
    }
}

