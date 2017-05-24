package main;

import (
    "encoding/json"
    "fmt"
    pb "../protobuf/go"
)

type LogResponse bool;
type FutureLogResponse chan LogResponse
type LogRequest struct {
    Transaction *pb.Transaction
    LogResponse FutureLogResponse
}

type Logger struct {
    Buffer [2][]*LogRequest
    BufferLength [2]int
    CurrentBuffer int
    Channel chan LogRequest

    bufferSaved chan bool
    config *ServerConfig
}

func NewLogger(conf *ServerConfig) (*Logger) {
    logger := new(Logger)
    for i := 0; i < 2; i++ {
        logger.Buffer[i] = make([]*pb.Transaction, conf.BlockSize)
        logger.BufferLength[i] = 0
    }

    logger.CurrentBuffer = 0
    logger.Channel = make(chan LogRequest)
    logger.bufferSaved = make(chan bool, 1)
    logger.bufferSaved <- true
    logger.config = conf
}

func NewLogRequest(t *pb.Transaction) (*LogRequest) {
    req := new(LogRequest)
    req.Transaction = t
    req.LogResponse = make(FutureLogResponse, 1)
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
    l.Channel <- reqA
    return req
}

func (l *Logger) save(current int) {
    l.bufferSaved <- true
    // TODO:: save persistent log of l.Buffer[current]
}

func (l *Logger) mainloop() {
    logFutures := make([]FutureLogResponse, s.config.blockSize)
    for {
        start := l.BufferLength[l.CurrentBuffer]
        for i := 0; i < l.config.LogBatchSize; i++ {
            if start != end && len(l.Channel) == 0 {
                break
            }
            this := <-l.Channel
            l.Buffer[l.CurrentBuffer][l.BufferLength[l.CurrentBuffer]] = this
            l.BufferLength[l.CurrentBuffer] += 1
            if l.BufferLength[l.CurrentBuffer] == l.config.BlockSize {
                break
            }
        }
        end := l.BufferLength[l.CurrentBuffer]

        // TODO:: save batched log from start to end
        for i := start; i < end; i++ {
            l.Buffer[l.CurrentBuffer][i].Success()
        }

        if end == s.config.blockSize {
            _ = <-l.bufferSaved
            go l.save(l.CurrentBuffer)
            l.CurrentBuffer = 1 - l.CurrentBuffer
            l.BufferLength[l.CurrentBuffer] = 0
        }
    }
}
