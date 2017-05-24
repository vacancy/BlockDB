package main;

import (
    pb "../protobuf/go"
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

    bufferSaved chan bool
    config *ServerConfig
}

func NewLogger(conf *ServerConfig) (*Logger) {
    logger := new(Logger)
    for i := 0; i < 2; i++ {
        logger.Buffer[i] = make([]*LogRequest, conf.BlockSize)
        logger.BufferLength[i] = 0
    }

    logger.CurrentBuffer = 0
    logger.Channel = make(chan *LogRequest)
    logger.bufferSaved = make(chan bool, 1)
    logger.bufferSaved <- true
    logger.config = conf

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

func (l *Logger) GetBufferLength() int {
    return l.BufferLength[l.CurrentBuffer]
}

func (l *Logger) Save(current int) {
    l.bufferSaved <- true
    // TODO:: save persistent log of l.Buffer[current]
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
            if end == l.config.BlockSize {
                break
            }
        }
        l.BufferLength[l.CurrentBuffer] = end

        // TODO:: save batched log from start to end
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
