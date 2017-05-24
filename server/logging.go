package main;

import (
    "os"
    "bufio"
    "strconv"
    pb "../protobuf/go"
    "github.com/golang/protobuf/jsonpb"
    "github.com/golang/protobuf/proto"
)

const STATE_FILE = "server.state"

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
    stateFile *os.File
    stateWriter *bufio.Writer
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
    l.bufferSaved = make(chan bool, 1)

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

func (l *Logger) GetFullPath(name string) string {
    return l.config.DataDir + name
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
    data, err := l.jsonMarshaler.MarshalToString(block)
    if err != nil {
        panic(err)
    }
    err = CRCSave(l.GetFullPath(strconv.Itoa(l.jsonIndex) + ".json"), data)
    if err != nil {
        panic(err)
    }

    if l.jsonIndex % l.config.SnapshotBlockSize == 0 {
        l.SaveSnapshot()
    }

    l.ResetInternal()
    l.bufferSaved <- true
}

func (l *Logger) SaveSnapshot() {
    data, err := l.server.Database.DumpSnapshot()
    if err == nil {
        l.lastSnapshot = l.jsonIndex
        snapFile, err := os.OpenFile(l.GetFullPath(strconv.Itoa(l.lastSnapshot) + ".snapshot"), os.O_WRONLY|os.O_CREATE, 0644)
        if err != nil {
            panic(err)
        }
        snapWriter := bufio.NewWriter(snapFile)
        err = CRCSaveStream(snapWriter, data)
        if err != nil {
            panic(err)
        }
        snapWriter.Flush()
        snapFile.Close()
    }
}

func (l *Logger) ResetInternal() {
    state := &pb.ServerState{JsonIndex: int32(l.jsonIndex), LastSnapshot: int32(l.lastSnapshot)}
    stateBytes, err := proto.Marshal(state)
    if err != nil {
        panic(err)
    }

    if l.stateFile != nil {
        err = l.stateFile.Close()
        if err != nil {
            panic(err)
        }
        err = os.Remove(l.GetFullPath(STATE_FILE))
        if err != nil {
            panic(err)
        }
    }
    l.stateFile, err = os.OpenFile(l.GetFullPath(STATE_FILE), os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        panic(err)
    }
    l.stateWriter = bufio.NewWriter(l.stateFile)
    err = CRCSaveStream(l.stateWriter, stateBytes)
    if err != nil {
        panic(err)
    }
    err = l.stateWriter.Flush()
    if err != nil {
        panic(err)
    }
}

func (l *Logger) AppendInternal(reqs []*LogRequest) {
    for _, req := range reqs {
        bytes, err := proto.Marshal(req.Transaction)
        err = CRCSaveStream(l.stateWriter, bytes)
        if err != nil {
            panic(err)
        }
    }
    err := l.stateWriter.Flush()
    if err != nil {
        panic(err)
    }
}

func (l *Logger) Recover() {
	if _, err := os.Stat(l.GetFullPath(STATE_FILE)); os.IsNotExist(err) {
        l.ResetInternal()
        l.bufferSaved <- true
        return
	}

    stateFile, err := os.Open(l.GetFullPath(STATE_FILE))
    if err != nil {
        panic(err)
    }
    stateReader := bufio.NewReader(stateFile)
    bytes, eof, err := CRCLoadStream(stateReader)
    if err != nil {
        panic(err)
    }

    state := &pb.ServerState{}
    proto.Unmarshal(bytes, state)
    l.jsonIndex = int(state.JsonIndex)
    l.lastSnapshot = int(state.LastSnapshot)

    transactions := make([]*pb.Transaction, 0)
    for {
        bytes, eof, err = CRCLoadStream(stateReader)
        if !eof {
            if err != nil {
                panic(err)
            }
            t := &pb.Transaction{}
            proto.Unmarshal(bytes, t)
            transactions = append(transactions, t)
            l.Buffer[l.CurrentBuffer][l.BufferLength[l.CurrentBuffer]] = &LogRequest{Transaction: t}
            l.BufferLength[l.CurrentBuffer] += 1
        } else {
            break
        }
    }
    stateFile.Close()

    l.stateFile, err = os.OpenFile(l.GetFullPath(STATE_FILE), os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        panic(err)
    }
    l.stateWriter = bufio.NewWriter(l.stateFile)
    l.bufferSaved <- true


    if l.lastSnapshot != 0 {
        snapFile, err := os.Open(l.GetFullPath(strconv.Itoa(l.lastSnapshot) + ".snapshot"))
        if err != nil {
            panic(err)
        }
        snapReader := bufio.NewReader(snapFile)
        bytes, eof, err = CRCLoadStream(snapReader)
        if err != nil {
            panic(err)
        }
        snapFile.Close()
        l.server.Database.LoadSnapshot(bytes)
    }

    for i := l.lastSnapshot + 1; i <= l.jsonIndex; i++ {
        block := new(pb.Block)
        json, err := CRCLoad(l.GetFullPath(strconv.Itoa(i) + ".json"))
        if err != nil {
            panic(err)
        }
        err = jsonpb.UnmarshalString(json, block)
        if err != nil {
            panic(err)
        }
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

