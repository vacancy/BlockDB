package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
    "errors"
	pb "../protobuf/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ServerConfig struct {
    Addr string
    DataDir string
    BlockSize int
    LogBatchSize int
}

type ServerContext struct {
    data *Database
}

type Server struct {
    config *ServerConfig
    context *ServerContext
    logChan LogRequestChan
}

func (s *Server) LogConcurrent() {
}

// Database Interface 
func (s *Server) Get(ctx context.Context, in *pb.GetRequest) (res *pb.GetResponse, err error) {
	return &pb.GetResponse{Value: 0}, nil
}
func (s *Server) Put(ctx context.Context, in *pb.Request) (res *pb.BooleanResponse, err error) {
    err = s.Log(pb.Transaction{Type: 2, UserID: in.UserID})
    if err != nil {
        return
    }
	s.context.data[in.UserID] = in.Value
	return &pb.BooleanResponse{Success: true}, nil
}
func (s *Server) Deposit(ctx context.Context, in *pb.Request) (res *pb.BooleanResponse, err error) {
    err = s.Log(pb.Transaction{Type: 3, UserID: in.UserID})
    if err != nil {
        return
    }
	s.context.data[in.UserID] += in.Value
	return &pb.BooleanResponse{Success: true}, nil
}
func (s *Server) Withdraw(ctx context.Context, in *pb.Request) (res *pb.BooleanResponse, err error) {
    err = s.Log(pb.Transaction{Type: 4, UserID: in.UserID})
    if err != nil {
        return
    }
	s.context.data[in.UserID] -= in.Value
	return &pb.BooleanResponse{Success: true}, nil
}
func (s *Server) Transfer(ctx context.Context, in *pb.TransferRequest) (res *pb.BooleanResponse, err error) {
    err = s.Log(pb.Transaction{Type: 5, FromID: in.FromID, ToID: in.ToID})
    if err != nil {
        return
    }
	s.context.data[in.FromID] -= in.Value
	s.context.data[in.ToID] += in.Value
	return &pb.BooleanResponse{Success: true}, nil
}
// Interface with test grader
func (s *Server) LogLength(ctx context.Context, in *pb.Null) (res *pb.GetResponse, err error) {
	return &pb.GetResponse{Value: int32(len(s.context.logs))}, nil
}

func initializeConfig(configFile string) (conf *ServerConfig, err error) {
    conf = new(ServerConfig)
    err = nil

    jsonf, err := ioutil.ReadFile(configFile)
	if err != nil {
        return
	}
	var jsond map[string]interface{}
	err = json.Unmarshal(jsonf, &jsond)
	if err != nil {
        return
	}
    if jsond["nservers"].(float64) != 1 {
        err = errors.New("nservers != 1")
        return
    }
	jsond = jsond["1"].(map[string]interface{}) // should be dat[myNum] in the future

    conf.Addr = fmt.Sprintf("%s:%s", jsond["ip"], jsond["port"])
    conf.DataDir = fmt.Sprintf("%s",jsond["dataDir"])
    conf.BlockSize = 50
    conf.LogBatchSize = 16
    return
}

func initializeContext(conf *ServerConfig) (ctx *ServerContext, err error) {
    ctx = new(ServerContext)
    ctx.data = database.New()
    ctx.logs = make([]pb.Transaction, 0, conf.blockSize)
    return
}

func mainloop(sconf *ServerConfig, sctx *ServerContext) (err error) {
	// Bind to port
	socket, err := net.Listen("tcp", sconf.Addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
        return
	}
	log.Printf("Listening: %s ...", sconf.Addr)

    server := new(Server)
    server.config = sconf
    server.context = sctx

	// Create gRPC server
	rpcServer := grpc.NewServer()
	pb.RegisterBlockDatabaseServer(rpcServer, server)
	// Register reflection service on gRPC server.
	reflection.Register(rpcServer)

	// Start server
	if err := rpcServer.Serve(socket); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
    return
}

// Main function, RPC server initialization
func main() {
    sconf, err := initializeConfig("config.json")
    if err != nil {
        panic(err)
    }
    sctx, err := initializeContext(sconf)
    if err != nil {
        panic(err)
    }

    err = mainloop(sconf, sctx)
    if err != nil {
        panic(err)
    }
}

