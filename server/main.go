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
    SnapshotBlockSize int
}

type Server struct {
    Config *ServerConfig
    Logger *Logger
    Database *Database
}

func (s *Server) RecoverAtomic(trans *pb.Transaction) error {
    var err error = nil
    switch trans.Type {
    case 2:
        err = s.Database.SetAtomicKey(trans.UserID, trans.Value, 1)
    case 3:
        err = s.Database.SetAtomicKey(trans.UserID, trans.Value, 2)
    case 4:
        err = s.Database.SetAtomicKey(trans.UserID, trans.Value, 3)
    case 5:
        err = s.Database.TransferAtomicKey(trans.FromID, trans.ToID, trans.Value)
    }
    return err
}

// Database Interface 
func (s *Server) Get(ctx context.Context, in *pb.GetRequest) (res *pb.GetResponse, err error) {
    val, err := s.Database.Get(in.UserID)
    if err != nil {
        return &pb.GetResponse{Value: 0}, nil
    }
	return &pb.GetResponse{Value: val}, nil
}
func (s *Server) Put(ctx context.Context, in *pb.Request) (res *pb.BooleanResponse, err error) {
    _, req, err := s.Database.Set(in.UserID, in.Value)
    if req != nil {
        req.Wait()
    }
	return &pb.BooleanResponse{Success: err == nil}, nil
}
func (s *Server) Deposit(ctx context.Context, in *pb.Request) (res *pb.BooleanResponse, err error) {
    _, req, err := s.Database.Increase(in.UserID, in.Value)
    if req != nil {
        req.Wait()
    }
	return &pb.BooleanResponse{Success: err == nil}, nil
}
func (s *Server) Withdraw(ctx context.Context, in *pb.Request) (res *pb.BooleanResponse, err error) {
    _, req, err := s.Database.Decrease(in.UserID, in.Value)
    if req != nil {
        req.Wait()
    }
	return &pb.BooleanResponse{Success: err == nil}, nil
}
func (s *Server) Transfer(ctx context.Context, in *pb.TransferRequest) (res *pb.BooleanResponse, err error) {
    _, req, err := s.Database.Transfer(in.FromID, in.ToID, in.Value)
    if req != nil {
        req.Wait()
    }
	return &pb.BooleanResponse{Success: err == nil}, nil
}

// Interface with test grader
func (s *Server) LogLength(ctx context.Context, in *pb.Null) (res *pb.GetResponse, err error) {
    return &pb.GetResponse{Value: int32(s.Logger.GetBufferLength())}, nil
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
    conf.SnapshotBlockSize = 10
    return
}

func mainloop(conf *ServerConfig) (err error) {
	// Bind to port
	socket, err := net.Listen("tcp", conf.Addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
        return
	}
	log.Printf("Listening: %s ...", conf.Addr)

    server := new(Server)
    server.Config = conf
    server.Logger = NewLogger(server)
    server.Database = NewDatabse(conf, server.Logger)
    server.Logger.Recover()

    go server.Logger.Mainloop()

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
    conf, err := initializeConfig("config.json")
    if err != nil {
        panic(err)
    }
    err = mainloop(conf)
    if err != nil {
        panic(err)
    }
}

