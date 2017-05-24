#!/bin/sh
protoc -I ./ ./db.proto ./dbserver.proto --go_out=plugins=grpc:./go 
