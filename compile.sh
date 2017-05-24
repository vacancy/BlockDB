#!/bin/bash

# Go program does not really need to be compiled; use "go run" will be fine.
cd server
go build main.go database.go logging.go io.go

