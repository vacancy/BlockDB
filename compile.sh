#!/bin/bash

# Go program does not really need to be compiled; use "go run" will be fine.
cd server
go build database.go main.go logging.go

