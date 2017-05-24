package main

import (
    "io"
    "io/ioutil"
    "encoding/binary"
)

func CRCSave(filename string, msg string){
    err := ioutil.WriteFile(filename, []byte(msg), 0644)
    if err != nil{
        panic(e)
    }
    err = ioutil.WriteFile(filename + ".crc", crc32.ChecksumIEEE([]byte(msg)))
    if err != nil{
        panic(e)
    }
}

func CRCSaveStream(w io.Writer, msg []byte) {
    bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, len(msg))
    _, err := w.Write(bs)
    if err != nil{
        panic(e)
    }
    _, err = w.write(msg)
    if err != nil{
        panic(e)
    }
    binary.LittleEndian.PutUint32(bs, crc32.ChecksumIEEE(msg))
    _, err = w.Write(bs)
    if err != nil{
        panic(e)
    }
}

func CRCLoad(filename string) msg string, err error {
    buf := ioutil.ReadFile(filename)
    crc := ioutil.ReadFile(filename + ".crc")
    err := nil
    if crc != crc32.ChecksumIEEE(buf) {
        err = errors.New("checksum fail")
    }
    msg = string(buf)
    return
}

func CRCLoadStream(r io.Reader) msg []byte, err error {
    bs := make([]byte, 4)
    _, err := r.Read(buff)
    if err != nil{
        panic(e)
    }
    l := binary.LittleEndian.Uint32(bs)

    msg := make([]byte, l)
    _, err = r.Read(msg)    
    if err != nil{
        panic(e)
    }

    _, err = r.Read(bs)
    if err != nil{
        panic(e)
    }
    crc := binary.LittleEndian.Uint32(bs)
    err = nil
    if crc != crc32.ChecksumIEEE(msg) {
        err = errors.New("checksum fail")
    }
}