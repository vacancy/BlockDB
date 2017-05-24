package main

import (
    "io"
    "io/ioutil"
    "encoding/binary"
)

func CRCSave(filename string, msg string) error {
    byteMsg := []byte(msg)
    err := ioutil.WriteFile(filename, byteMsg, 0644)
    if err != nil {
        return err
    }
    err = ioutil.WriteFile(filename + ".crc", crc32.ChecksumIEEE(byteMsg))
    if err != nil {
        return err
    }
    return nil
}

func CRCSaveStream(w io.Writer, msg []byte) error {
    bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, uint32(len(msg)))
    _, err := w.Write(bs)
    if err != nil{
        return err
    }
    _, err = w.write(msg)
    if err != nil{
        return err
    }
    binary.LittleEndian.PutUint32(bs, crc32.ChecksumIEEE(msg))
    _, err = w.Write(bs)
    if err != nil{
        return err
    }
}

func CRCLoad(filename string) (msg string, err error) {
    buf, err := ioutil.ReadFile(filename)
    if err != nil {
        return
    }
    crc, err := ioutil.ReadFile(filename + ".crc")
    if err != nil {
        return
    }
    if crc != crc32.ChecksumIEEE(buf) {
        err = errors.New("checksum fail")
    }
    msg = string(buf)
    return
}

func CRCLoadStream(r io.Reader) (msg []byte, err error) {
    bs := make([]byte, 4)
    _, err = r.Read(buff)
    if err != nil {
        return
    }
    l := binary.LittleEndian.Uint32(bs)

    msg := make([]byte, l)
    _, err = r.Read(msg)
    if err != nil {
        return
    }

    _, err = r.Read(bs)
    if err != nil {
        return
    }

    crc := binary.LittleEndian.Uint32(bs)
    err = nil
    if crc != crc32.ChecksumIEEE(msg) {
        err = errors.New("checksum fail")
    }
}

