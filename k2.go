package main

import (
	"encoding/binary"
	"hash/crc32"
	"log"
	"net"
)

const (
	Magic0 = '\x00'
	Magic1 = '\x01'
)

const (
	StatusSuccess          Status = 0
	StatusInvalidOffset    Status = 1
	StatusInvalidMessage   Status = 2
	StatusInvalidPartition Status = 3
	StatusInvalidFetchSize Status = 4
)

type Status uint16

type RequestHeader struct {
	// omitted: uint32 total size

	Type uint16 // 0==produce, 1==fetch, 2==multifetch, 3==multiproduce, 4==offsets

	// omitted: uint16 topic len; the number of chars in the topic name (the next field)

	Topic     string // topic name, without null termination
	Partition uint32 // partition number to use
}

type ResponseHeader struct {
	// omitted: uint32 total size

	Status Status
}

func handle(c net.Conn) {
	defer c.Close()
	log.Printf("Accepted connection from %s", c.RemoteAddr())

	//TODO implement request envelope

	// Read the message size
	szbuf := make([]byte, 4)
	if _, err := c.Read(szbuf); err != nil {
		log.Printf("client=%s Error reading message size: %v", c.RemoteAddr(), err)
		return
	}
	sz := binary.BigEndian.Uint32(szbuf)

	buf := make([]byte, sz)
	if _, err := c.Read(buf); err != nil {
		log.Printf("client=%s Error reading message: %v", c.RemoteAddr(), err)
		return
	}

	n := 1
	var attrs byte

	// check magic
	switch buf[0] {
	case Magic0:
		// nothing extra to do here
	case Magic1:
		if attrs = buf[1]; attrs != '\x00' {
			log.Printf("client=%s Unsupported attributes: %x", c.RemoteAddr(), buf[1])
		}
		n++
	default:
		log.Printf("client=%s Incorrect magic: %x", c.RemoteAddr(), buf[0])
		return
	}

	log.Printf("len=%d cap=%d %q", len(buf), cap(buf), buf)
	theirsum := binary.BigEndian.Uint32(buf[n : n+4])
	payload := buf[n+4 : sz]
	log.Printf("len=%d cap=%d %q", len(payload), cap(payload), payload)
	if oursum := crc32.ChecksumIEEE(payload); theirsum != oursum {
		log.Printf("client=%s Checksum mismatch: client=%d != %d", c.RemoteAddr(), theirsum, oursum)
		return
	}

	log.Printf("client=%s Message=\n%q\n", c.RemoteAddr(), payload)
}

func main() {
	l, err := net.Listen("tcp", "localhost:9092")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handle(c)
	}
}
