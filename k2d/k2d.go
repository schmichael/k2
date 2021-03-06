package k2d

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"

	"github.com/schmichael/k2/k2store"
)

const (
	Magic0 = '\x00'
	Magic1 = '\x01'

	RequestTypeProduce      = 0
	RequestTypeFetch        = 1
	RequestTypeMultiFetch   = 2
	RequestTypeMultiProduce = 3
	RequestTypeOffsets      = 4
)

type server struct {
	w k2store.MessageWriter
}

func (s *server) handle(c net.Conn) {
	defer c.Close()
	cname := c.RemoteAddr()
	log.Printf("Accepted connection from %s", cname)

	for {
		// Read the envelope size
		szbuf := make([]byte, 4)
		if _, err := c.Read(szbuf); err != nil {
			if err == io.EOF {
				log.Printf("client=%s closed connection", cname)
				return
			}
			log.Printf("client=%s Error reading envelope size: %v", cname, err)
			return
		}
		sz := binary.BigEndian.Uint32(szbuf)

		buf := make([]byte, sz)
		if _, err := c.Read(buf); err != nil {
			log.Printf("client=%s Error reading message: %v", cname, err)
			return
		}

		topiclen := binary.BigEndian.Uint16(buf[2:4])
		topic := string(buf[4 : 4+topiclen])
		part := binary.BigEndian.Uint32(buf[4+topiclen : 4+topiclen+4])

		switch reqtype := binary.BigEndian.Uint16(buf[0:2]); reqtype {
		case RequestTypeProduce:
			if err := s.handleProduce(c, topic, part, buf[4+4+topiclen:sz]); err != nil {
				log.Printf("client=%s Error handling produce request: %v", cname, err)
				return
			}
		case RequestTypeFetch:
			log.Printf("client=%s sent unsupported fetch request", cname)
			return
		case RequestTypeMultiFetch:
			log.Printf("client=%s sent unsupported multifetch request", cname)
			return
		case RequestTypeMultiProduce:
			log.Printf("client=%s sent unsupported multiproduce request", cname)
			return
		case RequestTypeOffsets:
			log.Printf("client=%s sent unsupported offset request", cname)
			return
		default:
			log.Printf("invalid request type: %d", cname, reqtype)
			return
		}
	}
}

func (s *server) handleProduce(c net.Conn, topic string, part uint32, reqbuf []byte) error {
	// check invariants; could be skipped
	msgssz := binary.BigEndian.Uint32(reqbuf[:4])
	reqbuf = reqbuf[4:]
	if int(msgssz) != len(reqbuf) {
		return fmt.Errorf("client specifies %d bytes of messages but received %d", msgssz, len(reqbuf))
	}

	// handle messages
	for len(reqbuf) > 4 {
		var attrs byte

		msgsz := binary.BigEndian.Uint32(reqbuf[:4])
		if int(msgsz) > len(reqbuf)-4 {
			return fmt.Errorf("message size %d with only %d bytes remaining", msgsz, len(reqbuf))
		}
		fullmsg := reqbuf[:msgsz+4]
		msgbuf := reqbuf[4 : msgsz+4]
		reqbuf = reqbuf[msgsz+4:]

		// check magic
		switch msgbuf[0] {
		case Magic0:
			msgbuf = msgbuf[1:]
		case Magic1:
			if attrs = msgbuf[1]; attrs != '\x00' {
				log.Printf("client=%s Unsupported attributes: %x", c.RemoteAddr(), msgbuf[1])
			}
			msgbuf = msgbuf[2:]
		default:
			return fmt.Errorf("incorrect/unsupported magic: %x", msgbuf[0])
		}

		theirsum := binary.BigEndian.Uint32(msgbuf[:4])
		msgbuf = msgbuf[4:]
		if oursum := crc32.ChecksumIEEE(msgbuf); theirsum != oursum {
			return fmt.Errorf("checksum mismatch: client=%d != %d", int32(theirsum), int32(oursum))
		}

		if err := s.w.Write(topic, part, fullmsg); err != nil {
			return err
		}
	}

	if len(reqbuf) != 0 {
		return fmt.Errorf("unexpected trailing bytes: hex=%x text=%q", reqbuf, reqbuf)
	}
	return nil
}

func ListenAndServe(nettype, laddr string, w k2store.MessageWriter) error {
	l, err := net.Listen(nettype, laddr)
	if err != nil {
		return err
	}
	defer l.Close()

	s := &server{w: w}

	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go s.handle(c)
	}
}
