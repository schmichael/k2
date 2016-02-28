package k2store

import (
	"fmt"
	"log"
	"os"
)

type MessageWriter interface {
	Write(topic string, partition uint32, message []byte) error
}

type msgt struct {
	topic string
	part  uint32
	body  []byte
}

type Store struct {
	incoming chan msgt
}

func New() *Store {
	s := Store{
		incoming: make(chan msgt),
	}
	go s.handleWrites()
	return &s
}

func (s *Store) Write(topic string, part uint32, msg []byte) error {
	s.incoming <- msgt{topic, part, msg}
	return nil
}

func (s *Store) handleWrites() {
	files := map[string]*os.File{}
	for msg := range s.incoming {
		fn := fmt.Sprintf("%s-%d.k2", msg.topic, msg.part)
		fd, ok := files[fn]
		if !ok {
			var err error
			fd, err = os.Create(fn)
			if err != nil {
				//FIXME need ability to shutdown store with an error
				panic(err)
			}
			defer fd.Close()
			files[fn] = fd
			log.Println("Created ", fn)
		}

		if _, err := fd.Write(msg.body); err != nil {
			//FIXME need ability to shutdown store with an error
			panic(err)
		}
	}
}
