package main

import (
	"log"

	"github.com/schmichael/k2/k2d"
)

type logwriter struct{}

func (w logwriter) Write(topic string, partition uint32, msg []byte) error {
	log.Printf("topic=%s part=%d msg=%q", topic, partition, msg)
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if err := k2d.ListenAndServe("tcp", "localhost:9092", logwriter{}); err != nil {
		log.Fatal(err)
	}
}
