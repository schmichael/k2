package main

import (
	"flag"
	"log"

	"github.com/schmichael/k2/k2d"
	"github.com/schmichael/k2/k2store"
)

type logwriter struct{}

func (w logwriter) Write(topic string, partition uint32, msg []byte) error {
	log.Printf("topic=%s part=%d msg=%q", topic, partition, msg)
	return nil
}

func main() {
	write := false
	flag.BoolVar(&write, "w", write, "actually write to disk")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var mw k2store.MessageWriter
	if write {
		mw = k2store.New()
	} else {
		mw = logwriter{}
	}
	if err := k2d.ListenAndServe("tcp", "localhost:9092", mw); err != nil {
		log.Fatal(err)
	}
}
