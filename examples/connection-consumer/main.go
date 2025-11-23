package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "my-topic"
	partition := 0

	// 底层API：直连partition leader
	conn, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		"localhost:9092",
		topic,
		partition,
	)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // 10K min, 1MB max

	b := make([]byte, 10e3) // 10KB max pre message
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}
	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
