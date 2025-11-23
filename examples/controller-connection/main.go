package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func main() {
	// to connect to the kafka leader via an existing non-leader connection rather than using DialLeader
	// Step 1: 先Dial任意一个broker
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	// Step 2：查询 Kafka Controller
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Controller found: host=%s port=%d\n", controller.Host, controller.Port)

	// Step 3：主动连接 Kafka Controller
	var connLeader *kafka.Conn
	connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer connLeader.Close()

	fmt.Println("Connected to Kafka Controller")

	// ------------------------------------------------------------------------------------------------------------

	// Step 4：使用 Controller 执行 Topic 管理操作（如检查现有 Topics）
	partitions, err := connLeader.ReadPartitions()
	if err != nil {
		log.Fatal("failed to read partitions:", err)
	}

	fmt.Println("Existing Topics & Partitions:")
	for _, p := range partitions {
		fmt.Printf("- Topic: %s  Partition: %d  Leader: %s:%d\n",
			p.Topic, p.ID, p.Leader.Host, p.Leader.Port)
	}

	// Step 5：确定一个 topic 的 leader
	topic := "my-topic"
	partition := 0

	leaderConn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer leaderConn.Close()

	fmt.Printf("Connected to Leader for topic=%s partition=%d\n", topic, partition)
}
