package main

import (
	"fmt"
	"listener-service/events"
	"log"
	"math"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	//try to connect to rabbit mq
	rabbitConn, err := connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	defer rabbitConn.Close()
	log.Println("connected to RabbitMQ")

	//start listening for messages
	log.Println("Listening for and consuming rabbitMQ messages...")

	//create consumer
	consumer, err := events.NewConsumer(rabbitConn)
	if err != nil {
		panic(err)
	}

	//watch queue and consume events
	err = consumer.Listen([]string{"log.INFO","log.WARNING","log.ERROR"})
	if err != nil {
		log.Println(err)
	}
}

func connect() (*amqp.Connection, error) {
	var counts int64
	var backoff = 1 * time.Second
	var connection *amqp.Connection

	// dont continue until rabbit is ready
	for {
		c, err := amqp.Dial("amqp://guest:guest@rabbitmq")
		if err != nil {
			fmt.Println("RabbitMQ not yet ready...")
			counts++
		} else {
			connection = c
			break
		}

		if counts > 5 {
			fmt.Println(err)
			return nil,err
		}

		backoff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("backing off..")
		time.Sleep(backoff)
		continue
	}

	return connection, nil
}