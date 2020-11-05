package main

import (
	"github.com/streadway/amqp"
	"strconv"
)

func main () {
	var index int
	size := 10000

	conn, _ := amqp.Dial("amqp://miinto:miinto@localhost:5672/")
	defer conn.Close()

	ch, _ := conn.Channel()
	defer ch.Close()

	index = 0
	for index < size {
			index++
			ch.Publish("", "go-generic-0-0", false, false, amqp.Publishing{Body: []byte("0-0-"+strconv.Itoa(index))})
	}

	index = 0
	for index < size {
		index++
		ch.Publish("", "go-generic-0-1", false, false, amqp.Publishing{Body: []byte("0-1-"+strconv.Itoa(index))})
	}

	index = 0
	for index < size {
		index++
		ch.Publish("", "go-generic-1-0", false, false, amqp.Publishing{Body: []byte("1-0-"+strconv.Itoa(index))})
	}
}