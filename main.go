package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"reflect"
	"time"
)

type poolEntry struct {
	name string
	pool []amqp.Delivery
}

type cfgEntry struct {
	url string
	queue string
}

func main() {
	var con *amqp.Connection
	var ch *amqp.Channel
	var del <-chan amqp.Delivery
	var cases []reflect.SelectCase

	cfg := getCfg()
	delChans := make([]<-chan amqp.Delivery, 0)

	for _, cfgE := range cfg {
		con, _ = amqp.Dial(cfgE.url)
		defer con.Close()

		ch, _ = con.Channel()
		defer ch.Close()

		_ = ch.Qos(1, 0,false)
		del, _ = ch.Consume(cfgE.queue, "go-consumer",false, false, false, false, nil)

		delChans = append(delChans, del)
	}

	infinite_block := make(chan bool)
	go func() {
		messagePool := make([]poolEntry, 0)

		for _, cfgE := range cfg {
			messagePool = append(messagePool, poolEntry{
				cfgE.queue,
				make([]amqp.Delivery, 0),
			})
		}

		cases = make([]reflect.SelectCase, len(delChans) + 1)
		for index, dCh := range delChans {
			cases[index] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(dCh),
			}
		}
		cases[len(cases)-1] = reflect.SelectCase{Dir: reflect.SelectDefault}

		for {
			index, command, _ := reflect.Select(cases)
			if index == (len(cases)-1) {
				execute(messagePool)
			} else {
				fmt.Println(messagePool[index].name)
				messagePool[index].pool = append(messagePool[index].pool, command.Interface().(amqp.Delivery))
			}
		}
	}()
	<-infinite_block
}

func execute (messagePool []poolEntry) {
	var cmd amqp.Delivery
	for queue, _ := range messagePool {
		if len(messagePool[queue].pool) > 0 {
			cmd = messagePool[queue].pool[0]
			messagePool[queue].pool = messagePool[queue].pool[1:]
			handle(&cmd)
			time.Sleep(10*time.Millisecond)
			break
		}
	}
}

func handle(command *amqp.Delivery) {
	fmt.Printf("message body: %s", command.Body)
	fmt.Println()
	time.Sleep(10*time.Millisecond)
	command.Ack(false)
}

func getCfg() ([]cfgEntry) {
	cfg := make ([]cfgEntry, 0)

	cfg = append(cfg, cfgEntry{
		"amqp://miinto:miinto@localhost:5672/",
		"go-generic-0-0",
	});

	cfg = append(cfg, cfgEntry{
		"amqp://miinto:miinto@localhost:5672/",
		"go-generic-0-1",
	});

	cfg = append(cfg, cfgEntry{
		"amqp://miinto:miinto@localhost:5672/",
		"go-generic-1-0",
	});

	cfg = append(cfg, cfgEntry{
		"amqp://miinto:miinto@localhost:5672/",
		"go-generic-2-0",
	});

	return cfg
}