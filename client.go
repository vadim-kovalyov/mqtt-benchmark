package main

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Message describes a message
type Message struct {
	Topic     string
	QoS       byte
	Payload   interface{}
	Sent      time.Time
	Delivered time.Time
	Error     bool
}

// Client represents a connection information for a publisher or a subscriber
type Client interface {
	ClientId() string
	BrokerUrl() string
	BrokerUser() string
	BrokerPass() string
	Run(res chan *RunResults)
	PanicMode() bool
}

func connect(c Client, onConnect func(client mqtt.Client)) {
	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerUrl()).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now().Format(time.RFC3339Nano), c.ClientId())).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetOnConnectHandler(onConnect).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("CLIENT %v lost connection to the broker: %v.\n", c.ClientId(), reason.Error())
			if c.PanicMode() {
				panic(reason.Error())
			}
		})
	if c.BrokerUser() != "" && c.BrokerPass() != "" {
		opts.SetUsername(c.BrokerUser())
		opts.SetPassword(c.BrokerPass())
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		log.Printf("CLIENT %v had error connecting to the broker: %v\n", c.ClientId(), token.Error())
		if c.PanicMode() {
			panic(token.Error())
		}
	}
}
