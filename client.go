package main

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Client interface {
	ClientId() int
	BrokerUrl() string
	BrokerUser() string
	BrokerPass() string
	Run(res chan *RunResults)
}

func connect(c Client, onConnect func(client mqtt.Client)) {
	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerUrl()).
		SetClientID(fmt.Sprintf("mqtt-benchmark-%v-%v", time.Now().Format(time.RFC3339Nano), c.ClientId())).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(onConnect).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", c.ClientId(), reason.Error())
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
	}
}
