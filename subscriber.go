package main

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Subscriber struct {
	id          int
	brokerURL   string
	brokerUser  string
	brokerPass  string
	MsgTopic    string
	MsgSize     int
	MsgCount    int
	MsgQoS      byte
	Quiet       bool
	Panic       bool
	IdleTimeout time.Duration
	idleTimer   *time.Timer
	connected   time.Time
}

func (c Subscriber) ClientId() string {
	return fmt.Sprintf("sub-%d", c.id)
}

func (c Subscriber) BrokerUrl() string {
	return c.brokerURL
}

func (c Subscriber) BrokerUser() string {
	return c.brokerUser
}

func (c Subscriber) BrokerPass() string {
	return c.brokerPass
}

func (c Subscriber) PanicMode() bool {
	return c.Panic
}

func (c Subscriber) Run(res chan *RunResults) {
	doneSub := make(chan bool)
	rcvMsgs := make(chan *Message)
	runResults := &RunResults{
		ID: c.ClientId(),
	}

	c.idleTimer = time.NewTimer(c.IdleTimeout)
	c.subscribe(rcvMsgs, doneSub)

	for {
		select {
		case m := <-rcvMsgs:
			if m.Error {
				log.Printf("CLIENT %v ERROR receiving message: %v: at %v\n", c.ClientId(), m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				runResults.Successes++
				c.idleTimer.Reset(c.IdleTimeout)
			}
		case <-doneSub:
			runResults = c.prepareResult(runResults)
			res <- runResults
			return
		case <-c.idleTimer.C:
			log.Printf("CLIENT %v stopping after idle time: %v\n", c.ClientId(), c.IdleTimeout)
			runResults = c.prepareResult(runResults)
			res <- runResults
			return
		}
	}
}

func (c *Subscriber) subscribe(rcvMsg chan *Message, doneSub chan bool) {
	onConnected := func(client mqtt.Client) {
		c.connected = time.Now()
		if !c.Quiet {
			log.Printf("CLIENT %v is connected to the broker %v and topic %v\n", c.ClientId(), c.BrokerUrl(), c.MsgTopic)
		}

		ctr := 0
		onMessage := func(inner mqtt.Client, m mqtt.Message) {
			ctr++
			rcvMsg <- &Message{
				Topic: m.Topic(),
				QoS:   m.Qos(),
			}

			if c.MsgCount > 0 && ctr >= c.MsgCount {
				client.Unsubscribe(c.MsgTopic)
				client.Disconnect(1000)
				if !c.Quiet {
					log.Printf("CLIENT %v is done receiving messages\n", c.ClientId())
				}
				doneSub <- true
			}
		}

		token := client.Subscribe(c.MsgTopic, c.MsgQoS, onMessage)
		token.Wait()
		if token.Error() != nil {
			log.Printf("CLIENT %v Error subscribing to the topic %v: %v\n", c.ClientId(), c.MsgTopic, token.Error())
			if c.Panic {
				panic(token.Error())
			}
		}
	}

	connect(c, onConnected)
}

func (c Subscriber) prepareResult(runResults *RunResults) *RunResults {
	duration := time.Since(c.connected)
	runResults.ClientRunTime = duration.Seconds()
	runResults.MsgsPerSec = float64(runResults.Successes) / duration.Seconds()
	return runResults
}
