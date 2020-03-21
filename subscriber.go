package main

import (
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Subscriber struct {
	id         int
	brokerURL  string
	brokerUser string
	brokerPass string
	MsgTopic   string
	MsgSize    int
	MsgCount   int
	MsgQoS     byte
	Quiet      bool
}

func (c Subscriber) ClientId() int {
	return c.id
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

func (c Subscriber) Run(res chan *RunResults) {
	doneSub := make(chan bool)
	rcvMsgs := make(chan *Message)
	runResults := &RunResults{
		Id: c.ClientId(),
	}

	started := time.Now()

	c.subscribe(rcvMsgs, doneSub)

	for {
		select {
		case m := <-rcvMsgs:
			if m.Error {
				log.Printf("CLIENT %v ERROR receiving message: %v: at %v\n", c.ClientId(), m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				runResults.Successes++
			}
		case <-doneSub:
			// calculate results
			duration := time.Now().Sub(started)
			// runResults.MsgTimeMin = stats.StatsMin(times)
			// runResults.MsgTimeMax = stats.StatsMax(times)
			// runResults.MsgTimeMean = stats.StatsMean(times)
			runResults.RunTime = duration.Seconds()
			runResults.MsgsPerSec = float64(runResults.Successes) / duration.Seconds()
			// calculate std if sample is > 1, otherwise leave as 0 (convention)
			if c.MsgCount > 1 {
				//runResults.MsgTimeStd = stats.StatsSampleStandardDeviation(times)
			}

			// report results and exit
			res <- runResults
			return
		}
	}
}

func (c Subscriber) subscribe(rcvMsg chan *Message, doneSub chan bool) {
	onConnected := func(client mqtt.Client) {
		if !c.Quiet {
			log.Printf("CLIENT %v is connected to the broker %v and topic %v\n", c.ClientId(), c.BrokerUrl(), c.MsgTopic)
		}

		ctr := 0
		onMessage := func(inner mqtt.Client, m mqtt.Message) {
			if !c.Quiet {
				//log.Printf("Message received")
			}
			ctr++
			rcvMsg <- &Message{
				Topic: m.Topic(),
				QoS:   m.Qos(),
			}

			if ctr >= c.MsgCount {
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
			log.Printf("CLIENT %v Error subscribing: %v\n", c.ClientId(), token.Error())
		}
	}

	connect(c, onConnected)
}
