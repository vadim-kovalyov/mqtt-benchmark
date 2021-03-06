package main

import (
	"fmt"
	"log"
	"time"

	"github.com/GaryBoone/GoStats/stats"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Publisher struct {
	id           int
	brokerURL    string
	brokerUser   string
	brokerPass   string
	MsgTopic     string
	MsgSize      int
	MsgCount     int
	MsgQoS       byte
	Quiet        bool
	Panic        bool
	TestDuration time.Duration
	testTimer    *time.Timer
	connected    time.Time
}

func (c Publisher) ClientId() string {
	return fmt.Sprintf("pub-%d", c.id)
}

func (c Publisher) BrokerUrl() string {
	return c.brokerURL
}

func (c Publisher) BrokerUser() string {
	return c.brokerUser
}

func (c Publisher) BrokerPass() string {
	return c.brokerPass
}

func (c Publisher) PanicMode() bool {
	return c.Panic
}

func (c Publisher) Run(res chan *RunResults) {
	newMsgs := make(chan *Message)
	pubMsgs := make(chan *Message)
	doneGen := make(chan bool)
	donePub := make(chan bool)
	runResults := &RunResults{
		ID: c.ClientId(),
	}

	c.testTimer = time.NewTimer(c.TestDuration)

	// start generator
	go c.genMessages(newMsgs, doneGen)
	// start publisher
	go c.pubMessages(newMsgs, pubMsgs, doneGen, donePub)

	times := make([]float64, 0, c.MsgCount)
	for {
		select {
		case m := <-pubMsgs:
			if m.Error {
				log.Printf("CLIENT %v ERROR publishing message: %v: at %v\n", c.ClientId(), m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				runResults.Successes++
				times = append(times, float64(m.Delivered.Sub(m.Sent).Milliseconds())) // in milliseconds
			}
		case <-donePub:
			runResults = c.prepareResult(runResults, times)
			res <- runResults
			return
		case <-c.testTimer.C:
			runResults = c.prepareResult(runResults, times)
			res <- runResults
			return
		}
	}
}

func (c Publisher) genMessages(ch chan *Message, done chan bool) {
	for i := 0; i < c.MsgCount || c.MsgCount == 0; i++ {
		ch <- &Message{
			Topic:   c.MsgTopic,
			QoS:     c.MsgQoS,
			Payload: make([]byte, c.MsgSize),
		}
	}
	done <- true
}

func (c *Publisher) pubMessages(in, out chan *Message, doneGen, donePub chan bool) {
	onConnected := func(client mqtt.Client) {
		c.connected = time.Now()
		if !c.Quiet {
			log.Printf("CLIENT %v is connected to the broker %v and topic %v\n", c.ClientId(), c.BrokerUrl(), c.MsgTopic)
		}
		for {
			select {
			case m := <-in:
				m.Sent = time.Now()
				token := client.Publish(m.Topic, m.QoS, false, m.Payload)
				token.Wait()
				if token.Error() != nil {
					log.Printf("CLIENT %v Error sending message: %v\n", c.ClientId(), token.Error())
					if c.Panic {
						panic(token.Error())
					}
					m.Error = true
				} else {
					m.Delivered = time.Now()
					m.Error = false
				}
				out <- m
			case <-doneGen:
				donePub <- true
				if !c.Quiet {
					log.Printf("CLIENT %v is done publishing\n", c.ClientId())
				}
				return
			}
		}
	}

	connect(c, onConnected)
}

func (c Publisher) prepareResult(runResults *RunResults, times []float64) *RunResults {
	duration := time.Since(c.connected)
	runResults.MsgTimeMin = stats.StatsMin(times)
	runResults.MsgTimeMax = stats.StatsMax(times)
	runResults.MsgTimeMean = stats.StatsMean(times)
	runResults.ClientRunTime = duration.Seconds()

	// calculate std if sample is > 1, otherwise leave as 0 (convention)
	if c.MsgCount > 1 {
		runResults.MsgTimeStd = stats.StatsSampleStandardDeviation(times)
	}

	return runResults
}
