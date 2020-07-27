package main

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Subscriber struct {
	id           int
	brokerURL    string
	brokerUser   string
	brokerPass   string
	ClientsCount int
	TopicsCount  int
	MsgSize      int
	MsgCount     int
	MsgQoS       byte
	Quiet        bool
	Panic        bool

	// TestDuration is the expected duration of the test.
	// The test will run for *at least* the specified duration,
	// after that if will continue process incoming messages (if any)
	// or stop after IdleTimeout.
	TestDuration time.Duration

	// IdleTimeout is the max idle time b/w incoming messages.
	// If idle timeout reached b/w incoming messages, the test will stop.
	IdleTimeout time.Duration
	testTimer   *time.Timer
	idleTimer   *time.Timer

	// endgame is true when the test duration is over and we just
	// waiting for remaining message queue to drain.
	endgame bool

	// connected is the time when a MQTT client first connected to
	// the broker. User for reporting results
	connected time.Time
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

	c.idleTimer = time.NewTimer(0)
	<-c.idleTimer.C

	c.testTimer = time.NewTimer(c.TestDuration)

	c.subscribe(rcvMsgs, doneSub)

	for {
		select {
		case m := <-rcvMsgs:
			if m.Error {
				log.Printf("CLIENT %v ERROR receiving message: %v: at %v\n", c.ClientId(), m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				runResults.Successes++
				if c.endgame {
					c.idleTimer.Reset(c.IdleTimeout)
				}
			}
		case <-doneSub:
			// Received expected number of messages. Test is over.
			runResults = c.prepareResult(runResults)
			res <- runResults
			return
		case <-c.testTimer.C:
			// Test duration is over, start idle timer.
			if !c.Quiet {
				log.Printf("CLIENT %v test duration is over: %v\n", c.ClientId(), c.TestDuration)
			}
			c.idleTimer.Reset(c.IdleTimeout)
			c.endgame = true
		case <-c.idleTimer.C:
			if !c.Quiet {
				log.Printf("CLIENT %v stopping after idle time: %v\n", c.ClientId(), c.IdleTimeout)
			}
			runResults = c.prepareResult(runResults)
			res <- runResults
			return
		}
	}
}

func (c *Subscriber) subscribe(rcvMsg chan *Message, doneSub chan bool) {
	onConnected := func(client mqtt.Client) {
		c.connected = time.Now()

		ctr := 0
		onMessage := func(inner mqtt.Client, m mqtt.Message) {
			ctr++
			rcvMsg <- &Message{
				Topic: m.Topic(),
				QoS:   m.Qos(),
			}

			if c.MsgCount > 0 && ctr >= c.MsgCount {
				client.Disconnect(1000)
				if !c.Quiet {
					log.Printf("CLIENT %v is done receiving messages\n", c.ClientId())
				}
				doneSub <- true
			}
		}

		topics := getTopicsForSubscriber(c.id, c.ClientsCount, c.TopicsCount, c.MsgQoS)

		if !c.Quiet {
			log.Printf("CLIENT %v is connected to the broker %v and topic(s) %v\n", c.ClientId(), c.BrokerUrl(), topics)
		}

		token := client.SubscribeMultiple(topics, onMessage)
		token.Wait()

		if token.Error() != nil {
			log.Printf("CLIENT %v Error subscribing to the topic %v: %v\n", c.ClientId(), topics, token.Error())
			if c.Panic {
				panic(token.Error())
			}
		}
	}

	connect(c, onConnected)
}

func (c Subscriber) prepareResult(runResults *RunResults) *RunResults {
	duration := time.Since(c.connected)
	duration = duration - c.IdleTimeout // subtract IdleTimeout from total duration.
	runResults.ClientRunTime = duration.Seconds()
	return runResults
}

/// getTopicsForSubscriber calculates the topic filters to subscribe based on the number of topics
/// and the total number of clients. There are two cases: if clients >= topics, then each client subscribes
/// to a single topic filter = [client id] % [topic count].
/// Otherwise we spread the topics among clients, for example (clients = 3, topics = 12)
///	client0: [0, 1, 2, 3]
///	client1: [4, 5, 6, 7]
///	client2: [8, 9, 10, 11]
func getTopicsForSubscriber(subscriberID int, subscribers int, topics int, qos byte) map[string]byte {
	if subscribers >= topics {
		topicName := fmt.Sprintf("/test%d", subscriberID%topics)
		return map[string]byte{topicName: qos}
	}

	topicsPerSubscriber := topics / subscribers
	result := make(map[string]byte)
	topicID := subscriberID * topicsPerSubscriber

	for i := 0; i < topicsPerSubscriber; i++ {
		topicName := fmt.Sprintf("/test%d", topicID)
		result[topicName] = qos
		topicID++
	}
	return result

}
