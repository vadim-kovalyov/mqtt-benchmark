package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"
)

func main() {

	var (
		pub      = flag.Bool("pub", false, "Indicates to initialize te test client as a publisher")
		sub      = flag.Bool("sub", false, "Indicates to initialize the test client as a subscriber")
		broker   = flag.String("broker", "tcp://localhost:1883", "MQTT broker endpoint as scheme://host:port")
		topic    = flag.String("topic", "/test", "MQTT topic for outgoing messages")
		topics   = flag.Int("topics", 1, "Number of topics to use")
		username = flag.String("username", "", "MQTT username (empty if auth disabled)")
		password = flag.String("password", "", "MQTT password (empty if auth disabled)")
		qos      = flag.Int("qos", 1, "QoS for published messages")
		size     = flag.Int("size", 100, "Size of the messages payload (bytes)")
		count    = flag.Int("count", 100, "Number of messages to send or receive per client")
		clients  = flag.Int("clients", 10, "Number of clients to start")
		quiet    = flag.Bool("quiet", false, "Suppress logs while running")
		dop      = flag.Int("dop", 1, "Max number of threads")
		runID    = flag.String("runId", "", "Test Run Id, used for reporting results")
	)

	flag.Parse()
	if !(*pub != *sub) {
		log.Fatalf("Invalid arguments: must speficy either pub or sub mode")
		return
	}

	if *clients < 1 {
		log.Fatalf("Invalid arguments: number of clients should be > 1, given: %v", *clients)
		return
	}

	if *count < 1 {
		log.Fatalf("Invalid arguments: messages count should be > 1, given: %v", *count)
		return
	}

	if *topics < 1 {
		log.Fatalf("Invalid arguments: topics count should be > 1, given: %v", *topics)
		return
	}

	runtime.GOMAXPROCS(*dop)

	resCh := make(chan *RunResults)
	start := time.Now()
	for i := 0; i < *clients; i++ {
		if !*quiet {
			log.Println("Starting client ", i)
		}
		if *pub {
			c := Publisher{
				id:         i,
				brokerURL:  *broker,
				brokerUser: *username,
				brokerPass: *password,
				MsgTopic:   fmt.Sprintf("%s%d", *topic, i%(*topics)),
				MsgSize:    *size,
				MsgCount:   *count,
				MsgQoS:     byte(*qos),
				Quiet:      *quiet,
			}
			go c.Run(resCh)
		} else {
			c := Subscriber{
				id:         i,
				brokerURL:  *broker,
				brokerUser: *username,
				brokerPass: *password,
				MsgTopic:   fmt.Sprintf("%s%d", *topic, i%(*topics)),
				MsgSize:    *size,
				MsgCount:   *count,
				MsgQoS:     byte(*qos),
				Quiet:      *quiet,
			}
			go c.Run(resCh)
		}
	}

	// collect the results
	results := make([]*RunResults, *clients)
	for i := 0; i < *clients; i++ {
		results[i] = <-resCh
	}
	totalTime := time.Since(start)
	testType := "pub"
	if *sub {
		testType = "sub"
	}
	totals := calculateTotalResults(*runID, results, totalTime, testType, *clients, *count, *size, *qos, *dop)

	// print stats
	printResults(results, totals)
	publishResults(results, totals)
}
