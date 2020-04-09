package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"

	rehttp "github.com/PuerkitoBio/rehttp"
)

func main() {

	var (
		pub         = flag.Bool("pub", false, "Indicates to initialize te test client as a publisher")
		sub         = flag.Bool("sub", false, "Indicates to initialize the test client as a subscriber")
		broker      = flag.String("broker", "tcp://localhost:1883", "MQTT broker endpoint as scheme://host:port")
		topic       = flag.String("topic", "/test", "MQTT topic for outgoing messages")
		topics      = flag.Int("topics", 1, "Number of topics to use")
		username    = flag.String("username", "", "MQTT username (empty if auth disabled)")
		password    = flag.String("password", "", "MQTT password (empty if auth disabled)")
		qos         = flag.Int("qos", 1, "QoS for published messages")
		size        = flag.Int("size", 100, "Size of the messages payload (bytes)")
		count       = flag.Int("count", 0, "Number of messages to send or receive per client. If not specifier - run for '-duration' instead.")
		duration    = flag.Duration("duration", 60*time.Minute, "Maximum duration of the test.")
		clients     = flag.Int("clients", 10, "Number of clients to start")
		quiet       = flag.Bool("quiet", false, "Suppress logs while running")
		dop         = flag.Int("dop", 1, "Max number of threads")
		runID       = flag.String("runId", "", "Test Run Id, used for reporting results")
		waitFor     = flag.String("waitFor", "", "Address of a subscriber tool to wait for, before starting the test.")
		panic       = flag.Bool("panic", false, "If specified, the tool will panic on any connection/protocol error.")
		idleTimeout = flag.Duration("idletimeout", 30*time.Second, "Max idle time b/w incoming messages.")
	)

	flag.Parse()
	if !(*pub != *sub) {
		log.Fatalf("Invalid arguments: must specify either pub or sub mode")
		return
	}

	if *clients < 1 {
		log.Fatalf("Invalid arguments: number of clients should be > 1, given: %v", *clients)
		return
	}

	if *count < 0 {
		log.Fatalf("Invalid arguments: messages count should be >= 0, given: %v", *count)
		return
	}

	if *topics < 1 {
		log.Fatalf("Invalid arguments: topics count should be > 1, given: %v", *topics)
		return
	}

	if *pub && *waitFor != "" {
		log.Printf("Waiting for subscriber at %v to start.", *waitFor)
		waitForSubscriber(*waitFor)
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
				id:           i,
				brokerURL:    *broker,
				brokerUser:   *username,
				brokerPass:   *password,
				MsgTopic:     fmt.Sprintf("%s%d", *topic, i%(*topics)),
				MsgSize:      *size,
				MsgCount:     *count,
				MsgQoS:       byte(*qos),
				Quiet:        *quiet,
				Panic:        *panic,
				TestDuration: *duration,
			}
			go c.Run(resCh)
		} else {
			c := Subscriber{
				id:           i,
				brokerURL:    *broker,
				brokerUser:   *username,
				brokerPass:   *password,
				MsgTopic:     fmt.Sprintf("%s%d", *topic, i%(*topics)),
				MsgSize:      *size,
				MsgCount:     *count,
				MsgQoS:       byte(*qos),
				Quiet:        *quiet,
				Panic:        *panic,
				TestDuration: *duration,
				IdleTimeout:  *idleTimeout,
			}
			go c.Run(resCh)
		}
	}

	log.Printf("All clients have started.")
	if *sub {
		go exposeReadyEndpoint()
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
		totalTime = totalTime - *idleTimeout // subtract IdleTimeout from total duration for subscribers.
	}
	totals := calculateTotalResults(*runID, results, totalTime, testType, *clients, *count, *size, *qos, *dop)

	// print stats
	printResults(results, totals)
	publishResults(results, totals)
}

func exposeReadyEndpoint() {
	log.Printf("Exposing ready endpoint on http://:8080/.")
	http.HandleFunc("/", readyHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Confirming ready status.")
	w.WriteHeader(http.StatusOK)
}

func waitForSubscriber(subAddress string) {
	// retry for 30 sec
	tr := rehttp.NewTransport(
		nil,
		rehttp.RetryAll(rehttp.RetryMaxRetries(30),
			rehttp.RetryIsErr(func(e error) bool { return e != nil })),
		rehttp.ConstDelay(time.Second),
	)
	client := &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second, // Client timeout applies to all retries as a whole
	}
	resp, err := client.Get(subAddress)
	if err != nil {
		log.Fatalf("Did not get the ready confirmation from subscriber in time. Err: %v", err)
		panic(err)
	}
	defer resp.Body.Close()
}
