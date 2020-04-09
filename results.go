package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GaryBoone/GoStats/stats"
)

// RunResults describes results of a single client / run
type RunResults struct {
	ID            string  `json:"id"`
	Successes     int64   `json:"successes"`
	Failures      int64   `json:"failures"`
	ClientRunTime float64 `json:"run_time"`
	MsgTimeMin    float64 `json:"msg_time_min"`
	MsgTimeMax    float64 `json:"msg_time_max"`
	MsgTimeMean   float64 `json:"msg_time_mean"`
	MsgTimeStd    float64 `json:"msg_time_std"`
}

// TotalResults describes results of all clients / runs
type TotalResults struct {
	TestRunID    string  `json:"run_id"`
	TestInstance string  `json:"run_instance"`
	TestRunType  string  `json:"run_type"` //pub or sub
	Clients      int     `json:"num_clients"`
	Topics       int     `json:"num_topics"`
	Messages     int     `json:"num_messages"`
	MessageSize  int     `json:"message_size"`
	Dop          int     `json:"dop"`
	QoS          int     `json:"qos"`
	Ratio        float64 `json:"ratio"`
	Successes    int64   `json:"successes"`
	Failures     int64   `json:"failures"`
	TotalRunTime float64 `json:"total_run_time"`

	ClientRunTimeMin  float64 `json:"client_run_time_min"`
	ClientRunTimeMax  float64 `json:"client_run_time_max"`
	ClientRunTimeMean float64 `json:"client_run_time_mean"`
	ClientRunTimeStd  float64 `json:"client_run_time_std"`

	MsgPerClientMin  float64 `json:"msg_per_client_min"`
	MsgPerClientMax  float64 `json:"msg_per_client_max"`
	MsgPerClientMean float64 `json:"msg_per_client_mean"`
	MsgPerClientStd  float64 `json:"msg_per_client_std"`

	MsgTimeMin  float64 `json:"msg_time_min"`
	MsgTimeMax  float64 `json:"msg_time_max"`
	MsgTimeMean float64 `json:"msg_time_mean_mean"`
	MsgTimeStd  float64 `json:"msg_time_mean_std"`

	// TotalMsgsPerSec is a total average throughput, calculated as sum of all messages
	// from all clients divided by total execution time
	TotalMsgsPerSec float64 `json:"total_msgs_per_sec"`

	// AvgMsgsPerSec is an average throughput per client, calculated as sum
	// of individual client throughputs divided by the number of clients.
	AvgMsgsPerSec float64 `json:"avg_msgs_per_sec"`
}

// JSONResults are used to export results as a JSON document
type JSONResults struct {
	Runs   []*RunResults `json:"runs"`
	Totals *TotalResults `json:"totals"`
}

var (
	// Replace with your Workspace ID
	customerID = os.Getenv("LOGANALYTICS_CUSTOMER_ID")

	// Replace with your Primary or Secondary ID
	sharedKey = os.Getenv("LOGANALYTICS_SHARED_KEY")

	//Specify the name of the record type that you'll be creating
	logType = os.Getenv("LOGANALYTICS_LOG_NAME")

	//Specify a field with the created time for the records
	timeStampField = "DateValue"
)

func calculateTotalResults(runID string, results []*RunResults, totalTime time.Duration,
	testType string, clients int, topics int, messages int, size int, qos int, dop int) *TotalResults {

	totals := new(TotalResults)
	var hostname, _ = os.Hostname()
	totals.TestRunID = runID
	totals.TestInstance = hostname
	totals.TotalRunTime = totalTime.Seconds()

	msgTimeMeans := make([]float64, len(results))
	msgsPerClient := make([]float64, len(results))
	msgsPerSecs := make([]float64, len(results))
	runTimes := make([]float64, len(results))

	totals.MsgTimeMin = results[0].MsgTimeMin
	for i, res := range results {
		totals.Successes += res.Successes
		totals.Failures += res.Failures

		if res.MsgTimeMin < totals.MsgTimeMin {
			totals.MsgTimeMin = res.MsgTimeMin
		}

		if res.MsgTimeMax > totals.MsgTimeMax {
			totals.MsgTimeMax = res.MsgTimeMax
		}

		msgTimeMeans[i] = res.MsgTimeMean
		msgsPerClient[i] = float64(res.Successes + res.Failures)
		runTimes[i] = res.ClientRunTime
		msgsPerSecs[i] = float64(res.Successes) / res.ClientRunTime

	}
	totals.TestRunType = testType
	totals.QoS = qos
	totals.Dop = dop
	totals.Clients = clients
	totals.Topics = topics
	totals.Messages = messages
	totals.MessageSize = size

	totals.TotalMsgsPerSec = float64(totals.Successes) / totals.TotalRunTime
	totals.Ratio = float64(totals.Successes) / float64(totals.Successes+totals.Failures)
	totals.AvgMsgsPerSec = stats.StatsMean(msgsPerSecs)

	totals.ClientRunTimeMean = stats.StatsMean(runTimes)
	totals.ClientRunTimeMin = stats.StatsMin(runTimes)
	totals.ClientRunTimeMax = stats.StatsMax(runTimes)

	totals.MsgPerClientMean = stats.StatsMean(msgsPerClient)
	totals.MsgPerClientMin = stats.StatsMin(msgsPerClient)
	totals.MsgPerClientMax = stats.StatsMax(msgsPerClient)

	totals.MsgTimeMin = stats.StatsMin(msgTimeMeans)
	totals.MsgTimeMax = stats.StatsMax(msgTimeMeans)
	totals.MsgTimeMean = stats.StatsMean(msgTimeMeans)

	// calculate std if # of clients is > 1, otherwise leave as 0 (convention)
	if clients > 1 {
		totals.ClientRunTimeStd = stats.StatsSampleStandardDeviation(runTimes)
		totals.MsgTimeStd = stats.StatsSampleStandardDeviation(msgTimeMeans)
		totals.MsgPerClientStd = stats.StatsSampleStandardDeviation(msgsPerClient)
	}

	return totals
}

func publishResults(results []*RunResults, totals *TotalResults) {
	log.Println("Publishing test results...")

	data, err := json.Marshal(totals)
	if err != nil {
		log.Fatalf("Error marshalling results: %v", err)
		return
	}

	err = sendResults(string(data))
	if err != nil {
		log.Printf("Error publishing test results to log analytics: %v", err)
	}
	log.Println("Done")
}

func printResults(results []*RunResults, totals *TotalResults) {
	fmt.Printf("========= TEST PARAMS =========\n")
	fmt.Printf("Test Run Id:                      %v\n", totals.TestRunID)
	fmt.Printf("Test Instance:                    %v\n", totals.TestInstance)
	fmt.Printf("Test Type:                        %v\n", totals.TestRunType)
	fmt.Printf("Number of Clients:                %v\n", totals.Clients)
	fmt.Printf("Number of Topics:                 %v\n", totals.Topics)
	if totals.Messages > 0 {
		fmt.Printf("Messages per Client:              %v\n", totals.Messages)
	}
	fmt.Printf("Messag size (bytes):              %v\n", totals.MessageSize)
	fmt.Printf("QoS:                              %v\n", totals.QoS)
	fmt.Printf("DOP (Max threads):                %v\n", totals.Dop)
	fmt.Printf("========= TEST RESULTS =========\n")
	fmt.Printf("Total Ratio:                      %.3f (%d/%d)\n", totals.Ratio, totals.Successes, totals.Successes+totals.Failures)
	fmt.Printf("Total Runtime (sec):              %.3f\n", totals.TotalRunTime)
	fmt.Printf("Client Runtime Avg (sec):         %.3f\n", totals.ClientRunTimeMean)
	fmt.Printf("Client Runtime Min (sec):         %.3f\n", totals.ClientRunTimeMin)
	fmt.Printf("Client Runtime Max (sec):         %.3f\n", totals.ClientRunTimeMax)
	fmt.Printf("Client Runtime Std (sec):         %.3f\n", totals.ClientRunTimeStd)

	fmt.Printf("Messages per Client Avg:         %.3f\n", totals.MsgPerClientMean)
	fmt.Printf("Messages per Client Min:         %.3f\n", totals.MsgPerClientMin)
	fmt.Printf("Messages per Client Max:         %.3f\n", totals.MsgPerClientMax)
	fmt.Printf("Messages per Client Std:         %.3f\n", totals.MsgPerClientStd)

	fmt.Printf("Msg Latency Avg (ms):             %.3f\n", totals.MsgTimeMean)
	fmt.Printf("Msg Latency Min (ms):             %.3f\n", totals.MsgTimeMin)
	fmt.Printf("Msg Latency Max (ms):             %.3f\n", totals.MsgTimeMax)
	fmt.Printf("Msg Latency Std (ms):             %.3f\n", totals.MsgTimeStd)
	fmt.Printf("Avg Bandwidth p/client (msg/sec): %.3f\n", totals.AvgMsgsPerSec)
	fmt.Printf("Total Test Bandwidth (msg/sec):   %.3f\n", totals.TotalMsgsPerSec)
	fmt.Printf("==============================\n")
}

// sendResults posts json data to azure log analytics.
func sendResults(data string) error {

	dateString := time.Now().UTC().Format(time.RFC1123)
	dateString = strings.Replace(dateString, "UTC", "GMT", -1)

	stringToHash := "POST\n" + strconv.Itoa(utf8.RuneCountInString(data)) + "\napplication/json\n" + "x-ms-date:" + dateString + "\n/api/logs"
	hashedString, err := buildSignature(stringToHash, sharedKey)
	if err != nil {
		return err
	}

	signature := "SharedKey " + customerID + ":" + hashedString
	url := "https://" + customerID + ".ods.opinsights.azure.com/api/logs?api-version=2016-04-01"

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewReader([]byte(data)))
	if err != nil {
		return err
	}

	req.Header.Add("Log-Type", logType)
	req.Header.Add("Authorization", signature)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("x-ms-date", dateString)
	req.Header.Add("time-generated-field", timeStampField)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if err != nil {
		return err
	}

	log.Println(resp.Status)
	return nil
}

func buildSignature(message, secret string) (string, error) {

	keyBytes, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", err
	}

	mac := hmac.New(sha256.New, keyBytes)
	mac.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}
