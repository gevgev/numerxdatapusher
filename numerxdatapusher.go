package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Creates a new file upload http request with optional extra params
func newfileUploadRequest(uri string, resource string, params map[string]string, path string) (*http.Request, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	file.Close()

	request, err := http.NewRequest("POST", uri+resource, bytes.NewBuffer([]byte(fileContents)))

	if err != nil {
		fmt.Println("Could not allocate new request object: ", err)
		return nil, err
	}

	values := request.URL.Query()
	for key, val := range params {
		values.Add(key, val)
	}

	request.URL.RawQuery = values.Encode()

	request.Header.Add("Accept", "application/json")
	request.Header.Add("Authorization", authorizationKey)
	request.Header.Add("Content-Type", "text/csv")

	return request, err
}

// Creates a new GET http request to check the status of previously submitted (through POST) file processing jobs
func fileUploadStatusRequest(uri string, resource string, params map[string]string) (*http.Request, error) {

	request, err := http.NewRequest("GET", uri+resource, nil)
	if err != nil {
		return nil, err
	}

	values := request.URL.Query()
	for key, val := range params {
		values.Add(key, val)
	}

	request.URL.RawQuery = values.Encode()

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Authorization", authorizationKey)

	return request, nil
}

type RQType string

const (
	RQ_Viewership   RQType = "/events/viewer"
	RQ_MetaChanMap  RQType = "/meta/chanmap"
	RQ_MetaBilling  RQType = "/meta/billing"
	RQ_MetaProgram  RQType = "/meta/program_id"
	RQ_MetaEventMap RQType = "/meta/eventmap"
)

type RQTypeParam string

const (
	param_RQ_Viewership   RQTypeParam = "events"
	param_RQ_MetaChanMap  RQTypeParam = "meta-chanmap"
	param_RQ_MetaBilling  RQTypeParam = "meta-billing"
	param_RQ_MetaProgram  RQTypeParam = "meta-program"
	param_RQ_MetaEventMap RQTypeParam = "meta-eventmap"
)

type DataType struct {
	RQTypeParam
	RQType
}

var dataType = [...]DataType{
	{param_RQ_Viewership, RQ_Viewership},
	{param_RQ_MetaChanMap, RQ_MetaChanMap},
	{param_RQ_MetaBilling, RQ_MetaBilling},
	{param_RQ_MetaProgram, RQ_MetaProgram},
	{param_RQ_MetaEventMap, RQ_MetaEventMap},
}
var DataTypes map[RQTypeParam]RQType

type KeyValue struct {
	RQType
	key string
}

// TODO: add mso key to meta - channel map and meta-program whenever the CSV data will have MSO column
var keyValue = [...]KeyValue{
	{RQ_Viewership, ""},
	{RQ_MetaChanMap, "display_channel_number"},
	{RQ_MetaBilling, "device_id"},
	{RQ_MetaProgram, "ID"},
	{RQ_MetaEventMap, "Event_Type"},
}

var KeyValues map[RQType]string

func initParams() {
	DataTypes = make(map[RQTypeParam]RQType)
	KeyValues = make(map[RQType]string)

	for _, dataType_i := range dataType {
		DataTypes[dataType_i.RQTypeParam] = dataType_i.RQType
	}

	for _, keyValue_i := range keyValue {
		KeyValues[keyValue_i.RQType] = keyValue_i.key
	}
}

var (
	authorizationKey string
	baseUrl          string
	requestType      RQType
	param_RQ_T       string
	inFileName       string
	dirName          string
	concurrency      int
	verbose          bool
	singleFileMode   bool
	retryNumber      int
	appName          string
	timeout          time.Duration
)

const (
	version       = "0.9"
	csvExt        = "csv"
	TIMEOUT       = 1
	RETRY_DEFAULT = 3
)

func init() {
	initParams()

	flagAuthorization := flag.String("a", "", "`Authorization key`")
	flagBaseUrl := flag.String("b", "", "`Base URL` for NumerXData service")
	flagRQType := flag.String("t", string(param_RQ_Viewership), "`Request/data type`")
	flagFileName := flag.String("f", "", "Input `filename` to process")
	flagDirName := flag.String("d", "", "Working `directory` for input files, default extension *.csv")
	flagConcurrency := flag.Int("c", 20, "The number of files to process `concurrent`ly")
	flagVerbose := flag.Bool("v", true, "`Verbose`: outputs to the screen")
	flagTimeout := flag.Int("s", TIMEOUT, "`Sleep time` in minutes")
	flagRetryNumber := flag.Int("r", RETRY_DEFAULT, "`Retry` number")

	flag.Parse()
	if flag.Parsed() {
		authorizationKey = *flagAuthorization
		baseUrl = *flagBaseUrl
		param_RQ_T = *flagRQType
		requestType = DataTypes[RQTypeParam(param_RQ_T)]
		inFileName = *flagFileName
		dirName = *flagDirName
		concurrency = *flagConcurrency
		verbose = *flagVerbose
		timeout = time.Duration(*flagTimeout)
		retryNumber = *flagRetryNumber

		appName = os.Args[0]
		if inFileName == "" && dirName == "" && len(os.Args) == 2 {
			inFileName = os.Args[1]
		}
	} else {
		usage()
	}

}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s -a <auth_key> -b <base_url> -t <request-type> [-f <filename> OR -d <dir>] -s <minutes> -v \n", appName)
	fmt.Println("Provide either file or dir. Dir takes over file, if both provided")
	flag.Usage()
	os.Exit(-1)
}

func printEnv() {
	fmt.Printf("Provided: -a: %s, -b: %s, -r: %v, -f: %s, -d: %s, -c: %v, -s: %v, -v: %v \n",
		authorizationKey,
		baseUrl,
		requestType,
		inFileName,
		dirName,
		concurrency,
		timeout,
		verbose,
	)
}

func ValidateRQType() bool {
	if requestType == "" {
		fmt.Println("Wrong request type parameter value provided: ", param_RQ_T)
		fmt.Println("Valid values are:")
		for _, rq_type := range dataType {
			fmt.Println(rq_type.RQTypeParam)
		}
		return false
	}

	return true
}

type StatusType string

const (
	Success StatusType = "success"
	Failure StatusType = "failed"
)

type EventProcessingSteps string

const (
	RawEventData    EventProcessingSteps = "rawevent"
	ParsedEventData EventProcessingSteps = "parsedevent"
	IndexEventData  EventProcessingSteps = "eventindexstatus"
)

type MetaProcessingSteps string

const (
	RawMetaData    EventProcessingSteps = "rawmeta"
	ParsedMetaData EventProcessingSteps = "parsedmeta"
	IndexMetaData  EventProcessingSteps = "metaindexstatus"
)

type JobType struct {
	JobId    string
	Filename string
}

// Check status for a job
func jobCompleted(job JobType) bool {
	// Call numerxData server to check the status of this job
	// return true if we get:
	// 		[“step”=”metaindexstatus”, “status”=”success”]
	//	or [“step”=“eventindexstatus”, “status” = “success”]
	/*
		[
			{"ID":"0.0.LqO~iOvJV3sdUOd8","Step":"metaindexstatus","Status":"success","Timestamp":1465589455508,"Notes":""},
			{"ID":"0.0.LqO~iOvJV3sdUOd8","Step":"parsedmeta","Status":"success","Timestamp":1465588843502,"Notes":""},
			{"ID":"0.0.LqO~iOvJV3sdUOd8","Step":"rawmeta","Status":"success","Timestamp":1465588543502,"Notes":""}
		]
	*/
	// uri string, resource string, params map[string]string
	var params map[string]string = make(map[string]string)
	params["id"] = job.JobId
	request, err := fileUploadStatusRequest(baseUrl, "/status", params)
	if err != nil {
		log.Println(err)
	}

	if verbose {
		fmt.Println("RQ URL: ", request.URL)
		fmt.Println("RQ Headers: ", request.Header)
		fmt.Println("RQ Body: ", request)
	}

	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		log.Println(err)
	} else {
		/* JSON
		[
			{"ID":"0.0.LqO~iOvJV3sdUOd8","Step":"metaindexstatus","Status":"success","Timestamp":1465589455508,"Notes":""},
			{"ID":"0.0.LqO~iOvJV3sdUOd8","Step":"parsedmeta","Status":"success","Timestamp":1465588843502,"Notes":""},
			{"ID":"0.0.LqO~iOvJV3sdUOd8","Step":"rawmeta","Status":"success","Timestamp":1465588543502,"Notes":""}
		]
		*/
		defer resp.Body.Close()

		var bodyContent []byte
		if verbose {
			fmt.Println("Status RS Status: ", resp.StatusCode)
			fmt.Println("Status RS Headers: ", resp.Header)
		}

		bodyContent, err := ioutil.ReadAll(resp.Body)

		if verbose {
			fmt.Println("Status RS Content: error? :", err)
			fmt.Println("Status RS Content: body: bytes:  ", bodyContent)
			fmt.Println("Status RS Content: body: string: ", string(bodyContent))
		}
		if resp.StatusCode == 200 {
			// Check the step's status
			status, err := getStatusResponse(bodyContent)
			if err != nil {
				fmt.Printf("Error %v while checking status for %v, file: %v \n", err, job.JobId, job.Filename)
			} else {
				switch requestType {
				case RQ_Viewership:
					for _, entry := range status {
						switch entry.Step {
						case string(IndexEventData): // "eventindexstatus":
							switch entry.Status {
							case string(Success): // "success":
								if verbose {
									fmt.Println("@", time.Now())
									fmt.Printf("Complete for: %s, file: %s\n", job.JobId, job.Filename)
									fmt.Println("Current state: ", status)
								}
								return true
							case string(Failure): // "failure":
								failedJobsChan <- job
								return true
							}
						case string(ParsedEventData), string(RawEventData):
							if entry.Status == string(Failure) {
								failedJobsChan <- job
								return true
							}
						}
					}

					if verbose {
						fmt.Println("@", time.Now())
						fmt.Printf("Not yet: %s, file: %s\n", job.JobId, job.Filename)
						fmt.Println("Current state: ", status)
					}

				//	(actually the new struct with file-name, id, and retry-number)
				case RQ_MetaBilling, RQ_MetaProgram, RQ_MetaChanMap, RQ_MetaEventMap:
					for _, entry := range status {
						switch entry.Step {
						case string(IndexMetaData): // "metaindexstatus":
							switch entry.Status {
							case string(Success): // "success":
								if verbose {
									fmt.Println("@", time.Now())
									fmt.Printf("Complete for: %s, file: %s\n", job.JobId, job.Filename)
									fmt.Println("Current state: ", status)
								}
								return true
							case string(Failure): // "failure":
								failedJobsChan <- job
								return true
							}
						case string(ParsedMetaData), string(RawMetaData):
							if entry.Status == string(Failure) {
								failedJobsChan <- job
								return true
							}
						}
					}

					if verbose {
						fmt.Println("@", time.Now())
						fmt.Printf("Not yet: %s, file: %s\n", job.JobId, job.Filename)
						fmt.Println("Current state: ", status)
					}

				}
			}
		} else {
			log.Println("Error Status %v while checking status for %v, file: %s \n", err, job.JobId, job.Filename)
			failedJobsChan <- job
			if verbose {
				fmt.Println("Error Status %v while checking status for %v, file: %s \n", err, job.JobId, job.Filename)
			}
			return true
		}
	}

	return false
}

// General loop-function to wait for a job to complete on numerx side
func waitingForJob(job JobType, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		// wait enough...
		if verbose {
			fmt.Println("Waiting for ", job.JobId)
		}
		time.Sleep(timeout * time.Minute)
		// Check if the numerx server has completed this job yet
		if jobCompleted(job) {
			return
		}
	}
}

var filesQueueChan chan string
var jobsInProcessChann chan JobType
var failedJobsChan chan JobType

func main() {

	/*
	   curl
	   	-H "Accept: application/json"
	   	-H "Content-Type: text/csv"
	   	-H "Authorization: EAP apikey:00000000-1234-5678-0000-000000000000"
	   	--data-binary @./data_Panhandle_only/Panhandle_viewership_pure_0501.csv
	   	"http://localhost:8080/api/v1/roviqa/events/viewer?timestamp=event_date&format=event_date,timestamp,regex%20(.*),%241%2000:00:00&csvHeaderLine=1"

	   	"...?timestamp=event_date&format=event_date,timestamp,regex%20(.*),%241%2000:00:00&csvHeaderLine=1"
	*/

	/*

	   -a Authorization
	   -f file
	   -b baseUrl
	   -t request/data type
	   	events 			"events/viewer"
	   	meta-chanmap	"meta/chanmap"
	   	meta-billing	"meta/billing"
	   	meta-program_id	"meta/program_id"
	   	meta-eventmap	"meta/eventmap"

	   -d folder to look for CSV files
	   -c concurrency
	*/

	if verbose {
		printEnv()
	}

	if !ValidateRQType() {
		os.Exit(-1)
	}

	/*
		// Get the list of CSV files
		// For each csv file:
		// 		Build the request
		// 		POST the request
		// 		Get the Id from response if 200 Ok
		// 		Start a goroutine for Id check
		// 			Inside goroutine:
		// 				sleep(nnn)
		// 				Check the status for [“step”=”metaindexstatus”, “status”=”success”]
		//								  or [“step”=“eventindexstatus”, “status” = “success”]
		//				if complete ==> exit with an indication of success
		// End for each csv file
		// Wait for all goroutines to end
		// End the app
	*/

	startTime := time.Now()
	// This is our semaphore/pool
	sem := make(chan bool, concurrency)

	filesQueueChan = make(chan string, concurrency)
	jobsInProcessChann = make(chan JobType, concurrency)
	failedJobsChan = make(chan JobType)

	var failedJobs []JobType
	failedJobs = make([]JobType, 0)

	var wg sync.WaitGroup

	// Start listening for failed jobs
	go func() {
		if verbose {
			fmt.Println("Ready to start getting Ids to wait for completeion...")
		}
		for {
			nextFailedJob, more := <-failedJobsChan
			if more {
				if verbose {
					fmt.Println("Got failed job: ", nextFailedJob)
					failedJobs = append(failedJobs, nextFailedJob)
					wg.Done()
				}
			} else {
				if verbose {
					fmt.Println("Got all Failed Jobs, breaking")
				}
				return
			}
		}
	}()

	// Start listening for the job Ids
	go func() {
		if verbose {
			fmt.Println("Ready to start getting Ids to wait for completeion...")
		}
		for {
			nextJob, more := <-jobsInProcessChann
			if more {
				if verbose {
					fmt.Println("Starting waiting for: ", nextJob.JobId)
				}
				go waitingForJob(nextJob, &wg)
			} else {
				if verbose {
					fmt.Println("Got all Ids, breaking")
				}
				return
			}
		}
	}()

	files := getFilesToProcess()

	for _, eachFile := range files {
		// if we still have available goroutine in the pool (out of concurrency )
		sem <- true

		// fire one file to be processed in a goroutine
		wg.Add(1)

		fmt.Println("About to process: ", eachFile)
		go func(fileName string) {
			// Signal end of processing at the end
			defer func() { <-sem }()

			var extraParams map[string]string = make(map[string]string)

			switch requestType {
			case RQ_Viewership:
				extraParams["timestamp"] = "event_date"
				extraParams["format"] = "event_date,timestamp,regex (.*),$1 00:00:00" //"event_date,timestamp,regex%20(.*),%241%2000:00:00"
				extraParams["csvHeaderLine"] = "1"

			case RQ_MetaChanMap, RQ_MetaBilling, RQ_MetaProgram, RQ_MetaEventMap:
				extraParams["key"] = KeyValues[requestType]
				extraParams["csvHeaderLine"] = "1"
			}

			request, err := newfileUploadRequest(baseUrl, string(requestType), extraParams, eachFile)
			if err != nil {
				// Wrong parameters/request - do not try again
				log.Println(err)
				failedJobsChan <- JobType{
					JobId:    err.Error(),
					Filename: eachFile,
				}
				return
			}

			if verbose {
				fmt.Println("POST RQ URL: ", request.URL)
				fmt.Println("POST RQ Headers: ", request.Header)
				fmt.Println("POST RQ Body: ", request)
			}

			client := &http.Client{}

			postRequestSucceeded := false
			var resp *http.Response

			for attemptNumber := 0; attemptNumber < retryNumber; attemptNumber++ {
				resp, err = client.Do(request)
				if err != nil {
					time.Sleep(timeout)
					if verbose {
						fmt.Printf("Attempt # %d for %s failed.\n", attemptNumber+1, eachFile)
					}
				} else {
					postRequestSucceeded = true
					break
				}
			}

			if !postRequestSucceeded {
				log.Println(err)
				failedJobsChan <- JobType{
					JobId:    time.Now().String() + ":" + err.Error(),
					Filename: eachFile,
				}
			} else {

				// JSON {"id" : "0.0.LqO~iOvJV3sdUOd8"}
				defer resp.Body.Close()

				if verbose {
					fmt.Println("POST Status code: ", resp.StatusCode)
					fmt.Println("POST Headers: ", resp.Header)
				}
				bodyContent, err := ioutil.ReadAll(resp.Body)

				if verbose {
					fmt.Printf("POST - File:[%s] Response body: %s\n", eachFile, string(bodyContent))
					fmt.Println("POST - Status RS Content: error? :", err)
					fmt.Println("POST - Status RS Content: body: ", bodyContent)
				}
				if resp.StatusCode == 200 {
					// get the id of the job on numerX server
					// sent this Id to the StatusChecker channel
					jobId, err := GetJobId(bodyContent)
					if err != nil {
						fmt.Printf("Error [%v] for submitting %v \n", err, eachFile)
					} else {
						if verbose {
							fmt.Printf("Posted file [%s] with Id {%s}, about to start checking on status update\n", eachFile, jobId)
						}

						newJob := JobType{
							JobId:    jobId,
							Filename: eachFile,
						}
						jobsInProcessChann <- newJob
					}
				} else if resp.StatusCode == 500 {
					// TODO: if 500 - re POST
					failedJobsChan <- JobType{
						JobId:    "",
						Filename: eachFile,
					}
					return
				} else {
					log.Println("Error Status [%v] for submitting %v \n", err, string(bodyContent))
					if verbose {
						fmt.Println("Error Status [%v] for submitting %v \n", err, string(bodyContent))
					}
					failedJobsChan <- JobType{
						JobId:    "",
						Filename: eachFile,
					}
					return
				}
			}

		}(eachFile)
	}

	// waiting for all goroutines to end
	if verbose {
		fmt.Println("Waiting for all goroutines to complete the work")
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	// Now waiting for status-waiter processes to end
	fmt.Println("Waiting for all status checks to complete")
	wg.Wait()

	// Done all gouroutines, close the jobs listener channel
	fmt.Println("Initial POST files complete, closing jobs processing channel")
	close(jobsInProcessChann)

	// Done all gouroutines, close the failed jobs listener channel
	fmt.Println("Failed jobs processing complete, closing processing channel")
	close(failedJobsChan)

	fmt.Println("jobs channel closed")

	fmt.Printf("Processed %d files, in %v\n", len(files), time.Since(startTime))

	if len(failedJobs) > 0 {
		PrintFailedJobs(failedJobs)
	} else {
		fmt.Println("No failed jobs reported")
	}
}

func PrintFailedJobs(failedJobs []JobType) {
	for _, job := range failedJobs {
		fmt.Printf("Failed job: [%s], file: %s\n", job.JobId, job.Filename)
	}
}

type NumerXPOSTResponse struct {
	Id string `json:"id"`
}

type NumerXStatusResponse struct {
	ID        string `json:"id"`
	Step      string `json:"step"`
	Status    string `json:"status"`
	Timestamp int    `json:"timestamp"`
	Notes     string `json:"notes"`
}

// Unmarshal Status response to []NumerXStatusResponse
func getStatusResponse(bodyContent []byte) ([]NumerXStatusResponse, error) {
	var response []NumerXStatusResponse
	if verbose {
		fmt.Println("Status response: Bytes:  ", bodyContent)
		fmt.Println("Status response: String: ", string(bodyContent))
	}
	err := json.Unmarshal(bodyContent, &response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// Unmarshall POST response to job Id
func GetJobId(bodyContent []byte) (string, error) {
	var response NumerXPOSTResponse
	if verbose {
		fmt.Println("Post response: Bytes:  ", bodyContent)
		fmt.Println("Post response: String: ", string(bodyContent))
	}
	err := json.Unmarshal(bodyContent, &response)
	if err != nil {
		return "", err
	}

	//	if len(response) == 0 {
	//		return "", errors.New("No Id found in response")
	//	}
	if verbose {
		fmt.Println("Post response: Id: ", response.Id)
	}
	return response.Id, nil
}

// Get the list of files to process in the target folder
func getFilesToProcess() []string {
	fileList := []string{}
	singleFileMode = false

	if dirName == "" {
		if inFileName != "" {
			// no Dir name provided, but file name provided =>
			// Single file mode
			singleFileMode = true
			fileList = append(fileList, inFileName)
			return fileList
		} else {
			// no Dir name, no file name
			fmt.Println("Input file name or working directory is not provided")
			usage()
		}
	}

	// We have working directory - takes over single file name, if both provided
	err := filepath.Walk(dirName, func(path string, f os.FileInfo, _ error) error {
		if isCsvFile(path) {
			fileList = append(fileList, path)
		}
		return nil
	})

	if err != nil {
		fmt.Println("Error getting files list: ", err)
		os.Exit(-1)
	}

	return fileList
}

func isCsvFile(fileName string) bool {
	return filepath.Ext(fileName) == "."+csvExt
}
