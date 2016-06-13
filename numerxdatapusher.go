package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Creates a new file upload http request with optional extra params
func newfileUploadRequest(uri string, resource string, params map[string]string, paramName, path string) (*http.Request, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	file.Close()

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(paramName, fi.Name())
	if err != nil {
		return nil, err
	}
	part.Write(fileContents)

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest("POST", uri+resource, body)

	values := request.URL.Query()
	for key, val := range params {
		values.Add(key, val)
	}

	request.URL.RawQuery = values.Encode()

	request.Header.Add("Content-Type", writer.FormDataContentType())
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Authorization", authorizationKey)

	return request, err
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
	appName          string
)

const (
	version = "0.9"
	csvExt  = "csv"
)

func init() {
	initParams()

	flagAuthorization := flag.String("a", "", "`Authorization key`")
	flagBaseUrl := flag.String("b", "", "`Base URL` for NumerXData service")
	flagRQType := flag.String("t", string(param_RQ_Viewership), "`Request/data type`")
	flagFileName := flag.String("f", "", "Input `filename` to process")
	flagDirName := flag.String("d", "", "Working `directory` for input files, default extension *.csv")
	flagConcurrency := flag.Int("c", 20, "The number of files to process `concurrent`ly")
	flagVerbose := flag.Bool("v", false, "`Verbose`: outputs to the screen")

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
	fmt.Printf("\tprompt$>%s -a <auth_key> -b <base_url> -t <request-type> [-f <filename> OR -d <dir>] -v \n", appName)
	fmt.Println("Provide either file or dir. Dir takes over file, if both provided")
	flag.Usage()
	os.Exit(-1)
}

func printEnv() {
	fmt.Printf("Provided: -a: %s, -b: %s, -r: %v, -f: %s, -d: %s, -c: %v, -v: %v \n",
		authorizationKey,
		baseUrl,
		requestType,
		inFileName,
		dirName,
		concurrency,
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

	if verbose {
		printEnv()
	}

	if !ValidateRQType() {
		os.Exit(-1)
	}

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

	startTime := time.Now()
	// This is our semaphore/pool
	sem := make(chan bool, concurrency)

	files := getFilesToProcess()

	for _, eachFile := range files {
		// if we still have available goroutine in the pool (out of concurrency )
		sem <- true

		// fire one file to be processed in a goroutine

		fmt.Println("About to process: ", eachFile)
		go func(fileName string) {
			// Signal end of processing at the end
			defer func() { <-sem }()

			var extraParams map[string]string = make(map[string]string)

			switch requestType {
			case RQ_Viewership:
				extraParams["timestamp"] = "event_date"
				extraParams["format"] = "event_date,timestamp,regex%20(.*),%241%2000:00:00"
				extraParams["csvHeaderLine"] = "1"

			case RQ_MetaChanMap:
			case RQ_MetaBilling:
			case RQ_MetaProgram:
			case RQ_MetaEventMap:
				extraParams["key"] = KeyValues[requestType]
				extraParams["csvHeaderLine"] = "1"
			}

			request, err := newfileUploadRequest(baseUrl, string(requestType), extraParams, "file", eachFile)
			if err != nil {
				log.Fatal(err)
			}

			if verbose {
				fmt.Println("RQ URL: ", request.URL)
				fmt.Println("Headers: ", request.Header)
				//fmt.Println("RQ: ", request)
			}

			// TMP - just print the RQ and return from the goroutine
			return

			client := &http.Client{}
			resp, err := client.Do(request)
			if err != nil {
				log.Fatal(err)
			} else {
				var bodyContent []byte
				fmt.Println(resp.StatusCode)
				fmt.Println(resp.Header)
				resp.Body.Read(bodyContent)
				resp.Body.Close()
				fmt.Println(bodyContent)
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
	// Done all gouroutines, close the total channel

	fmt.Printf("Processed %d files, in %v\n", len(files), time.Since(startTime))

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
