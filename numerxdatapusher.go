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
)

// Creates a new file upload http request with optional extra params
func newfileUploadRequest(uri string, params map[string]string, paramName, path string) (*http.Request, error) {
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

	for key, val := range params {
		_ = writer.WriteField(key, val)
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequest("POST", uri, body)
	request.Header.Add("Content-Type", writer.FormDataContentType())
	return request, err
}

type RQType string

const (
	RQ_Viewership   RQType = "events/viewer"
	RQ_MetaChanMap  RQType = "meta/chanmap"
	RQ_MetaBilling  RQType = "meta/billing"
	RQ_MetaProgram  RQType = "meta/program_id"
	RQ_MetaEventMap RQType = "meta/eventmap"
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

func initParams() {
	DataTypes = make(map[RQTypeParam]RQType)

	for _, dataType_i := range dataType {
		DataTypes[dataType_i.RQTypeParam] = dataType_i.RQType
	}
}

var (
	authorizationKey string
	baseUrl          string
	requestType      RQType
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
		requestType = DataTypes[RQTypeParam(*flagRQType)]
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

	printEnv()
	os.Exit(0)

	path, _ := os.Getwd()
	path += "/test.pdf"
	extraParams := map[string]string{
		"title":       "My Document",
		"author":      "Matt Aimonetti",
		"description": "A document with all the Go programming language secrets",
	}
	request, err := newfileUploadRequest("https://google.com/upload", extraParams, "file", "/tmp/doc.pdf")
	if err != nil {
		log.Fatal(err)
	}
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
}
