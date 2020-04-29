package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	pilosa "github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/encoding/proto"
)

var host string
var index string
var field string
var keyStr string
var keys []string

func init() {
	flag.StringVar(&host, "host", "", "pilosa host:port")
	flag.StringVar(&index, "index", "", "index name")
	flag.StringVar(&field, "field", "", "field name")
	flag.StringVar(&keyStr, "keys", "", "comma-separated list of string")
	flag.Parse()

	keys = strings.Split(keyStr, ",")
}

func main() {
	if index == "" {
		log.Fatal("-index is required")
	}
	var foundKey bool
	for i := range keys {
		if strings.Trim(keys[i], " ") != "" {
			foundKey = true
		}
	}
	if !foundKey {
		log.Fatal("-keys is required")
	}
	if host == "" {
		host = "localhost:10101"
	}

	api, err := pilosa.NewAPI()
	if err != nil {
		log.Fatal(err)
	}
	api.Serializer = proto.Serializer{}

	reqBody, err := api.Serializer.Marshal(&pilosa.TranslateKeysRequest{
		Index: index,
		Field: field,
		Keys:  keys,
	})
	if err != nil {
		log.Fatal(err)
	}

	apiUrl := fmt.Sprintf("http://%s", host)
	resource := "/internal/translate/keys"

	u, _ := url.ParseRequestURI(apiUrl)
	u.Path = resource
	urlStr := u.String()

	r, err := http.NewRequest("POST", urlStr, bytes.NewReader(reqBody))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Accept", "application/x-protobuf")

	client := &http.Client{}
	resp, _ := client.Do(r)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("StatusCode: %v", resp.StatusCode)
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	tkr := pilosa.TranslateKeysResponse{}
	err = api.Serializer.Unmarshal(bodyBytes, &tkr)
	if err != nil {
		log.Fatal(err)
	}

	for i := range tkr.IDs {
		fmt.Printf("%s: %d\n", keys[i], tkr.IDs[i])
	}
}
