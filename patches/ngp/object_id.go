package ngp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type (
	NgpProcMeta = struct {
		NodeID   string `json:",omitempty"`
		Object   string `json:",omitempty"`
		NoExport bool   `json:",omitempty"` // processes whose metrics should not be exported
	}
	ProcReq  = []string
	ProcResp = map[string]NgpProcMeta
)

func drmonkeyServerPort() string {
	value := os.ExpandEnv("$DRMONKEY_PORT")
	if value == "" {
		value = "4692"
		os.Setenv("DRMONKEY_PORT", value)
		log.Printf("DrMonkey server port not provided, used default:%v", value)
	}
	return value
}

func RequestProcObjects(list ProcReq) (ProcResp, error) {
	data := new(bytes.Buffer)
	json.NewEncoder(data).Encode(list)
	var (
		url      = fmt.Sprintf("http://localhost:%v%v", drmonkeyServerPort(), "/api/v1/internal/scanner/proc_object")
		req, err = http.NewRequest("GET", url, data)
	)
	if err != nil {
		return nil, err
	}
	body, err := doRequest(req)
	if err != nil {
		return nil, err
	}
	resp := ProcResp{}
	json.Unmarshal(body, &resp)
	return resp, nil
}

func doRequest(req *http.Request) ([]byte, error) {
	client := http.Client{Timeout: time.Second * 10}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, err
}
