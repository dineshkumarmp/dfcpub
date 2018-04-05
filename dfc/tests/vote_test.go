package dfc_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/OneOfOne/xxhash"
)

const (
	HRWmLCG32 = 1103515245
)

var (
	voteTests = []Test{
		Test{"Proxy Failure", proxy_failure},
		Test{"Multiple Failures", multiple_failures},
		Test{"Rejoin", rejoin},
	}
	currentPrimaryProxyID = ""
	originalProxyURL      string
	originalProxyPort     string
)

func init() {

}

//===================
//
// Main Test Function
//
//===================

func Test_vote(t *testing.T) {
	parse()

	smap := getClusterMap(httpclient, t)
	if len(smap.Pmap) <= 1 {
		t.Errorf("Not enough proxies to run Test_vote, must be more than 1")
		return
	}

	originalProxyURL = proxyurl
	originalProxyPort = smap.ProxySI.DaemonPort

	for _, test := range voteTests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}

		// Reset Changed Settings
		//		restore(httpclient, originalProxyURL, originalProxyPort)
	}
}

//==========
//
// Subtests
//
//==========

func proxy_failure(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy
	cmd, args, err := kill(httpclient, smap.ProxySI.DirectURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}

	currentPrimaryProxyID = nextProxyID

	// Wait the Keepalive Time (max time it should take to switch)
	time.Sleep(30 * time.Second) // TODO: Keepalive Time

	// Check if the next proxy is the one we found from hrw
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}

	restore(httpclient, originalProxyURL, cmd, args)
}

func multiple_failures(t *testing.T) {

	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy and a target
	primaryProxyURL := smap.ProxySI.DirectURL
	pcmd, pargs, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}

	targetURLToKill := ""
	targetPortToKill := ""
	// Select a random target
	for _, tgtinfo := range smap.Smap {
		targetURLToKill = tgtinfo.DirectURL
		targetPortToKill = tgtinfo.DaemonPort
		break
	}
	tcmd, targs, err := kill(httpclient, targetURLToKill, targetPortToKill)
	if err != nil {
		t.Errorf("Error killing Target: %v", err)
	}

	currentPrimaryProxyID = nextProxyID

	// Wait the Keepalive Time (max time it should take to switch)
	time.Sleep(30 * time.Second) // TODO: Keepalive Time

	// Check if the next proxy is the one we found from hrw
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}

	// Restore the killed target
	restore(httpclient, targetURLToKill, tcmd, targs)
	restore(httpclient, primaryProxyURL, pcmd, pargs)
}

func rejoin(t *testing.T) {

	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	pcmd, pargs, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v")
	}

	// Wait the Keepalive Time (max time it should take to switch)
	time.Sleep(30 * time.Second) // TODO: Keepalive Time

	// Kill a Target
	targetURLToKill := ""
	targetIDToKill := ""
	targetPortToKill := ""
	// Select a random target
	for _, tgtinfo := range smap.Smap {
		targetURLToKill = tgtinfo.DirectURL
		targetIDToKill = tgtinfo.DaemonID
		targetPortToKill = tgtinfo.DaemonPort
		break
	}

	tcmd, targs, err := kill(httpclient, targetURLToKill, targetPortToKill)

	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Smap[targetIDToKill]; ok {
		t.Errorf("Killed Target was not removed from the cluster map: %v", targetIDToKill)
	}
	// Restart that Target
	restore(httpclient, targetURLToKill, tcmd, targs)

	// See that it successfully rejoins the cluster
	smap = getClusterMap(httpclient, t)
	if _, ok := smap.Smap[targetIDToKill]; !ok {
		t.Errorf("Restarted Target did not rejoin the cluster: %v", targetIDToKill)
	}

	restore(httpclient, primaryProxyURL, pcmd, pargs)
}

//=========
//
// Helpers
//
//=========
func hrwProxy(smap *dfc.Smap) (proxyid, proxyurl string, err error) {
	var max uint64

	for id, sinfo := range smap.Pmap {
		cs := xxhash.ChecksumString64S(id, HRWmLCG32)
		if cs > max {
			max = cs
			proxyid = sinfo.DaemonID
			proxyurl = sinfo.DirectURL
		}
	}

	if proxyid == "" {
		err = fmt.Errorf("Smap has no non-skipped proxies: Cannot perform HRW")
	}

	return
}

func kill(httpclient *http.Client, url, port string) (cmd string, args []string, err error) {
	cmd, args, err = getProcessOnPort(port)
	if err != nil {
		err = fmt.Errorf("Error retrieving process on port %v: %v", port, err)
		return
	}

	killurl := url + "/" + dfc.Rversion + "/" + dfc.Rdaemon + "?" + dfc.URLParamForce + "=true"
	msg := &dfc.ActionMsg{Action: dfc.ActShutdown}
	jsbytes, err := json.Marshal(&msg)
	if err != nil {
		err = fmt.Errorf("Unexpected failure to marshal VoteMessage: %v", err)
		return
	}

	req, err := http.NewRequest(http.MethodPut, killurl, bytes.NewBuffer(jsbytes))
	if err != nil {
		err = fmt.Errorf("Unexpected failure to create http request %s %s, err: %v", http.MethodPut, killurl, err)
		return
	}

	r, err := httpclient.Do(req)
	if err != nil {
		err = fmt.Errorf("Error sending HTTP request %v %v: %v", http.MethodGet, killurl, err)
		return
	}
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()
	_, err = dfc.ReadToNull(r.Body)
	if err != nil {
		err = fmt.Errorf("Error reading HTTP Body: %v", err)
		return
	}

	return
}

func restore(httpclient *http.Client, url, cmd string, args []string) error {
	// Restart it
	cmdStart := exec.Command(cmd, args...)
	go func() {
		cmdStart.Run()
	}()

	time.Sleep(5 * time.Second)

	// Send a Election Confirmation message to update the proxy:
	vr := dfc.VoteRecord{
		Candidate: currentPrimaryProxyID,
	}
	msg := dfc.VoteMessage{Record: vr}
	jsbytes, err := json.Marshal(&msg)
	if err != nil {
		return fmt.Errorf("Unexpected failure to marshal VoteMessage: %v", err)
	}

	requrl := fmt.Sprintf("%s/%s/%s/%s", url, dfc.Rversion, dfc.Rvote, dfc.Rvoteres)
	req, err := http.NewRequest(http.MethodPut, requrl, bytes.NewBuffer(jsbytes))

	if err != nil {
		return fmt.Errorf("Unexpected failure to create http request %s %s, err: %v", http.MethodPut, requrl, err)
	}

	r, err := httpclient.Do(req)
	if err != nil {
		return fmt.Errorf("Error sending HTTP Request: %v", err)
	}
	defer func(r *http.Response) {
		if r.Body != nil {
			r.Body.Close()
		}
	}(r)
	_, err = ioutil.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("Error reading HTTP Body: %v", err)
	}

	return nil
}

func getProcessOnPort(port string) (command string, args []string, err error) {
	syscallLSOF := "sudo"
	argsLSOF := []string{"lsof", "-sTCP:LISTEN", "-i", ":8080"}
	commandLSOF := exec.Command(syscallLSOF, argsLSOF...)
	output, err := commandLSOF.CombinedOutput()
	if err != nil {
		err = fmt.Errorf("Error executing LSOF command: %v", err)
		return
	}
	// Find process listening on the port:
	line := strings.Split(string(output), "\n")[1] // The first line will always be output parameters
	fields := strings.Fields(line)
	pid := fields[1] // PID is the second output paremeter

	syscallPS := "ps"
	argsPS := []string{"-p", pid, "-o", "command"}
	commandPS := exec.Command(syscallPS, argsPS...)

	output, err = commandPS.CombinedOutput()
	if err != nil {
		err = fmt.Errorf("Error executing PS command: %v", err)
		return
	}
	line = strings.Split(string(output), "\n")[1] // The first line will always be output parameters
	fields = strings.Fields(line)
	if len(fields) == 0 {
		err = fmt.Errorf("No returned fields")
		return
	}
	return fields[0], fields[1:], nil
}
