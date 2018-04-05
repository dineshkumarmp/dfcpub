package dfc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context/ctxhttp"
)

type Vote string

const (
	VoteYes Vote = "YES"
	VoteNo  Vote = "NO"
)

// xaction constant for Election
const ActElection = "election"

const (
	ProxyPingTimeout = 100 * time.Millisecond
)

//==========
//
// Messages
//
//==========

type VoteMessage struct {
	Record VoteRecord `json:"voterecord"`
}

type VoteRecord struct {
	Candidate   string    `json:"candidate"`
	Primary     string    `json:"primary"`
	SmapVersion int64     `json:"smapversion"`
	StartTime   time.Time `json:"starttime"`
}

//==========
//
// Handlers
//
//==========

// "/"+Rversion+"/"+Rvote+"/"
func (t *targetrunner) votehdlr(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	switch r.Method {
	case http.MethodGet:
		t.httpproxyvote(w, r)
	case http.MethodPut:
		t.httpsetprimaryproxy(w, r)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		t.invalmsghdlr(w, r, s)
	}
}

// "/"+Rversion+"/"+Rvote+"/"
func (p *proxyrunner) votehdlr(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	switch r.Method {
	case http.MethodGet:
		p.httpproxyvote(w, r)
	case http.MethodPut:
		if apitems[0] == Rvoteres {
			p.primary = false
			p.httpsetprimaryproxy(w, r)
			return
		} else if apitems[0] == Rvoteinit {
			p.httpRequestNewPrimary(w, r)
			return
		}
		fallthrough
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		p.invalmsghdlr(w, r, s)
	}
}

// GET "/"+Rversion+"/"+Rvote+"/"+Rvotepxy
func (h *httprunner) httpproxyvote(w http.ResponseWriter, r *http.Request) {
	apitems := h.restAPIItems(r.URL.Path, 5)
	if apitems = h.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	msg := VoteMessage{}
	err := h.readJSON(w, r, &msg)
	if err != nil {
		s := fmt.Sprintf("Error reading Vote Request body: %v", err)
		h.invalmsghdlr(w, r, s)
		return
	}

	v := h.smap.versionLocked()
	if v != msg.Record.SmapVersion {
		glog.Errorf("Invalid Smap version in VoteMessage: %v, should be %v\n", msg.Record.SmapVersion, v)
		w.Write([]byte(VoteNo))
		return
	}

	candidate := msg.Record.Candidate
	if candidate == "" {
		s := fmt.Sprintln("Cannot request vote without Candidate field")
		h.invalmsghdlr(w, r, s)
		return
	}
	vote, err := h.VoteOnProxy(candidate)
	if err != nil {
		h.invalmsghdlr(w, r, err.Error())
	}

	if vote {
		w.Write([]byte(VoteYes))
	} else {
		w.Write([]byte(VoteNo))
	}
}

// PUT "/"+Rversion+"/"+Rvote+"/"+Rvoteres
func (h *httprunner) httpsetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems := h.restAPIItems(r.URL.Path, 5)
	if apitems = h.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	msg := VoteMessage{}
	err := h.readJSON(w, r, &msg)
	if err != nil {
		s := fmt.Sprintf("Error reading Vote Message body: %v", err)
		h.invalmsghdlr(w, r, s)
		return
	}

	vr := msg.Record

	glog.Infof("%v recieved vote result: %v\n", h.si.DaemonID, vr)

	h.smap.lock()
	defer h.smap.unlock()

	proxyinfo, ok := h.smap.Pmap[vr.Candidate]
	if !ok {
		s := fmt.Sprintf("Candidate not present in proxy smap: %s", vr.Candidate)
		h.invalmsghdlr(w, r, s)
		return
	}

	proxyinfo.Primary = true
	h.proxysi = proxyinfo
	h.smap.delProxy(vr.Primary)
	h.smap.ProxySI = proxyinfo
	ctx.config.Proxy.ID = proxyinfo.DaemonID
	ctx.config.Proxy.URL = proxyinfo.DirectURL
	err = h.writeConfigFile()
	if err != nil {
		glog.Errorf("Error writinc config file: %v", err)
	}
}

// PUT "/"+Rversion+"/"+Rvote+"/"+Rvoteinit
func (p *proxyrunner) httpRequestNewPrimary(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	msg := VoteMessage{}
	err := p.readJSON(w, r, &msg)
	if err != nil {
		s := fmt.Sprintf("Error reading Vote Request body: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	go p.ProxyElection(msg.Record)
}

//===================
//
// Election Functions
//
//===================

func (p *proxyrunner) ProxyElection(vr VoteRecord) error {

	xele := p.xactinp.renewElection(p, vr)
	if xele == nil {
		glog.Infoln("An election is already in progress, returning.")
		return nil
	}

	defer func() {
		xele.etime = time.Now()
		glog.Infoln(xele.tostring())
		p.xactinp.del(xele.id)
	}()

	if p.primary {
		glog.Infoln("Already in Primary state.")
		return nil
	}

	// First, ping current proxy with a short timeout: (Primary? State)
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rhealth
	proxyup, err := p.PingWithTimeout(url, ProxyPingTimeout)

	if proxyup {
		// Move back to Idle state
		glog.Infoln("Moving back to Idle state")
		return nil
	}

	glog.Infof("%v: Primary Proxy %v is confirmed down\n", p.si.DaemonID, p.proxysi.DaemonID)
	glog.Infoln("Moving to Election state")
	// Begin Election State
	elected, err := p.ElectAmongProxies(vr)
	if err != nil {
		return fmt.Errorf("Error requesting Election from other proxies/targets: %v", err)
	}
	if !elected {
		// Move back to Idle state
		glog.Infoln("Moving back to Idle state")
		return nil
	}

	glog.Infoln("Moving to Election2 State")

	// Begin Election2 State
	err = p.ConfirmElectionVictory(vr)
	if err != nil {
		// Return to Idle state
		// FIXME: Ignore error, become new primary?
		return fmt.Errorf("Error confirming election victory with other proxies/targets: %v", err)
	}

	glog.Infoln("Moving to Primary state")
	// Begin Primary State
	p.BecomePrimaryProxy(vr)

	return nil
}

func (p *proxyrunner) PingWithTimeout(url string, timeout time.Duration) (bool, error) {
	timeoutctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := ctxhttp.Get(timeoutctx, p.httpclient, url)
	if err == nil {
		// There is no issue with the current Primary Proxy
		return true, nil
	}
	if err == context.DeadlineExceeded || IsErrConnectionRefused(err) {
		// Then the proxy is unreachable
		return false, nil
	}
	return false, err
}

func (p *proxyrunner) ElectAmongProxies(vr VoteRecord) (winner bool, err error) {
	// Simple Majority Vote

	wg := &sync.WaitGroup{}
	resch, errch := p.RequestVotes(vr, wg)
	wg.Wait()
	close(resch)
	close(errch)
	y, n := 0, 0
	for err := range errch {
		if err != nil {
			glog.Warningf("Error response from target: %v", err)
			n++
		}
	}

	for res := range resch {
		if res {
			y++
		} else {
			n++
		}
	}

	winner = y > n || (y+n == 0) // No Votes: Default Winner
	glog.Infof("Y: %v, N:%v, W: %v\n", y, n, winner)
	return
}

func (p *proxyrunner) ConfirmElectionVictory(vr VoteRecord) error {
	wg := &sync.WaitGroup{}
	errch := p.BroadcastElectionVictory(vr, wg)
	wg.Wait()
	close(errch)
	for err := range errch {
		if err != nil {
			glog.Warningf("Error broadcasting election victory: %v", err)
		}
	}
	return nil
}

func (p *proxyrunner) RequestVotes(vr VoteRecord, wg *sync.WaitGroup) (chan bool, chan error) {
	p.smap.lock()
	defer p.smap.unlock()
	chansize := p.smap.count() + p.smap.countProxies() - 1
	resch := make(chan bool, chansize)
	errch := make(chan error, chansize)
	for _, pi := range p.smap.Pmap {
		if pi.DaemonID != p.si.DaemonID && pi.DaemonID != vr.Primary {
			// Do not request a vote from this proxy, or the previous (down) primary proxy.
			wg.Add(1)
			go p.RequestVote(vr, &pi.daemonInfo, wg, resch, errch)
		}
	}
	for _, pi := range p.smap.Smap {
		wg.Add(1)
		go p.RequestVote(vr, pi, wg, resch, errch)
	}

	return resch, errch
}

func (p *proxyrunner) BroadcastElectionVictory(vr VoteRecord, wg *sync.WaitGroup) chan error {
	p.smap.lock()
	defer p.smap.unlock()
	errch := make(chan error, p.smap.count()+p.smap.countProxies()-1)
	for _, pi := range p.smap.Pmap {
		if pi.DaemonID != vr.Candidate && pi.DaemonID != vr.Primary {
			wg.Add(1)
			go p.SendNewPrimaryProxy(vr, &pi.daemonInfo, wg, errch)
		}
	}
	for _, di := range p.smap.Smap {
		wg.Add(1)
		go p.SendNewPrimaryProxy(vr, di, wg, errch)
	}
	return errch
}

func (p *proxyrunner) RequestVote(vr VoteRecord, si *daemonInfo, wg *sync.WaitGroup, resultch chan bool, errch chan error) {
	defer wg.Done()
	url := fmt.Sprintf("%s/%s/%s/%s?%s=%s", si.DirectURL, Rversion, Rvote, Rproxy, URLParamPrimaryCandidate, p.si.DaemonID)

	msg := VoteMessage{Record: vr}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)

	req, err := http.NewRequest(http.MethodGet, url, bytes.NewBuffer(jsbytes))
	if err != nil {
		e := fmt.Errorf("Unexpected failure to create http request %s %s, err: %v", http.MethodGet, url, err)
		errch <- e
		return
	}

	r, err := p.httpclient.Do(req)
	if err != nil {
		e := fmt.Errorf("Error requesting vote from %s(%s): %v", si.DaemonID, si.DirectURL, err)
		errch <- e
		return
	}
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()

	respbytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		e := fmt.Errorf("Error reading response from %s(%s): %v", si.DaemonID, si.DirectURL, err)
		errch <- e
		return
	}

	resultch <- (VoteYes == Vote(respbytes))
}

func (p *proxyrunner) SendNewPrimaryProxy(vr VoteRecord, di *daemonInfo, wg *sync.WaitGroup, errch chan error) {
	defer wg.Done()

	msg := VoteMessage{Record: vr}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)

	url := fmt.Sprintf("%s/%s/%s/%s", di.DirectURL, Rversion, Rvote, Rvoteres)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsbytes))
	if err != nil {
		e := fmt.Errorf("Unexpected failure to create http request %s %s, err: %v", http.MethodPut, url, err)
		errch <- e
		return
	}

	r, err := p.httpclient.Do(req)
	if err != nil {
		e := fmt.Errorf("Error committing result for %s(%s): %v", di.DaemonID, di.DirectURL, err)
		errch <- e
		return
	}
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()

	// Discard Body
	_, err = ioutil.ReadAll(r.Body)
	if err != nil {
		e := fmt.Errorf("Error reading response from %s(%s): %v", di.DaemonID, di.DirectURL, err)
		errch <- e
		return
	}

}

func (p *proxyrunner) BecomePrimaryProxy(vr VoteRecord) {
	p.smap.lock()
	defer p.smap.unlock()

	p.smap.delProxy(vr.Primary)
	p.primary = true
	psi := p.smap.getProxy(p.si.DaemonID)
	psi.Primary = true
	p.proxysi = psi
	p.smap.ProxySI = psi
	ctx.config.Proxy.ID = psi.DaemonID
	ctx.config.Proxy.URL = psi.DirectURL
	err := p.writeConfigFile()
	if err != nil {
		glog.Errorf("Error writinc config file: %v", err)
	}
	go p.synchronizeMaps(0, "")
}

func (p *proxyrunner) onPrimaryProxyFailure() {
	glog.Infof("%v: Primary Proxy (%v @ %v) Failed\n", p.si.DaemonID, p.proxysi.DaemonID, p.proxysi.DirectURL)

	nextPrimaryProxy, errstr := hrwProxy(p.smap, p.proxysi.DaemonID)
	if errstr != "" {
		glog.Errorf("Failed to execute hrwProxy after Primary Proxy Failure: %v", errstr)
	}

	vr := VoteRecord{
		Candidate:   nextPrimaryProxy.DaemonID,
		Primary:     p.proxysi.DaemonID,
		SmapVersion: p.smap.versionLocked(),
		StartTime:   time.Now(),
	}
	if nextPrimaryProxy.DaemonID == p.si.DaemonID {
		// If this proxy is the next primary proxy candidate, it starts the election directly.
		go p.ProxyElection(vr)
	} else {
		p.sendElectionRequest(vr, nextPrimaryProxy)
	}
}

func (t *targetrunner) onPrimaryProxyFailure() {
	glog.Infof("%v: Primary Proxy (%v @ %v) Failed\n", t.si.DaemonID, t.proxysi.DaemonID, t.proxysi.DirectURL)

	nextPrimaryProxy, errstr := hrwProxy(t.smap, t.proxysi.DaemonID)
	if errstr != "" {
		glog.Errorf("Failed to execute hrwProxy after Primary Proxy Failure: %v", errstr)
	}

	if nextPrimaryProxy == nil {
		// There is only one proxy, so we cannot select a next in line
		glog.Warningf("Primary Proxy failed, but there are no candidates to fall back on.")
		return
	}

	vr := VoteRecord{
		Candidate:   nextPrimaryProxy.DaemonID,
		Primary:     t.proxysi.DaemonID,
		SmapVersion: t.smap.versionLocked(),
		StartTime:   time.Now(),
	}
	t.sendElectionRequest(vr, nextPrimaryProxy)
}

func (h *httprunner) sendElectionRequest(vr VoteRecord, nextPrimaryProxy *proxyInfo) {
	url := nextPrimaryProxy.DirectURL + "/" + Rversion + "/" + Rvote + "/" + Rvoteinit
	msg := VoteMessage{Record: vr}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)

	req, err := http.NewRequest(http.MethodGet, url, bytes.NewBuffer(jsbytes))
	if err != nil {
		glog.Errorf("Unexpected failure to create http request %s %s, err: %v", http.MethodGet, url, err)
		return
	}

	r, err := h.httpclient.Do(req)
	if err != nil {
		glog.Errorf("Failed to request election from next Primary Proxy: %v", err)
		return
	}
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()

	_, err = ioutil.ReadAll(r.Body)
	if err != nil {
		glog.Errorf("Failed to request election from next Primary Proxy: %v", err)
	}
}

func (h *httprunner) VoteOnProxy(candidate string) (bool, error) {
	proxyinfo, ok := h.GetProxyLocked(candidate)
	if !ok {
		return false, fmt.Errorf("Candidate not present in proxy smap: %s (%v)", candidate, h.smap.Pmap)
	}

	// First: Check last keepalive timestamp. If the proxy was recently successfully reached,
	// this will always vote no, as we believe the original proxy is still alive.
	lastKeepaliveTime := h.kalive.getTimestamp(h.proxysi.DaemonID)
	timeSinceLastKalive := time.Since(lastKeepaliveTime)
	if timeSinceLastKalive < ctx.config.KeepAliveTime/2 {
		// KeepAliveTime/2 is the expected amount time since the last keepalive was sent
		return false, nil
	}

	// Second: Vote according to whether or not the candidate is the Highest Random Weight remaining
	// in the Smap
	hrwmax, errstr := hrwProxy(h.smap, h.proxysi.DaemonID)
	if errstr != "" {
		return false, fmt.Errorf("Error executing HRW: %v", errstr)
	}

	return hrwmax.DaemonID == proxyinfo.DaemonID, nil
}

//==================
//
// Helper Functions
//
//==================

func (h *httprunner) GetProxyLocked(candidate string) (*proxyInfo, bool) {
	h.smap.lock()
	defer h.smap.unlock()
	proxyinfo := h.smap.getProxy(candidate)
	return proxyinfo, (proxyinfo != nil)
}

func (h *httprunner) writeConfigFile() error {
	jsbytes, err := json.Marshal(ctx.config)
	assert(err == nil)

	conffile := clivars.conffile
	if err := ioutil.WriteFile(conffile, jsbytes, os.ModePerm); err != nil {
		return fmt.Errorf("Error writing Config File: %v", err)
	}

	return nil

}
