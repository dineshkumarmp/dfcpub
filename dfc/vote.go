package dfc

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
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

const (
	ProxyPingTimeout = 100 * time.Millisecond
)

func (t *targetrunner) votehdlr(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	switch apitems[0] {
	case Rtarget:
		t.httptargetvote(w, r)
	case Rproxy:
		t.httpproxyvote(w, r)
	case Rvoteres:
		t.httpsetprimaryproxy(w, r)
	}
}

func (p *proxyrunner) votehdlr(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	switch apitems[0] {
	case Rproxy:
		p.httpproxyvote(w, r)
	case Rvoteres:
		p.httpsetprimaryproxy(w, r)
	case Rvoteinit:
		p.httpRequestNewPrimary(w, r)
	}
}

// GET "/"+Rversion+"/"+Rvote+"/"+votetgt+"?"+ParamSuspectedTarget+"="
func (t *targetrunner) httptargetvote(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	query := r.URL.Query()
	candidate := query.Get(URLParamSuspectedTarget)
	if candidate == "" {
		s := fmt.Sprintf("Cannot request vote without %s query parameter.", URLParamSuspectedTarget)
		t.invalmsghdlr(w, r, s)
		return
	}

	targetinfo, ok := t.smap.Smap[candidate]
	if !ok {
		s := fmt.Sprintf("Suspect not present in target smap: %s", candidate)
		t.invalmsghdlr(w, r, s)
		return
	}

	_ = targetinfo

}

// GET "/"+Rversion+"/"+Rvote+"/"+Rvotepxy+"?"+ParamPrimaryCandidate+"="
func (h *httprunner) httpproxyvote(w http.ResponseWriter, r *http.Request) {
	apitems := h.restAPIItems(r.URL.Path, 5)
	if apitems = h.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	query := r.URL.Query()
	candidate := query.Get(URLParamPrimaryCandidate)
	if candidate == "" {
		s := fmt.Sprintf("Cannot request vote without %s query parameter.", URLParamPrimaryCandidate)
		h.invalmsghdlr(w, r, s)
		return
	}

	proxyinfo, ok := h.smap.Pmap[candidate]
	if !ok {
		s := fmt.Sprintf("Candidate not present in proxy smap: %s", candidate)
		h.invalmsghdlr(w, r, s)
		return
	}

	hrwmax, errstr := hrwProxy(h.smap)
	if errstr != "" {
		s := fmt.Sprintf("Error executing HRW: %v", errstr)
		h.invalmsghdlr(w, r, s)
		return
	}

	if hrwmax.DaemonID == proxyinfo.DaemonID {
		w.Write([]byte(VoteYes)) // FIXME: JSON Struct?
	} else {
		w.Write([]byte(VoteNo))
	}
}

// GET "/"+Rversion+"/"+Rvote+"/"+Rvoteres+"?"+ParamPrimaryCandidate+"="
func (h *httprunner) httpsetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems := h.restAPIItems(r.URL.Path, 5)
	if apitems = h.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	query := r.URL.Query()
	candidate := query.Get(URLParamPrimaryCandidate)
	if candidate == "" {
		s := fmt.Sprintf("Cannot confirm vote result without %s query parameter.", URLParamPrimaryCandidate)
		h.invalmsghdlr(w, r, s)
		return
	}

	proxyinfo, ok := h.smap.Pmap[candidate]
	if !ok {
		s := fmt.Sprintf("Candidate not present in proxy smap: %s", candidate)
		h.invalmsghdlr(w, r, s)
		return
	}

	h.proxysi = proxyinfo
}

// PUT "/"+Rversion+"/"+Rvote+"/"+Rvoteinit
func (p *proxyrunner) httpRequestNewPrimary(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	go p.ProxyElection()
}

func (p *proxyrunner) ProxyElection() error {
	// First, ping current proxy with a short timeout: (Primary? State)

	// FIXME: Different Lock? Finer-grained synchronization?
	p.smap.lock()
	defer p.smap.unlock()

	if p.primary {
		fmt.Println("Already in Primary state.")
		return nil
	}

	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rhealth
	proxyup, err := p.PingWithTimeout(url, ProxyPingTimeout)
	if err != nil {
		proxyup = false
	}
	if proxyup {
		// Move back to Idle state
		fmt.Println("Moving back to Idle state")
		return nil
	}

	fmt.Printf("%v: Primary Proxy %v is down, removing from Smap\n", p.si.DaemonID, p.proxysi.DaemonID)
	p.smap.delProxy(p.proxysi.DaemonID)

	fmt.Println("Moving to Election state")
	// Begin Election State
	elected, err := p.ElectAmongProxies()
	if err != nil {
		return fmt.Errorf("Error requesting Election from other proxies: %v", err)
	}
	if !elected {
		// Move back to Idle state
		fmt.Println("Moving back to Idle state")
		return nil
	}

	fmt.Println("Moving to Election2 State")

	// Begin Election2 State
	err = p.ConfirmElectionVictory()
	if err != nil {
		// Return to Idle state
		// FIXME: Ignore error, become new primary?
		return fmt.Errorf("Error confirming election victory with other proxies: %v", err)
	}

	fmt.Println("Moving to Primary state")
	// Begin Primary State
	p.BecomePrimaryProxy()

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

	if err == context.DeadlineExceeded {
		// Then the proxy is unreachable
		return false, nil
	}

	// The proxy may/may not be down, because we encountered a non-timeout error.
	return false, err
}

func (p *proxyrunner) ElectAmongProxies() (winner bool, err error) {
	// Currently: Simple Majority
	wg := &sync.WaitGroup{}
	chansize := p.smap.count() + p.smap.countProxies() - 1
	resch := make(chan bool, chansize)
	errch := make(chan error, chansize)
	defer close(errch)
	for _, pi := range p.smap.Pmap {
		if pi.DaemonID != p.si.DaemonID {
			// Do not request a vote from this proxy
			wg.Add(1)
			go p.RequestVote(&pi.daemonInfo, wg, resch, errch)
		}
	}
	for _, pi := range p.smap.Smap {
		wg.Add(1)
		go p.RequestVote(pi, wg, resch, errch)
	}
	wg.Wait()
	close(resch)
	select {
	case err = <-errch:
		return false, err
	default:
	}

	y, n := 0, 0
	for res := range resch {
		if res {
			y++
		} else {
			n++
		}
	}

	winner = y > n || (y+n == 0) // No Votes: Default Winner
	fmt.Printf("Y: %v, N:%v, W: %v\n", y, n, winner)
	return
}

func (p *proxyrunner) ConfirmElectionVictory() error {
	wg := &sync.WaitGroup{}
	errch := make(chan error, len(p.smap.Pmap)-1)
	defer close(errch)
	for _, pi := range p.smap.Pmap {
		if pi.DaemonID != p.si.DaemonID && pi.DaemonID != p.proxysi.DaemonID {
			wg.Add(1)
			go p.SendNewPrimaryProxy(&pi.daemonInfo, wg, errch)
		}
	}
	for _, di := range p.smap.Smap {
		wg.Add(1)
		go p.SendNewPrimaryProxy(di, wg, errch)
	}
	wg.Wait()
	select {
	case err := <-errch:
		return err
	default:
	}
	return nil
}

func (p *proxyrunner) RequestVote(si *daemonInfo, wg *sync.WaitGroup, resultch chan bool, errch chan error) {
	defer wg.Done()
	url := fmt.Sprintf("%s/%s/%s/%s?%s=%s", si.DirectURL, Rversion, Rvote, Rproxy, URLParamPrimaryCandidate, p.si.DaemonID)
	r, err := p.httpclient.Get(url)
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
	//TODO proper json structure?
	resultch <- (VoteYes == Vote(respbytes))
}

func (p *proxyrunner) SendNewPrimaryProxy(di *daemonInfo, wg *sync.WaitGroup, errch chan error) {
	defer wg.Done()

	url := fmt.Sprintf("%s/%s/%s/%s?%s=%s", di.DirectURL, Rversion, Rvote, Rvoteres, ParamPrimaryCandidate, p.si.DaemonID)
	r, err := p.httpclient.Get(url)
	if err != nil {
		e := fmt.Errorf("Error requesting vote from %s(%s): %v", di.DaemonID, di.DirectURL, err)
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

func (p *proxyrunner) BecomePrimaryProxy() {
	p.primary = true
	p.smap.getProxy(p.si.DaemonID).Primary = true
	p.smap.ProxySI = p.smap.getProxy(p.si.DaemonID)
}

func (h *httprunner) onPrimaryProxyFailure() {
	fmt.Println("Primary Proxy Failed")
	// The old primary proxy is unreachable, so it is removed from our local Smap. If this is a false positive, it will be updated by the next smap synchronization.
	h.smap.delProxy(h.proxysi.DaemonID)

	nextPrimaryProxy, errstr := hrwProxy(h.smap)
	if errstr != "" {
		glog.Errorf("Failed to execute hrwProxy after Primary Proxy Failure: %v", errstr)
	}
	//h.proxysi = nextPrimaryProxy
	// FIXME: Should the next primary proxy be set immediately?

	url := nextPrimaryProxy.DirectURL + "/" + Rversion + "/" + Rvote + "/" + Rvoteinit
	r, err := h.httpclient.Get(url)
	if err != nil {
		glog.Errorf("Failed to request election from next Primary Proxy: %v", err)
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
