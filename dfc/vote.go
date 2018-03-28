package dfc

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

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

// GET "/"+Rversion+"/"+Rvote+"/"+votetgt+"?"+ParamSuspectedTarget+"="
func (t *targetrunner) httptargetvote(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	query := r.URL.Query()
	candidate := query.Get(ParamSuspectedTarget)
	if candidate == "" {
		s := fmt.Sprintf("Cannot request vote without %s query parameter.", ParamSuspectedTarget)
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
func (p *proxyrunner) httpproxyvote(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	query := r.URL.Query()
	candidate := query.Get(ParamPrimaryCandidate)
	if candidate == "" {
		s := fmt.Sprintf("Cannot request vote without %s query parameter.", ParamPrimaryCandidate)
		p.invalmsghdlr(w, r, s)
		return
	}

	proxyinfo, ok := ctx.smap.Pmap[candidate]
	if !ok {
		s := fmt.Sprintf("Candidate not present in proxy smap: %s", candidate)
		p.invalmsghdlr(w, r, s)
		return
	}

	hrwmax, errstr := hrwProxy(ctx.smap)
	if errstr != "" {
		s := fmt.Sprintf("Error executing HRW: %v", errstr)
		p.invalmsghdlr(w, r, s)
		return
	}

	if hrwmax.DaemonID == proxyinfo.DaemonID {
		w.Write([]byte(VoteYes)) // FIXME: JSON Struct?
	} else {
		w.Write([]byte(VoteNo))
	}
}

// GET "/"+Rversion+"/"+Rvote+"/"+Rvoteres+"?"+ParamPrimaryCandidate+"="
func (p *proxyrunner) httpsetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	//TODO

}

// ?? name tbd
func (p *proxyrunner) ProxyElection() error {
	// First, ping current proxy with a short timeout: (Primary? State)
	//TODO: move Primary ProxyURL to proxyrunner
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rhealth
	proxydown, err := p.PingWithTimeout(url, ProxyPingTimeout)
	if err != nil {
		return fmt.Errorf("Error pinging Primary Proxy: %v", err)
	}
	if !proxydown {
		// Move back to Idle state
		return nil
	}

	// Begin Election State
	elected, err := p.ElectAmongProxies()
	if err != nil {
		return fmt.Errorf("Error requesting Election from other proxies: %v", err)
	}
	if !elected {
		// Move back to Idle state
		return nil
	}

	// Begin Election2 State
	err = p.ConfirmElectionVictory()
	if err != nil {
		// Return to Idle state
		// FIXME: Ignore error, become new primary?
		return fmt.Errorf("Error confirming election victory with other proxies: %v", err)
	}

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
	resch := make(chan bool, len(ctx.smap.Pmap)-1)
	errch := make(chan error, len(ctx.smap.Pmap)-1)
	for _, pi := range ctx.smap.Pmap {
		if pi.DaemonID != p.si.DaemonID {
			go p.RequestVote(pi, wg, resch, errch)
		}
	}
	wg.Wait()
	close(resch)
	close(errch)
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

	winner = y > n
	return
}

func (p *proxyrunner) ConfirmElectionVictory() error {
	wg := &sync.WaitGroup{}
	errch := make(chan error, len(ctx.smap.Pmap)-1)
	for _, pi := range ctx.smap.Pmap {
		if pi.DaemonID != p.si.DaemonID {
			go p.SetNewPrimaryProxy(pi, wg, errch)
		}
	}
	wg.Wait()
	close(errch)
	select {
	case err := <-errch:
		return err
	default:
	}
	return nil
}

func (p *proxyrunner) RequestVote(pi *proxyInfo, wg *sync.WaitGroup, resultch chan bool, errch chan error) {
	defer wg.Done()
	url := fmt.Sprintf("%s/%s/%s?%s=%s", pi.DirectURL, Rvote, Rtarget, ParamPrimaryCandidate, p.si.DaemonID)
	r, err := p.httpclient.Get(url)
	if err != nil {
		errch <- fmt.Errorf("Error requesting vote from %s(%s): %v", pi.DaemonID, pi.DirectURL, err)
		return
	}
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()

	respbytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errch <- fmt.Errorf("Error reading response from %s(%s): %v", pi.DaemonID, pi.DirectURL, err)
		return
	}
	//TODO proper json structure?
	resultch <- (VoteYes == Vote(respbytes))
}

func (p *proxyrunner) SetNewPrimaryProxy(pi *proxyInfo, wg *sync.WaitGroup, errch chan error) {
	defer wg.Done()
	//TODO
}

func (p *proxyrunner) BecomePrimaryProxy() {
	//TODO
}
