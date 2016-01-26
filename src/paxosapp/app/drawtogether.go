package main

import (
	"net/http"
	"net/rpc"
	"net/url"
	"html/template"
	"fmt"
	"strconv"
	//"sync"
	"encoding/json"
	"flag"
	"github.com/cmu440-F15/paxosapp/rpc/paxosrpc"
)

var (
	client *rpc.Client
	paxosport = flag.String("paxosport", "", "port for one paxos node")
	myport = flag.String("port", "8080", "port for server")
	templates = template.Must(template.ParseFiles("index.html"))
)

const (
	rows = 75//150
	cols = 60//120
	white = "FFFFFF"
)

func renderTemplate(w http.ResponseWriter, tmpl string) {
	err := templates.ExecuteTemplate(w, tmpl+".html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func defaultHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	query := r.URL.RawQuery
	if (path == "/" || path == "/index.html") {
		renderTemplate(w, "index")
	} else if(path == "/clear") {
	//	lock.Lock()
	//	defer lock.Unlock()
		for r := 0; r < rows; r++ {
			for c := 0; c < cols; c++ {
				propose(strconv.Itoa(r*cols+c), white)
				//pixels[strconv.Itoa(r*cols+c)] = white
			}
		}
	} else if (path == "/getpixels") {
		//lock.RLock()
		//defer lock.RUnlock()
		var colors = make([]string, rows*cols)
		var pixels map[string] interface{}
		json.Unmarshal(getValues(), &pixels)
		for r := 0; r < rows; r++ {
			for c := 0; c < cols; c++ {
				colors[r*cols + c] = pixels[strconv.Itoa(r*cols+c)].(string)
			}
		}
		response,_ := json.Marshal(colors)
		fmt.Fprintf(w, "%s", response)
	} else if (path == "/setpixel") {
		m, err := url.ParseQuery(query)
		if (err == nil) {
			key := m["key"][0]
			color := m["color"][0]
			propose(key, color)
		}
	}
}

func propose(key string, value string) {
	for {
		args := paxosrpc.ProposalNumberArgs{key}
		var reply paxosrpc.ProposalNumberReply

		err := client.Call("PaxosNode.GetNextProposalNumber", args, &reply)
		if err != nil {
			fmt.Println("Call")
			continue
		}

		proposeArgs := paxosrpc.ProposeArgs{reply.N, key, value}
		var proposeReply paxosrpc.ProposeReply
		err = client.Call("PaxosNode.Propose", proposeArgs, &proposeReply)
		if err !=nil {
			fmt.Println("Call")
			continue
		}
		break;
	}
}

func getValue(key string) string {
	for {
		args := paxosrpc.GetValueArgs{key}
		var reply paxosrpc.GetValueReply
		err := client.Call("PaxosNode.GetValue", args, &reply)
		if err == nil {
			if reply.Status == paxosrpc.KeyNotFound {
				return white
			} else {
				return reply.V.(string)
			}
		}
	}

}

func getValues() []byte {
	for {
		args := paxosrpc.GetMapArgs{}
		var reply paxosrpc.GetMapReply
		err := client.Call("PaxosNode.GetMap", args, &reply)
		if err == nil {
			return reply.Data
		}
	}

}


func main() {
	flag.Parse()
	var err error
	for {
		client, err = rpc.DialHTTP("tcp", "localhost:"+*paxosport)
		if err == nil {
			break
		}
	}
	fmt.Println("Server started. Paxos port="+*paxosport)
	for r := 0; r < rows; r++ {
		for c := 0; c < cols; c++ {
			propose(strconv.Itoa(r*cols+c), white)
		}
	}
	fmt.Println("Initialize panel successfully.")
	fmt.Println("Listen on port: " + *myport)
	http.HandleFunc("/", defaultHandler)
	http.Handle("/css/", http.StripPrefix("/css/", http.FileServer(http.Dir("css"))))
	http.Handle("/font/", http.StripPrefix("/font/", http.FileServer(http.Dir("font"))))
	http.Handle("/fonts/", http.StripPrefix("/fonts/", http.FileServer(http.Dir("fonts"))))
	http.Handle("/js/", http.StripPrefix("/js/", http.FileServer(http.Dir("js"))))
	http.ListenAndServe(":"+*myport, nil)
}
