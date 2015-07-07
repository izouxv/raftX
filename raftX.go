package main

import (
    "flag"
    "fmt"
    "github.com/goraft/raft"
    "github.com/izouxv/raftX/server"
    "github.com/izouxv/raftX/server/command"
    "log"
    "math/rand"
    "os"
    "time"
    "runtime"

    "net/http"
    _ "net/http/pprof"
)

//raftx -p 4001 ~/node.1
//go get -u

var verbose bool
var trace bool
var debug bool
var host string
var port int
var tcpSerPort int
var join string

func init() {
    flag.BoolVar(&verbose, "vl", false, "verbose logging")
    flag.BoolVar(&trace, "trace", false, "Raft trace debugging")
    flag.BoolVar(&debug, "debug", false, "Raft debugging")
    flag.StringVar(&host, "h", "127.0.0.1", "hostname")
    flag.IntVar(&port, "p", 4001, "port")
    flag.IntVar(&tcpSerPort, "tsp", 9999, "tcpSerPort")
    flag.StringVar(&join, "join", "", "host:port of leader to join")
    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
        flag.PrintDefaults()
    }
}

func main() {
    go func() {
        //http://localhost:6060/debug/pprof/
        log.Println(http.ListenAndServe("localhost:6060", nil))
    }()

    log.SetFlags(0)
    flag.Parse()
    if verbose {
        log.Print("Verbose logging enabled.")
    }
    if trace {
        raft.SetLogLevel(raft.Trace)
        log.Print("Raft trace debugging enabled.")
    } else if debug {
        raft.SetLogLevel(raft.Debug)
        log.Print("Raft debugging enabled.")
    }

    rand.Seed(time.Now().UnixNano())

    // Setup commands.
    raft.RegisterCommand(&command.SetCommand{})
    raft.RegisterCommand(&command.RemoveTmpDataCommand{})

    // Set the data directory.
    if flag.NArg() == 0 {
        flag.Usage()
        log.Fatal("Data path argument required")
    }
    path := flag.Arg(0)
    if err := os.MkdirAll(path, 0744); err != nil {
        log.Fatalf("Unable to create path: %v", err)
    }

    //	log.Fatalf("join: %v", join)
    //	log.Fatalf(" path: %v", path)

    log.SetFlags(log.LstdFlags)
    s := server.New(path, host, port)

    if true {
        runtime.GOMAXPROCS(runtime.NumCPU())
    }
    addr := fmt.Sprintf("%s:%d", host, tcpSerPort)

    //log.Fatal(s.ListenAndServe(join, addr))
    s.ListenAndServe(join, addr)
    select {}
}
