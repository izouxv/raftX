package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"path/filepath"
	"sync"
	"github.com/golang/protobuf/proto"
	"time"
	"strings"
	"fish.red/zouxu/goall/raftx/server/command"
	."fish.red/zouxu/goall/raftx/common/protobuf"
	."fish.red/zouxu/goall/raftx/util"
	."fish.red/zouxu/goall/raftx/common/request"
	"fish.red/zouxu/goall/raftx/server/db"

)

const (
	kUserDir        = "/user/"
	kLeaderIp       = "/root/leader"
	KRootDir        = "/root"
	kFollowIpPat    = "/root/follow/*"
	kSnapshotItems  = 1000000
	kSessionTimeOut = time.Second * 5
)

const (
	kEventJoinLeader       = "joinLeader"
	kEventLeaderChanged    = "leaderChange"
	kEventUpdataLibClients = "updataLibClients" //leader call this event only
	kEventTcpLibSerJoin    = "tcpLibSerJoin"
	kEventLib              = "tcpLibSerJoin"

)

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name       string
	host       string
	port       int
	path       string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	fs         *db.Fs
	mutex      sync.RWMutex
	//Server        *ServerTcp

	tcpIp                      string
	tcpSer        *TcpSer
	recvQueryReqQ              chan *ServerRequestItem
	recvUpdateReqQ             chan *ServerRequestItem
	sendRespQ                  chan *ServerResponceItem
	snapshotIndex              uint64

	facade *EventDispatcher
}

// Creates a new server.
func New(path string, host string, port int) *Server {
	s := &Server{
		host:   host,
		port:   port,
		path:   path,
		//	fs:     db.NewFs(),
		router: mux.NewRouter(),
		recvQueryReqQ:make(chan *ServerRequestItem, 100000),
		recvUpdateReqQ:make(chan *ServerRequestItem, 100000),
		sendRespQ:make(chan *ServerResponceItem, 100000),
	}
	s.fs = db.NewFs(s.fsNotifyCb)

	s.facade = NewEventDispatcher(s)
	s.facade.AddEventListener(kEventLeaderChanged, s.eventListener)

	log.Printf("filePath:%v", filepath.Join(path, "name"))
	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}
	return s
}

// Returns the connection string.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

// Starts the server.
func (s *Server) ListenAndServe(leader string, tcpIp string) error {
	var err error

	log.Printf("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft", 200*time.Millisecond)
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, NewFsStateMachine(s.fs), s.fs, "")
	if err != nil {
		log.Fatal(err)
	}
	//s.Server.raftServer = s.raftServer

	s.raftServer.LoadSnapshot()
	s.snapshotIndex = s.raftServer.CommitIndex()

	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	leaderChangeFun := func(e raft.Event) {
		server := e.Source().(raft.Server)
		log.Printf("change: [%s] %s %v -> %v\n", server.Name(), e.Type(), e.PrevValue(), e.Value())

		if e.Type() == raft.LeaderChangeEventType {
			go func() {
				s.facade.DispatchEvent(NewEvent(kEventLeaderChanged, server, nil))
				s.facade.DispatchEvent(NewEvent(kEventUpdataLibClients, nil, nil))
			}()
		}
	}

	s.raftServer.AddEventListener(raft.StateChangeEventType, leaderChangeFun)
	s.raftServer.AddEventListener(raft.LeaderChangeEventType, leaderChangeFun)
	s.raftServer.AddEventListener(raft.TermChangeEventType, leaderChangeFun)
	s.raftServer.AddEventListener(raft.AddPeerEventType, leaderChangeFun)
	//s.raftServer.AddEventListener(raft.CommitEventType, leaderChangeFun)
	s.raftServer.AddEventListener(raft.RemovePeerEventType, leaderChangeFun)
	//s.raftServer.AddEventListener(raft.HeartbeatIntervalEventType, leaderChangeFun)


	if leader != "" {
		// Join to leader if specified.

		log.Println("Attempting to join leader:", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Fatal(err)
		}
	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Println("Initializing new cluster")
		join := raft.DefaultJoinCommand{ Name:
		s.raftServer.Name(),
			ConnectionString: s.connectionString()}
		_, err := s.raftServer.Do(&join)
		if err != nil {
			log.Fatal(err)
		}

	}else {
		log.Println("Recovered from log")
	}

	log.Println("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Addr:
		fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}

	s.router.HandleFunc("/db/{key}", s.readHandler).Methods("GET")
	s.router.HandleFunc("/db/{key}", s.writeHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	log.Println("Listening at:", s.connectionString())


	s.startTcpSer(tcpIp)

	GoDone(1, func() {
			s.httpServer.ListenAndServe()
		})
	//	go func() {
	//		s.httpServer.ListenAndServe()
	//	}()
	//	return s.httpServer.ListenAndServe()
	return nil
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////HTTP SECTION/////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:
		s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer

	json.NewEncoder(&b).Encode(command)
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	log.Println("joinHandler ok: %v", req.Body)

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Println("joinHandler ok: %v", req.Body)
}

func (s *Server) readHandler(w http.ResponseWriter, req *http.Request) {
	//vars := mux.Vars(req)
	//value, err := s.fs.Get(vars["key"])

	body, _, err := s.fs.Get("/**", true)
	if err == Err_OK {
		w.Write([]byte(body))
	}else {
		str := fmt.Sprintf("readHandler:[%v %v]", "/**", err)
		w.Write([]byte(str))
		//panic(str)
	}
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value := string(b)

	// Execute the command against the Raft server.
	//_, err = s.raftServer.Do(command.NewSetCommand(vars["key"], value))
	_, err = s.raftServer.Do(command.NewSetCommand(vars["key"], value, "", uint32(Verb_SET), ""))

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////TCP SECTION//////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


// Starts the tcp server.
func (s *Server) startTcpSer(ip string) {
	s.tcpIp = ip
	//	s.tcpSer = NewTcpSer(ip, false, s.serCB, 1, 1000, 1, 1)
	s.tcpSer = NewTcpSer(ip, nil, s.serCB, 100, 1000, 1, 10)
	go s.waitForReq()
}

func (s *Server) serCB(stat ConnStat, content []byte, ser* TcpSer, ele* TcpBase) {

	if stat == Connect {
		//fmt.Printf("serCB Connect: %v\n", ele.ConnIp())
		userId := s.raftServer.Name() + "_" + ele.ConnIp()
		ser.SetConnId(ele.ConnIp(), userId)
	}else if stat == Disconnect {
		go func() {
			//log.Printf("Server serCB Disconnect: %v\n", ele.ConnIp())
			s.raftServer.Do(command.NewRemoveTmpDataCommand(ele.ConnIp()))
			s.fs.DelWatchAllWithIp(ele.ConnIp())
		}()

	}else if stat == Data {
		//defer PoolSetByte(content)
		var req Request
		err := proto.Unmarshal(content, &req)
		if err != nil {
			fmt.Printf("proto.Unmarsha err: %v\n", err)
		}

		//log.Printf("serCB [%v]\n", req.String())
		verb := req.GetVerb()
		if verb&uint32(Verb_GET) > 0 {
			s.recvQueryReqQ<-&ServerRequestItem{
				Req: &req,
				Addr:ele.ConnIp(),
			}
		}else {
			s.recvUpdateReqQ<-&ServerRequestItem{
				Req: &req,
				Addr:ele.ConnIp(),
			}
		}
		PoolSetByte(content)
	}
}

func (s *Server) requestAction(reqItem *ServerRequestItem) {

	req := reqItem.Req
	addr := reqItem.Addr

	value := req.GetValue()
	tag := req.GetTag()
	path := req.GetPath()
	verb := req.GetVerb()


	//log.Printf("serRecv [%v]\n", req.String())

	var resp *Response

	opServerInner := verb&uint32(Verb_SERVER) > 0
	if opServerInner {
		jsonStr := s.serverLevelPro(addr, value)
		if len(jsonStr) > 0 {
			resp = NewRespByReq(req, jsonStr, Err_OK)
		}
	}else {
		opGet := verb&uint32(Verb_GET) > 0
		opWatch := verb&uint32(Verb_WATCH) > 0
		opWatchAll := verb&uint32(Verb_WATCH_ALL) > 0

		opPat := false
		if (opWatch || opWatchAll) || opGet {
			opPat = strings.ContainsAny(path, "*?")
		}

		if opWatch || opWatchAll {
			opCreate := verb&uint32(Verb_CREATE) > 0
			opDel := verb&uint32(Verb_DEL) > 0
			//fmt.Printf("opWatch: [%v]\n", path)
			if opCreate {
				s.fs.SetWatch(path, addr, tag, opWatchAll)
				resp = NewRespByReq(req, value, Err_OK)
			}else if opDel {
				s.fs.DelWatch(path, addr)
				resp = NewRespByReq(req, value, Err_OK)
			}
		}else if opGet {
			value, verB, err := s.fs.Get(path, opPat)
			resp = NewRespByReq(req, value, err)
			verb32 := uint32(verB)
			resp.Verb = &verb32
		}else {
			//处理Raft的数据
			if strings.HasPrefix(path, KRootDir) {
				//log.Printf("WK 1 requestAction %v %v [%v]\n", path, KRootDir, req.String())
				resp = NewRespByReq(req, value, Err_ROOT_IS_READONLY)
			}else {
				//	log.Printf("WK 2 requestAction %v %v [%v]\n", path, KRootDir, req.String())

				//log.Printf("serDoStart: %v\n", tag)
				resp = s.serverDo(req, command.NewSetCommand(path, value, tag, verb, addr))
				//log.Printf("serDoEnd: %v\n", tag)
			}
		}
	}




	if resp == nil {
		resp = NewRespByReq(req, value, Err_UNKNOWN)
	}
	//	log.Printf("serLog: \n%v\n%v\n", req.String(), resp.String())
	s.sendRespQ<-&ServerResponceItem{Addr:addr, Resp:resp}
}

func (s*Server) serverLevelPro(addr string, jsonStr string) (string) {
	//data := Json2Ojb(jsonStr).(map[string]interface{})
	var data map[string]interface{}
	Json2Ojb(jsonStr, &data)
	//log.Printf("\n\n\n\n\n\ndata: %v \n\n", data)
	if data != nil {
		dataBack := make(map[string]string)

		for k, _ := range data {
			switch k{
			case KGetMyName: {
				userId, err := s.tcpSer.ConIdByIp(addr)
				//	log.Printf("\n\n\n\n\n\ndata2: %v  %v   %v \n\n", data, userId, addr)
				if err == nil {
					dataBack[KGetMyName] = userId
				}
			}
			}
		}
		outStr := Obj2Json(dataBack, false)
		//	log.Printf("\naddr: %v,\n inStr: %v\n outStr: %v\n\n", addr, jsonStr, outStr)
		return outStr
	}
	return ""
}

func (s*Server) serverDo(req* Request, command raft.Command) *Response {
	//	s.tryTakeSnapshot()
	if s.raftServer.State() == raft.Leader {

		data, err := s.raftServer.Do(command)
		dataObj, ok := data.(map[string]interface{})
		if err != nil || !ok {
			log.Printf("IncErr %v   %v:%v   %v\n", err, command, data)
		}
		ret := dataObj["ret"]
		retErr := dataObj["err"].(Err)
		//retStr := Obj2Json(ret)
		retStr := ret.(string)
		return NewRespByReq(req, retStr, retErr)
	}else {
		//log.Print("\n\n\nnotLead\n\n\n\n")
		return NewRespByReq(req, "", Err_NOT_LEADER)
	}
}

func (s*Server) sendRespWithIp(addr string, resp* Response) {

	if resp != nil {
		//log.Printf("---111: %v,%v\n", req.GetTag(), verb)
		bytes, err := proto.Marshal(resp)
		if err != nil {
			log.Printf("marshalErr %v \n", err)
			//return nil, errors.New("proto.Marshal(resp):"+err.Error())
		}
		//	fmt.Printf("s.server.SendWithIp: ip: %v, bytes: %v\n", addr, bytes)
		s.tcpSer.SendWithIp(addr, bytes)
	}else {
		log.Printf("notBack: %v\n", addr)
	}
}

func (s *Server) waitForReq() {
	for {
		select {//TODO: 如果Upadat在这里卡住，整个Goroutine都会卡住。可能需要多个GO FOR SELECT来做
		case req := <-s.recvUpdateReqQ: {
			///go s.requestAction(req)
			go s.requestAction(req)
		}
		case req := <-s.recvQueryReqQ: {
			go s.requestAction(req)//get 不需要去raft里搞。所以可以
		}
		case respItem := <-s.sendRespQ: {
			addr := respItem.Addr
			resp := respItem.Resp
			s.sendRespWithIp(addr, resp)
		}
		}
	}
}

func (s *Server) fsNotifyCb(path string, value string, tag string, verb Verb, ip string, watchPath string) {
	resp := NewRespWatch(path, value, tag, watchPath, verb)
	s.sendRespQ<-&ServerResponceItem{Addr:ip, Resp:resp}
}

func (s* Server) tryTakeSnapshot() {
	currentIndex := s.raftServer.CommitIndex()

	if uint64(currentIndex - s.snapshotIndex) > kSnapshotItems {
		log.Printf("currentIndex: %d\n", currentIndex)
		//log.Println("Committed log entries snapshot threshold reached, starting snapshot")
		err := s.raftServer.TakeSnapshot()
		s.snapshotIndex = currentIndex
		if err != nil {
			log.Printf("TakeSnapshot failed: %v\n", err)
		} else {
			log.Printf("TakeSnapshot completed\n")
		}
	}
}

func (s* Server) eventListener(event Event) {

	switch event.Type(){
	case kEventLeaderChanged: {
		server := event.Value().(raft.Server)
		log.Printf("LeaderChangeEventType1: %v\n", server.Name())

		//		_, err := server.Do(command.NewSetCommand(kLeaderIp, s.tcpIp, "", uint32(Verb_SET), ""))
		//		if err != nil {
		//			log.Printf("IncErr %v   %v:%v\n", err, s.tcpIp)
		//		}
		log.Printf("LeaderChangeEventType2: %v\n", server.Name())
	}
	case kEventUpdataLibClients: {

		//		value, err := s.fs.GetWithPat(kFollowIpPat)
		//		if err != nil && err != db.ErrNotExist {
		//			log.Printf("reqGetErr %v : %v\n", kFollowIpPat, err)
		//		}

	}
	case kEventTcpLibSerJoin: {

	}
	case kEventJoinLeader: {

	}
	}
}

//			peers := server.Peers()
//			if peer, ok := peers[server.Name()]; ok {
//				log.Printf("LeaderChangeEventType: %v\n", server.Name())
//				addr := peer.ConnectionString
//			}else {
//				addr := peer.ConnectionString
//			}

