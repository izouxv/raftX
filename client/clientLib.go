package client

import (
    "code.google.com/p/gogoprotobuf/proto"
    "log"
    "sync"
    "time"
    . "fish.red/zouxu/goall/raftx/util"
    . "fish.red/zouxu/goall/raftx/common/protobuf"
    . "fish.red/zouxu/goall/raftx/common/request"
    "strconv"
    "fmt"
    "strings"
)

//watch verb is Verb_Create | Verb_Set | Verb_Del
type WatchNotify func(path string, value string, verb Verb, stopWatch*bool)

type MsgSendType int

const (
    kMsgSendToAll = iota
    kMsgSendWithIp
    kMsgSendWithHash
)

func Path(dir string, name string) string {
    return fmt.Sprintf("%v/%v", dir, name)
}

type Client struct {
    rwLock *sync.RWMutex
    clients    *TcpCli

    msgItems map[string]*ClientRequestItem

    watchCbItems map[string]WatchNotify //{path:WatchNotify}
    watchChan    chan *Response

    leaderAddr   string
    name         string
    Tag          int

}

// Creates a new server.
func New(addrArray []string) *Client {

    c := &Client{
        rwLock:&sync.RWMutex{},
        msgItems:make(map[string]*ClientRequestItem),
        watchCbItems:make(map[string]WatchNotify),
        watchChan:make(chan *Response, 1000),
    }

    //	log.Printf("Initializing Tcp Client, ips: %v", addrArray)
    c.clients = NewTcpCli(c.cliTcpCB, true, 1, 3, 10)

    leaderAddr := addrArray[0]

    c.leaderAddr = leaderAddr

    for i := 0; i < len(addrArray); i++ {
        addr := addrArray[i]
        err := c.clients.Connect(addr)
        if err!=nil {
            c.reconnect(addr)
            log.Printf("raftSer: %v can't conntent: %v", addr, err)
        }
    }

    // GoDone(1,c.waitForReq)
    go c.waitForReq()

    return c
}

func (c* Client) reconnect(ip string) {
    return
    log.Printf("reconnect: %v\n", ip)
    go func() {
        maxSecond := 60*30
        cur := 0
        go func() {
            for {
                time.Sleep(time.Second*1)
                cur++
                if cur>maxSecond {
                    return
                }
                if err := c.clients.Connect(ip); err==nil {
                    return
                }
            }
        }()
    }()
}

func (c* Client) Close() {
    c.rwLock.Lock()
    defer c.rwLock.Unlock()

    c.watchChan <- nil
    close(c.watchChan)

    c.clients.Close()
    c.clients = nil


    for k, _ := range c.msgItems {
        delete(c.msgItems, k)
    }

    for k, _ := range c.watchCbItems {
        delete(c.watchCbItems, k)
    }
}

func (c *Client) Name() string {
    return c.name
}

// Get the server list.
func (c *Client) initSerData() {
    getMyName := map[string]interface{}{KGetMyName:""}
    back, err := c._sendServerLevelPro(getMyName)

    if err == Err_OK {
        c.name = back[KGetMyName].(string)
      //  log.Printf("raftName: %v", c.name)
    }else {
        log.Printf("raftCliInitSerDataErr: %v", err)
    }
    //	log.Printf("initSerData: %v %v\n", back, c.name)
}

func (c *Client) _sendServerLevelPro(data map[string]interface{}) (map[string]interface{}, Err) {
    outStr := Obj2Json(data, false)
    req := NewReq("server", outStr, uint32(Verb_SERVER))
    resp := c.sendMsg(req, kMsgSendToAll, c.leaderAddr)//KMsgSendToAll
    if resp.GetErrCode() == Err_OK {
        value := resp.GetValue()
        var obj map[string]interface{}
        Json2Ojb(value, &obj)//.(map[string]interface{})
        return obj, resp.GetErrCode()
    }
    return nil, resp.GetErrCode()
}

// Get the server list.
func (c *Client) Start() {
    c.initSerData()
    cb := func(key string, value string, verb Verb, stopWatch*bool) {

    }
    c.GetAndWatch(KSerListPath, true, cb)
}

func (c *Client) Get(path string) (*Response) {
    verb := Verb_GET
    resp := c.requestAndGetRet(path, "", verb, kMsgSendWithHash, path, nil)
    return resp
}

func (c *Client) GetList(path string) (*Response) {
    return c.Get(path)
}

func (c *Client) GetAndWatch(path string, immediately bool, notify WatchNotify) {
    resp := c.Get(path)
    opPat := strings.ContainsAny(path, "*?")
    stopWatch := false
    if resp.GetErrCode() == Err_OK {
        if opPat {
            jsonStr := resp.GetValue()
            if len(jsonStr) > 0 {
                //log.Printf("jsonStr: %v\n", resp)
                //return
                var obj map[string]interface{}
                Json2Ojb(jsonStr, &obj)//.(map[string]interface{})

                for dirPath, dirInter := range obj {
                    dir := dirInter.(map[string]interface{})
                    //		log.Printf("dir: %v\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n", dir)
                    for name, fileAttInter := range dir {
                        fileAtt := fileAttInter.(map[string]interface{})
                        fileName := dirPath + name//fileAtt["name"].(string)
                        fileValue := fileAtt["value"].(string)
                        notify(fileName, fileValue, Verb(resp.GetVerb()), &stopWatch)
                        if stopWatch {
                            return
                        }
                    }
                }
            }
        }else {
            notify(resp.GetPath(), resp.GetValue(), Verb(resp.GetVerb()), &stopWatch)
        }
    }
    if !stopWatch {
        c.Watch(path, immediately, notify)
    }
}

func (c *Client) MkFile(path string, value string) (*Response) {
    //	verb := Verb_CREATE | Verb_PERSISTENT
    //	return c.requestAndGetRet(path, value, verb, kMsgSendWithIp, c.leaderAddr, nil)
    return c.MkFileWithVerb(path, value, Verb_PERSISTENT)
}

func (c *Client) MkFileWithVerb(path string, value string, v Verb) (*Response) {
    verb := Verb_CREATE | v
    return c.requestAndGetRet(path, value, verb, kMsgSendWithIp, c.leaderAddr, nil)
}

func (c *Client) MkFileTmp(path string, value string) (*Response) {
    verb := Verb_CREATE | Verb_EPHEMERAL
    return c.requestAndGetRet(path, value, verb, kMsgSendWithIp, c.leaderAddr, nil)
}

func (c *Client) MkDir(path string, value string) (*Response) {
    verb := Verb_CREATE | Verb_DIR
    return c.requestAndGetRet(path, value, verb, kMsgSendWithIp, c.leaderAddr, nil)
}

func (c *Client) CreateInt(path string, value int) (*Response) {
    verb := Verb_CREATE | Verb_NUMBER
    return c.requestAndGetRet(path, strconv.Itoa(value), verb, kMsgSendWithIp, c.leaderAddr, nil)
}

func (c *Client) Set(path string, value string) (*Response) {
    return c.set(path, value, Verb_SET)
}

func (c *Client) Add(path string, num int) (*Response) {
    return c.set(path, strconv.Itoa(num), Verb_NUMBER)
}

//Set的msgItem需要设置最新的leaderip来发送。
func (c *Client) set(path string, value string, verb2 Verb) (*Response) {
    verb := verb2 | Verb_SET
    return c.requestAndGetRet(path, value, verb, kMsgSendWithIp, c.leaderAddr, nil)
}

func (c *Client) Inc(path string) (*Response) {
    return c.Add(path, 1)
}

func (c *Client) Lock(path string) {
    resp := c.MkFileTmp(path, c.Name())
    if resp.GetErrCode() != Err_OK {
        wait := make(chan bool)
        watchCB := func(key string, value string, verb Verb, stopWatch*bool) {
            if verb&Verb_DEL > 0 {
                resp := c.MkFileTmp(path, c.Name())
                if resp.GetErrCode() == Err_OK {
                    c.RemoveWatch(path)
                    wait <- true
                }
            }
        }
        c.Watch(path, true, watchCB)
        <-wait
    }
}

func (c *Client) ULock(path string) {
    c.Del(path)
}

func (c *Client) Del(path string) (*Response) {
    verb := Verb_DEL
    return c.requestAndGetRet(path, "", verb, kMsgSendWithIp, c.leaderAddr, nil)
}

func (c *Client) RemoveWatch(path string) (*Response) {
    verb := Verb_DEL|Verb_WATCH_ALL | Verb_WATCH
    resp := c.requestAndGetRet(path, "", verb, kMsgSendWithIp, c.leaderAddr, nil)

    if resp.GetErrCode() == Err_OK {
        c.rwLock.Lock()
        defer c.rwLock.Unlock()
        delete(c.watchCbItems, path)
    }
    return resp
}

func (c *Client) Watch(path string, immediately bool, notify WatchNotify) (*Response) {
    verb := Verb_CREATE
    if immediately {
        verb = verb|Verb_WATCH_ALL
    }else {
        verb = verb|Verb_WATCH
    }

    resp := c.requestAndGetRet(path, "", verb, kMsgSendWithHash, path, notify)

    if resp.GetErrCode() == Err_OK {
        c.rwLock.Lock()
        defer c.rwLock.Unlock()
        c.watchCbItems[path] = notify
    }
    return resp
}

func (c *Client) requestAndGetRet(path string, value string, verb Verb, sendType MsgSendType, addr string, notify WatchNotify) (*Response) {
    req := NewReq(path, value, uint32(verb))
    return c.sendMsg(req, sendType, addr)
}

func (c *Client) sendMsg(req *Request, sendType MsgSendType, addr string) (*Response) {

    bytes, err := proto.Marshal(req)
    if err != nil {
        log.Printf("proto.Marshal: %v\n", err)
        //	return nil, errors.New("proto.Marshal(req):"+err.Error())
        resp := NewRespByReq(req, "", Err_PROTO_MARSHAL)
        return resp
    }

    reqItem := &ClientRequestItem{Req:req, ReqChan:make(chan *Response)};

    c.rwLock.Lock()
    c.msgItems[req.GetTag()] = reqItem
    c.rwLock.Unlock()

    //	log.Printf("sendMsg req: des: %v\n", req.String())
    switch sendType{
        case kMsgSendToAll:
        c.clients.SendAllConns(bytes)
        case kMsgSendWithIp:
        c.clients.SendWithIp(addr, bytes)
        case kMsgSendWithHash:
        c.clients.SendByHash(addr, bytes)
    }

    var timer *time.Timer
    timer = time.AfterFunc(time.Second*3, func() {
        c.rwLock.Lock()
        defer c.rwLock.Unlock()

        delete(c.msgItems, req.GetTag())

        resp := NewRespByReq(req, "", Err_TIME_OUT)
        reqItem.ReqChan <- resp
        //TODO: 超时的问题，当lib发现超时的消息，ser如果被堵塞，会接收到到，处理完返回给lib。这个时候就会数据不一致
        //TODO: 需要在每个msgItem上加一个timestamp，ser接收到发现超时了就不操作。
        //TODO: lib->ser , ser operation, ser->lib 超时
        //	log.Printf("ErrorCode: %v\n", resp.String())
    })

    //TODO:需要处理timeout
    resp := <-reqItem.ReqChan
    timer.Stop()

    //log.Printf("cliRecv: [%v    :     %v]\n", req.String(), resp.String())

    if resp.GetErrCode() == Err_NOT_LEADER {
        //TODO: 需要延时在重发
    }else if resp.GetErrCode() == Err_TIME_OUT {
        log.Printf("libError: \n%v\n%v\n", req.String(), resp.String())
        return resp
    }
    return resp
}

func (c *Client) cliTcpCB(stat ConnStat, content []byte, cli*TcpCli, ele*TcpBase) {
    if stat == Connect {
        //	fmt.Printf("cliTcpCB Connect: %v\n", ele.ConnIp())
    }else if stat == Disconnect {
        c.reconnect(ele.ConnIp())
        //fmt.Printf("cliTcpCB Disconnect: %v\n", ele.ConnIp())
    }else if stat == Data {
        defer PoolSetByte(content)

        var resp Response
        err := proto.Unmarshal(content, &resp)
        if err != nil {
            log.Printf("proto.Unmarshal: %v\n", err)
        }

        verb := resp.GetVerb()
        tag := resp.GetTag()

        //		log.Printf("cliTcpCB %v\n", content)
        //		log.Printf("cliTcpCB watch: %v\n", resp)
        //Watch and get is push
        if verb&uint32(Verb_WATCH_PUSH) > 0 {
            c.watchChan <- &resp
            return
        }

        c.rwLock.Lock()
        defer c.rwLock.Unlock()
        if msgItem, ok := c.msgItems[tag]; ok {
            delete(c.msgItems, tag)
            msgItem.ReqChan <- &resp
        }else {
            //需要区分推送的消息还是多返回的resp,或者是超时的返回
        }
    }
}

func (c *Client) waitForReq() {
    for {
        select {
        case resp := <-c.watchChan: {
            if resp == nil {
                return
            }
            patPath := resp.GetCnt()
            if k, ok := c.watchCbItems[patPath]; ok {
                path := resp.GetPath()
                value := resp.GetValue()
                verb := Verb(resp.GetVerb())

                go func() {
                    stopWatch := false
                    k(path, value, verb, &stopWatch)
                    if stopWatch {
                        c.rwLock.Lock()
                        delete(c.watchCbItems, path)
                        c.rwLock.Unlock()

                        c.RemoveWatch(patPath)
                    }
                }()
                //				go k(path, value, verb)
            }
        }
        }
    }
}

//	var b bytes.Buffer
//	if _, err := req.Encode(&b); err != nil {
//		traceln("transporter.rv.encoding.error:", err)
//		return nil
//	}

