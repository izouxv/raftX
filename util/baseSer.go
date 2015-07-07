package util

import (
    "crypto/rand"
    "crypto/tls"
//"fmt"
    "io"
    "net"
    "os"
    "time"
    "syscall"
    "log"
    "sync"
    "sync/atomic"
    "runtime/debug"
    "github.com/golang/glog"
    "github.com/serialx/hashring"
    "runtime"
    "strings"
    "encoding/binary"
//	"../util"
    "math"
    "errors"
)


//                 \,,,/
//                 (o o)
//        -----oOOo-(_)-oOOo-----
//
//如果ser里的cb被堵塞，这个ser的cliitem就会的input就会被堵塞，是不是需要一种chan来统一处理这些消息？需要思考一下，
//是否需要一个消息队列，非常短时间后去读取这个队列，如果发送了读取队列消息后，定时期间的消息就不需要再发送读取的时间了？

//想了一下outputgo做成可变个数，但是有些问题,在outputgo变化时，有部分数据会错误，Maybe，要看这个数据调用多少次conn write
//发送的时候conn是好的，sendAction的时候挂了，这可能有问题，因为上层不知道这个conn挂了
//心跳需要自己来维持

//可以使用SetReadDeadline来做心跳超时
//统一的Q会出现多个go同时写一个conn的情况，数据出现乱序

var syncPoolMap []*sync.Pool
var syncPoolKey []int

func init() {
    for i := 10; i < 21; i++ {
        num := int(math.Pow(2, float64(i)))
        //fmt.Printf("%d %d\n", i, num)
        syncPoolKey = append(syncPoolKey, num)
        syncPoolMap = append(syncPoolMap, &sync.Pool{})
    }
   // log.Printf("baseSerInitPool 1\n")
}

func rwPool(size int) (*sync.Pool, int) {
    for i := 0; i < len(syncPoolKey); i++ {
        poolSize := syncPoolKey[i]
        if poolSize >= size {
            return syncPoolMap[i], poolSize
        }
    }
    return nil, size
}

func PoolSetByte(orgBytes []byte) {//WARING 只有在数据使用完毕后才能调用，如果提前调用需要加入defer。如果数据需要转发给其他地方，绝对不可以调用，切记！！！
    rwPoolSetByte(&orgBytes)
}

func rwPoolSetByte(b*[]byte) {
    if pool, _ := rwPool(cap(*b)); pool != nil {
        pool.Put(b)
    }
}

//TODO: 返回的byte不能异步传送给别的地方，如果需要异步传送到别地方，那么input完事后不能加入pool里
func rwPoolGetByte(size int) (*[]byte) {
    pool, maxSize := rwPool(size);
    if pool != nil {
        if v := pool.Get(); v != nil {
            content := v.(*[]byte)
            return content
        }
    }
    b := make([]byte, maxSize)
    return &b
}

const (
    KGlogLevel = 3
    KConnTimeoutSeconds = 3
//    kListenerTempErrDelay = 5 * time.Millisecond
    k1MNotCopyHead = 1 << 20
)


const (
    ReadOK = iota
    ReadError

    ReadTimeOut
    KReplyStatSendSuc
    KReplyStatSendTimeOut
    KReplyStatPingTimeOut
    KReplyStatPongTimeOut
)

type ConnStat int

const (
    pingOrPong ConnStat = iota
    ppTimeOut
    Connect //you can call one of
    Disconnect
    Data

)

type PingPongStat int

const (
    kWaitPing PingPongStat = iota
    kWaitPong
    kToPing

)

//重用conn在多线程的环境下可能出现，一个conn发了一半，另外一个线程下这个conn又被发送，出现数据错乱的问题
//可以加N个chan，chan对应一个go，每个ip hash这个N，这样不会出现多线程的问题

//type baseBaseCb func(stat ConnStat, content []byte, ele* TcpBase)
type BaseCliCb func(stat ConnStat, content []byte, cli*TcpCli, ele*TcpBase)
type BaseSerCb func(stat ConnStat, content []byte, ser*TcpSer, ele*TcpBase)

type tcpBaseCb interface {
    callback(stat ConnStat, content []byte, ele*TcpBase)
    HeartBeatSecond() int
}


type TcpBase struct {
    cb            tcpBaseCb
    conn          net.Conn
    connIp        string //用户的IP
    connId           string
    timer *time.Timer
    PPStat                                 PingPongStat
    ppTimeOutCount                int8
}

func (tcp* TcpBase) ConnIp() (string) {
    return tcp.connIp
}

func (tcp* TcpBase) ConnId() (string) {
    return tcp.connId
}

func (tcp *TcpBase) close() {
    tcp.conn.Close()
    if tcp.timer != nil {
        tcp.timer.Stop()
    }
}
func (tcp*TcpBase) connHasError(tmpLen int, err error) bool {
    if err != nil || tmpLen == 0 {
        defer func() {
          //  log.Printf("connHasError: %v, len: %d\n", err, tmpLen)
        }()
        //fmt.Println("CONN ERROR")
        //	log.Printf("connHasError: %v, len: %d\n", err, tmpLen)
        if tmpLen == 0 {//other close connection
            //tcp.sendingQ<-nil//send nil to sendingQ, let it complete
            tcp.cb.callback(Disconnect, nil, tcp)
            //(Note that this is not special to Go, read()/recv() returning 0 on a TCP connection more or less universally means the other end has closed the connection)
            return true
        }
        if err == io.EOF {
            //fmt.Println("the app is shutdown")
        }
        if neterr, ok := err.(net.Error); ok {//net.OpError
            if ok && neterr.Timeout() {
                //return ReadTimeOut
            }
        }else if _, ok := err.(syscall.Errno); ok {
            glog.V(KGlogLevel).Infof("syscall.Errno\n")
            //fmt.Println("syscall.Errno")
        }
        //tcp.sendingQ<-nil//send nil to sendingQ, let it complete
        tcp.cb.callback(Disconnect, nil, tcp)
        return true
    }
    return false
}

func (tcp*TcpBase) readByte(b []byte, maxLength int) (uint8) {//0:ok 1:err 2:timeout
    readedLeng := 0
    for {
        pkg := b[readedLeng:maxLength]
        //tcp.conn.SetReadDeadline(time.Now().Add(KConnTimeoutSeconds * time.Second))
        leng, error := tcp.conn.Read(pkg)
        if tcp.connHasError(leng, error) {
            //fmt.Printf("read error:%d, %v\n\n\n\n", maxLength, error)
            return ReadError
        }
        readedLeng = readedLeng + leng
        if readedLeng == maxLength {
            return ReadOK
        } else {
        }
    }
    return ReadError
}



func (tcp* TcpBase) resetPPStat(isSer bool) {
    tcp.PPStat = kWaitPong
    if isSer {
        tcp.waitPing()
    }else {
        tcp.toPing()
    }
}
func (tcp*TcpBase) IsAuth() bool {
    return len(tcp.ConnId()) > 0
}

func (tcp* TcpBase) authOut() {
}


func (tcp* TcpBase) toPing() {
    // log.Printf("toPing\n")
    tcp.timerReset(kToPing, tcp.cb.HeartBeatSecond())
}

func (tcp* TcpBase) waitPong() {
    //log.Printf("waitPong\n")
    tcp.timerReset(kWaitPong, tcp.cb.HeartBeatSecond())
}

func (tcp* TcpBase) waitPing() {
    // log.Printf("waitPing\n")
    tcp.timerReset(kWaitPing, tcp.cb.HeartBeatSecond())
}
func (tcp* TcpBase) timerOutCallBack() {
    // log.Printf("timerOutCallBack\n")

        tcp.ppTimeOutCount++
        tcp.cb.callback(ppTimeOut, nil, tcp)

}
func (tcp* TcpBase) resetPPTimeOut() {
    tcp.ppTimeOutCount=0
}

func (tcp* TcpBase) timerReset(stat PingPongStat, second int) {
    if tcp.timer == nil {
        tcp.timer = time.AfterFunc(time.Second*1, tcp.timerOutCallBack)
    }

    tcp.PPStat = stat

    d := time.Second * time.Duration(second)
    // log.Printf("timerAfter: %v  %v\n", d, second)
    tcp.timer.Reset(d)
}

func (tcp*TcpBase) handleInput() {

    defer func() {
        if err := recover(); err != nil {
            debug.PrintStack()
            glog.Error("handleInput error(%v)", err)
        }else {
        }
    }()

    headBuf := make([]byte, 4)
    var maxLength int
    var stat uint8
    var orgBytes*[]byte
    var content []byte
    for {
        stat = tcp.readByte(headBuf, 4)
        if stat == ReadError {
            return
        }

        maxLength = int(binary.BigEndian.Uint32(headBuf))
        //	maxLength = int(util.BytesToUint32(headBuf))
        if maxLength == 0 {
            tcp.cb.callback(pingOrPong, content, tcp)
            continue//heart beat
        }
        //content := make([]byte, maxLength)
        orgBytes = rwPoolGetByte(maxLength)
        content = (*orgBytes)[0:maxLength]
        stat = tcp.readByte(content, maxLength)
        if stat == ReadOK {
            //tcp.conn.SetReadDeadline(time.Now().Add(c.Timeout))
            tcp.cb.callback(Data, content, tcp)
        }else {
            return
        }
    }
}

func (tcp*TcpBase) sendActionWithMsg(msg*tcpMsg) {
    //当前正在发送，先把消息放在队列里面，然后挨个挨个发送，活着直接发送［］byte里发
    hasHead := msg.hasHead
    data := msg.data
    if hasHead {
        if tcp.sendAction(data) {
        }else {
            return
        }
        rwPoolSetByte(&data)
    }else {
        content := make([]byte, 4)
        binary.BigEndian.PutUint32(content, uint32(len(data)))
        if tcp.sendAction(content) && tcp.sendAction(data) {
        }else {
            log.Printf("errInSending: %v\n", tcp.ConnIp())
            return
        }
    }
}


func (tcp* TcpBase) sendAction(data []byte) bool {
    totalLen := len(data)
    sendLen := 0
    for { //by_zouxu
        pkg := data[sendLen:totalLen]
        //	tcp.conn.SetWriteDeadline(time.Now().Add(KConnTimeoutSeconds * time.Second))
        tmpLen, error := tcp.conn.Write(pkg)
        if tcp.connHasError(tmpLen, error) {
            log.Printf("write error: %v\n\n\n\n\n", error)
            return false
        }
        sendLen = sendLen + tmpLen
        if totalLen == sendLen {
            break
        } else {
            glog.V(KGlogLevel).Infof("handleOutputing 3  %d-%d\n", totalLen, tmpLen)
        }
    }

    return true
}

func NewTcpBase(cb tcpBaseCb, conn net.Conn) (*TcpBase) {
    tcp := &TcpBase{
        conn:conn,
        cb:cb,
        connIp:conn.RemoteAddr().String(),
    }
    //	tcp.connIp = conn.RemoteAddr().String()
    //tcp.heartBeatTime = time.Now().Unix()

    //TODO: tls not tested
    //有些设备对这个属性不支持，所以这个设置不能100%保证是active的
    //TODO: 需要使用心跳来保证链接可用

    if tcpConn, ok := conn.(*net.TCPConn); ok {
        tcpConn.SetKeepAlive(true)
        tcpConn.SetKeepAlivePeriod(time.Minute)
    }
    //tcpConn.SetNoDelay(true)
    return tcp
}

/////////////////////////////////////////////////
/////////////////////////////////////////////////

type tcpMsg struct {
    ip         string
    data       []byte
    hasHead    bool
}

func newTcpMsg(ip string, data []byte) (msg*tcpMsg) {
    length := len(data)
    //	if true || length < KRW4kSize {
    if length < k1MNotCopyHead {//raftX出现卡死的现象是else条件导致的。我晕
        //	if false {//TODO: bug
        totalLen := length + 4
        orgBytes := rwPoolGetByte(totalLen)
        content := (*orgBytes)[0:totalLen]

        binary.BigEndian.PutUint32(content, uint32(length))
        //TODO: 效率需要优化，不必要的内存拷贝，或者可以把消息头部分放在发送部分，用状态区分。（刚进入函数体发送时候先发送头，然后再发送包体）
        if length > 0 {//length is zero is heart beat
            copy(content[4:totalLen], data)
        }
        msg = &tcpMsg{ip:ip, data:content, hasHead:true}
        return msg;
    }else {
        msg = &tcpMsg{ip:ip, data:data, hasHead:false}
        return msg;
    }
}

/////////////////////////////////////////////////
/////////////////////////////////////////////////

type TcpNBase struct {
    sync.RWMutex
    Tag                                    int
    ConnCount                              uint32
    cons                          map[string]*TcpBase
    connId                          map[string]string //{userId:ip}
    sendingQ                               chan *tcpMsg
    outputGoNum                            uint32
    isSer                                  bool
    heartBeatSecond                        int
    ppMaxTimeOutTimes   int8
}

func (tcp* TcpNBase) HeartBeatSecond() int {
    return tcp.heartBeatSecond
}


func (tcp* TcpNBase) ConIdByIp(ip string) (string, error) {
    tcp.RLock()
    defer tcp.RUnlock()

    if conn, ok := tcp.cons[ip]; ok {
        return conn.connId, nil
    }
    return "", errors.New("NotFound")
}
func (tcp* TcpNBase) ConnByIp(ip string) (*TcpBase) {
    tcp.RLock()
    defer tcp.RUnlock()

    if conn, ok := tcp.cons[ip]; ok {
        return conn
    }
    return nil
}


func (tcp* TcpNBase) AuthOut(connId string) {
    tcp.Lock()
    defer tcp.Unlock()

    if ip, ok := tcp.connId[connId]; ok {
        delete(tcp.connId, connId)
        conn := tcp.ConnByIp(ip)
        conn.authOut()
        conn.connId = ""
    }
}


func (tcp* TcpNBase) SetConnId(ip, connId string) {//true is new, false is replace
    tcp.Lock()
    defer tcp.Unlock()

    if conn, ok := tcp.cons[ip]; ok {
        conn.connId = connId
        tcp.connId[connId] = ip
    }
}

func (tcp* TcpNBase) initData(goNum int, outGoBuf int, heartBeatSecond int, isSer bool) {

    tcp.isSer = isSer
    tcp.cons = make(map[string]*TcpBase)
    tcp.connId = make(map[string]string)
    tcp.ConnCount = 0
    tcp.sendingQ = make(chan *tcpMsg, outGoBuf)
    tcp.outputGoNum = uint32(goNum)
    tcp.ppMaxTimeOutTimes = 3
    //tcp.createOutputGo(goNum)
    GoDone(goNum, tcp.handleOutput)

    tcp.heartBeatSecond = heartBeatSecond
}


func (tcp* TcpNBase) createNewItem(cb tcpBaseCb, conn net.Conn) (*TcpBase) {
    item := NewTcpBase(cb, conn)
    connIp := item.connIp
    tcp.Lock()
    tcp.ConnCount++
    tcp.cons[connIp] = item
    tcp.Unlock()
    if tcp.isSer {
        item.waitPing()
    }else {
        item.toPing()
    }
    item.cb.callback(Connect, nil, item)
    GoDone(1, item.handleInput)
    return item
}

func (tcp* TcpNBase) SendAllConns(bytes []byte) {
    tcp.RLock()
    defer tcp.RUnlock()
    for k, _ := range tcp.cons {
        tcp.SendWithIp(k, bytes)
    }
}

func (tcp* TcpNBase) sendPing(connIp string) {
    //log.Printf("send ping\n")
    tcp.SendWithIp(connIp, nil)
}

func (tcp* TcpNBase) sendPong(connIp string) {
  //  log.Printf("send pong\n")
    tcp.SendWithIp(connIp, nil)
}

func (tcp* TcpNBase) handleOutput() () {
    for {
        select {
        case msg := <-tcp.sendingQ:
            if msg == nil {
                return
            }
            tcp.sendWithTcpMsg(msg)
        }
    }
}

func (tcp* TcpNBase) SendWithConn(connId string, bytes []byte) {
    tcp.RLock()
    defer tcp.RUnlock()
    if connIp, ok := tcp.connId[connId]; ok {
        tcp.SendWithIp(connIp, bytes)
        //tcp.sendingQ<-newTcpMsg(connIp, bytes)
    }
}

func (tcp* TcpNBase) SendWithIp(connIp string, bytes []byte) {
    tcp.sendingQ <- newTcpMsg(connIp, bytes)
}

func (tcp* TcpNBase) sendWithTcpMsg(msg*tcpMsg) {
    connIp := msg.ip
    tcp.RLock()
    if conn, ok := tcp.cons[connIp]; ok == true {
        tcp.RUnlock()
        conn.sendActionWithMsg(msg)
        if !tcp.isSer {
            //		conn.waitPong()
        }
    }else {//可能在发送队列中，这个conn已经断掉了
        //fmt.Printf("connIsDown: %v\n", connIp)
        tcp.RUnlock()
    }
}

func (tcp *TcpNBase) Close() {
    tcp.Lock()
    defer tcp.Unlock()

    for k, _ := range tcp.cons {
        tcp.connRemove(k)
    }
    close(tcp.sendingQ)
}

func (tcp *TcpNBase) CloseAllConns() {
    tcp.Lock()
    defer tcp.Unlock()

    //test key existence in a map?
    for k, _ := range tcp.cons {
        tcp.connRemove(k)
    }
}
func (tcp *TcpNBase) connRemove(connIp string) {
    if val, ok := tcp.cons[connIp]; ok {

        tcp.ConnCount--
        //fmt.Printf("connRemove: %v   %d\n", connIp, tcp.ConnCount)
        delete(tcp.connId, val.connId)
        delete(tcp.cons, connIp)
        val.close()
        //fmt.Printf("tcpClose: %v\n", ConnIp)
    }
}

func (tcp *TcpNBase) connRemoveByConnid(connId string) {
    if ip, ok := tcp.connId[connId]; ok {
        tcp.connRemove(ip)
    }
}

func (tcp *TcpNBase) ConnRemoveByConnId(connId string) {
    //test key existence in a map?
    tcp.Lock()
    defer tcp.Unlock()

    tcp.connRemoveByConnid(connId)
}

func (tcp *TcpNBase) ConnRemove(connIp string) {
    //test key existence in a map?
    tcp.Lock()
    defer tcp.Unlock()

    tcp.connRemove(connIp)
}

type TlsCfg struct {
    Pem string
    Key string
}

type TcpSer struct {
    TcpNBase
    cb                     BaseSerCb
    listener               net.Listener
    MaxConn                uint32
}

func NewTcpSer(ipPort string, tlsCfg*TlsCfg, cb BaseSerCb, maxOutgo int, outGoBuf int, listenerGoNum int, heartBeatSecond int) (*TcpSer) {

    listener := GetListener(ipPort)
    if (tlsCfg != nil) {
        listener = ListenerEnableTLS(listener, tlsCfg)
    }
    tcp := &TcpSer{
        cb:cb,
        listener:listener,
        MaxConn:3000000,
    }

    tcp.initData(maxOutgo, outGoBuf, heartBeatSecond, true)
    GoDone(listenerGoNum, tcp.listenAction)
    log.Printf("NewTcpSer start ok: %v\n", ipPort)
    return tcp
}

func ListenerEnableTLS(listener net.Listener, tlsCfg*TlsCfg) net.Listener {
    //openssl genrsa -out server.key 1024
    //openssl req -new -key server.key -out csr.pem
    //openssl x509 -req -in csr.pem -signkey server.key -out cert.pem
    pwd, err := os.Getwd()
    glog.V(KGlogLevel).Infof("pwd: %v\n", pwd)
    //fmt.Printf("pwd: %v\n", pwd)
    //	cert, err := tls.LoadX509KeyPair("./src/utilx/cert.pem", "./src/utilx/server.key")
    cert, err := tls.LoadX509KeyPair(tlsCfg.Pem, tlsCfg.Key)
    if err != nil {
        glog.Fatalf("server: loadkeys: %s", err)
        //log.Fatalf("server: loadkeys: %s", err)
    }
    config := tls.Config{Certificates: []tls.Certificate{cert}}
    config.Time = time.Now
    config.Rand = rand.Reader
    listen := tls.NewListener(listener, &config)
    return listen
}

func GetListener(ipPort string) net.Listener {
    addr, err := net.ResolveTCPAddr("tcp", ipPort)
    if err != nil {
        glog.V(KGlogLevel).Infof("net.ResolveTCPAddr(\"tcp\"), %s) error(%v)", ipPort, err)
        panic(err)
    }

    listener, err := net.ListenTCP("tcp", addr)

    if err != nil {
        glog.V(KGlogLevel).Infof("net.ListenTCP(\"tcp4\", \"%s\") error(%v)", addr, err)
        panic(err)
    }
    return listener
}

func (tcp *TcpSer) Close() {
    tcp.listener.Close()
    tcp.TcpNBase.Close()
}

func (tcp* TcpSer) listenAction() {

    defer func() {
        if err := recover(); err != nil {
            debug.PrintStack()
            glog.Error("handleInput error(%v)", err)
        }else {
        }
    }()

    for {
        conn, err := tcp.listener.Accept()

        if err != nil {
            if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
                log.Printf("NOTICE: temporary Accept() failure - %s", err.Error())
                runtime.Gosched()
                continue
            }
            // theres no direct way to detect this error because it is not exposed
            if !strings.Contains(err.Error(), "use of closed network connection") {
                log.Printf("ERROR: listener.Accept() - %s", err.Error())
            }
            break
        }

        if tcp.ConnCount+1 == tcp.MaxConn {
            glog.Warningf("Too many connections! Active Denial %s\n", conn.RemoteAddr().String())
            conn.Close()
            runtime.Gosched()
            continue
        }
        go tcp.createNewItem(tcp, conn)
    }
}

func (tcp* TcpSer) callback(stat ConnStat, content []byte, ele*TcpBase) {
    switch stat{
        case Connect: {
            ele.waitPing()
            tcp.cb(stat, content, tcp, ele)
        }
        case Disconnect: {
        //    log.Printf("serDisconnect: %v", ele.ConnIp())
            id := ele.connIp
            tcp.ConnRemove(id)
            tcp.cb(stat, content, tcp, ele)
        }
        case Data: {
            ele.waitPing()
            tcp.cb(stat, content, tcp, ele)
        }
        case ppTimeOut: {
            if ele.PPStat == kWaitPing {
                //  log.Printf("serWatingPingTimeOut: %v [%v:%v]", ele.ConnIp(), tcp.ppMaxTimeOutTimes, ele.ppTimeOutCount)
                if tcp.ppMaxTimeOutTimes<ele.ppTimeOutCount {
                    tcp.callback(Disconnect, content, ele)
                }else {
                    ele.waitPing()
                }
            }else {
                log.Printf("ser tcpGenCb stat error: %v\n", ele.PPStat)
            }
        }
        case pingOrPong: {
           // log.Printf("recv ping\n")
            ele.resetPPTimeOut()
            tcp.sendPong(ele.ConnIp())//TODO:这两个是有时间差异的，因为sendPong可能在队列里呆几秒
            ele.waitPing()
        }
        default: {
            glog.Info("Error")
            atomic.AddUint32(&tcp.ConnCount, 1)
        }
    }
}

type TcpCli struct {
    TcpNBase
    cb                  BaseCliCb
    hashRing*hashring.HashRing
    enableHash          bool
}

func NewTcpCli(bsCb BaseCliCb, enableHash bool, maxOutgo int, outGoBuf int, heartBeatSecond int) (*TcpCli) {
    tcp := &TcpCli{
        cb:bsCb,
        enableHash:enableHash,
    }
    if tcp.enableHash {
        tcp.hashRing = hashring.New([]string{})
    }
    tcp.initData(maxOutgo, outGoBuf, heartBeatSecond, false)
    return tcp
}

func (tcp* TcpCli) callback(stat ConnStat, content []byte, ele*TcpBase) {

    switch stat{
        case Connect: {
            ip := ele.connIp
            if tcp.enableHash {
                tcp.addHash(ip)
            }
            ele.toPing()
            tcp.cb(stat, content, tcp, ele)
        }
        case Disconnect: {
        //    log.Printf("cliDisconnect: %v", ele.ConnIp())
            ip := ele.connIp
            if tcp.enableHash {
                tcp.delHash(ip)
            }
            tcp.ConnRemove(ip)
            tcp.cb(stat, content, tcp, ele)
        }
        case Data: {
            ele.toPing()
            tcp.cb(stat, content, tcp, ele)
        }
        case ppTimeOut: {
            if ele.PPStat == kToPing {
                tcp.sendPing(ele.ConnIp())
                ele.waitPong()
            }else if ele.PPStat == kWaitPong {
                //timeout, conn is disconnet
                if tcp.ppMaxTimeOutTimes<ele.ppTimeOutCount {
                    log.Printf("cliWatingPongTimeOut: %v", ele.ConnIp())
                    tcp.callback(Disconnect, content, ele)
                }else {
                    tcp.sendPing(ele.ConnIp())
                    ele.waitPong()
                }
            }else {
                log.Printf("cli tcpGenCb stat error\n")
            }
        }
        case pingOrPong: {
           // log.Printf("recv pong\n")
            ele.resetPPTimeOut()
            ele.toPing()
        }
        default: {
            glog.Info("Error")
            atomic.AddUint32(&tcp.ConnCount, 1)
        }
    }
}

func (tcp*TcpCli) Connect(ipPort string) error {

    addr, err := net.ResolveTCPAddr("tcp", ipPort)
    if err != nil {
        log.Println(err)
        return err
    }

    conn, err := net.DialTCP("tcp", nil, addr)
    if err != nil {
        log.Printf("cli: connectError: %v   %v\n", ipPort, err)
        glog.V(KGlogLevel).Infof("Error NewBaseCli creating: %v, %v\n", ipPort, err)
        return err
    }
    base := tcp.createNewItem(tcp, conn)

    //waring hostlocal会变成127.0.0.1
    if base.ConnIp() != ipPort {
        log.Printf("\n\n\nConnect Error ［%v %v］  \n\n\n\n", ipPort, base.ConnIp())

        //		tcp.Lock()
        //		delete(tcp.cons, base.ConnIp())
        //		tcp.cons[ipPort] = base
        //		base.connIp = ipPort
        //		tcp.Unlock()
        //		fmt.Printf("\n\n\nConnect Error ［%v %v］  \n\n\n\n", ipPort, base.ConnIp())
    }

    //fmt.Printf("tcpCli: connectOk: %v\n", ipPort)
    return nil
}

func (tcp*TcpCli) ConnRemove(ip string) {
    if tcp.enableHash {
        tcp.delHash(ip)
    }
    tcp.TcpNBase.ConnRemove(ip)
}

func (tcp*TcpCli) addHash(ipAddr string) {
    tcp.Lock()
    defer tcp.Unlock()
    tcp.hashRing = tcp.hashRing.AddNode(ipAddr)
}

func (tcp*TcpCli) delHash(ipAddr string) {
    tcp.Lock()
    defer tcp.Unlock()
    tcp.hashRing = tcp.hashRing.RemoveNode(ipAddr)
}

func (tcp*TcpCli) SendByHash(key string, content []byte) {
    if !tcp.enableHash {
        log.Printf("you are not enable hash")
        return
    }

    //userIdStr := fmt.Sprintf("userId:%d", userId)//by_zouxu 可以Hash优化称int64的
    tcp.RLock()
    defer tcp.RUnlock()

    nodeSerIp, ok := tcp.hashRing.GetNode(key)
    if (!ok) {
        glog.Warning("sendDataToNodeSer hash not have: %v", nodeSerIp)
        return
    }
    tcp.SendWithIp(nodeSerIp, content)
}

