package util

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/cheggaaa/pb"
    "runtime"
//	"fmt"
//"sync/atomic"//num := atomic.AddUint64(&msgRecvCount, 1)
    "sync"
    "time"
    "os"
    "log"
    "math/rand"
    "math"
    "fmt"
//"../util"
)

//go test -test.cpu 4
//go test -v -bench=".*"
//go test -bench=".*" -cpuprofile=cpu.prof -c
//go test -bench=".*" -cpuprofile=cpu.prof
//go test -v -cpuprofile cpu.out
//go tool pprof basSer.test cpu.prof

// go test -x -v -cpuprofile=prof.out


//--OK--
//go test -bench=".*"  -test.cpuprofile cpu.out -test.cpu 4
//go test -test.memprofile mem.out
//go test -test.cpuprofile cpu.out -test.cpu 4
//go tool pprof baseSer.test cpu.out


//sshfs zouxu@192.168.1.8:/Users/zouxu/dev/_GO/goall macGoall/
//go run testBasSer.go -t ser -ip 192.168.1.150:20000 -msg 20 -cli 10000


//type BaseCliCb func(stat ConnStat, content []byte, cli* TcpCli, ele* TcpBase)
//type BaseSerCb func(stat ConnStat, content []byte, ser* TcpSer, ele* TcpBase)


//go test -bench=".*" -cpuprofile=cpu.prof -c


var (
    ip = "127.0.0.1:20001"
    maxClient = 6000
    maxMsgPerClient = 100000
)
var kTestByte        []string

func init() {
    //bytes := []byte("")
    kTestByte = make([]string, 100)
    for j := 0; j < 4; j++ {
        num := int(math.Pow10(3 + j))
        testByte := []byte("")
        for i := 0; i < num; i++ {
            testByte = append(testByte, byte(i))
        }
        kTestByte[j] = string(testByte)
    }
}

func Test_Ser_SendMsgSequence_BIG_DATA(t*testing.T) {
    return
    resetMaxCli()
    maxClient = 2
    maxMsgPerClient = 20
    kSendNum := 10
    allItem := maxMsgPerClient * maxClient


    testByte := []byte("")
    for i := 0; i < 100000000; i++ {
        testByte = append(testByte, byte(i))
    }
    kTestString := string(testByte)

    index := make(map[uint32]int)
    for i := 0; i < allItem; i++ {
        index[uint32(i)] = 0
    }

    lock := &sync.Mutex{}

    cliCB := func(stat ConnStat, content []byte, cli*TcpCli, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {

            for i := 0; i < kSendNum; i++ {
                //	go func() {

                dddd := []byte(kTestString)
                crc32 := Crc32(dddd)
                buf := NewW()
                buf.WriteUint32(uint32(0))
                buf.WriteUint32(crc32)
                buf.WriteUint32Bytes(dddd)
                data := buf.Bytes()

                cli.SendWithIp(ele.ConnIp(), data)

                //	}()
            }
        }else if stat == Disconnect {
        }else if stat == Data {

            defer PoolSetByte(content)

            buf := NewR(content)
            times := buf.ReadUint32()

            //log.Printf("cli: [%v]\n", times)


            crc32V := buf.ReadUint32()
            data := buf.ReadUint32Bytes()
            tailShouldEmpty := buf.TailBytes()
            assert.Equal(t, 0, len(tailShouldEmpty))
            //log.Printf("[%v:%v]\n", times, data)

            datacrc32 := Crc32(data)
            assert.Equal(t, datacrc32, crc32V)


            send := false

            lock.Lock()
            defer lock.Unlock()
            v := index[times]
            if v > kSendNum {
                return
            }else {
                v++
                index[times] = v
                if v == kSendNum {
                    send = true
                }
            }
            if send {

                times++
                //log.Printf("clientSend: [%v:%v]\n", times, allItem)
                //log.Printf("clientSend: [%v]   %v\n", kSendNum, index)
                dot()
                serOrCliOk.Done()
                if times >= uint32(allItem) {
                    log.Printf("OK [%v:%v]\n", times, allItem)
                    //				time.Sleep(time.Second * 2)
                    return
                }

                for i := 0; i < kSendNum; i++ {
                    //	go func() {
                    bufW := NewW()
                    bufW.WriteUint32(times)
                    bufW.WriteUint32(crc32V)
                    bufW.WriteUint32Bytes(data)
                    dataW := bufW.Bytes()

                    cli.SendWithIp(ele.ConnIp(), dataW)
                    //	}()
                }
            }
        }
    }

    serCB := func(stat ConnStat, content[]byte, ser*TcpSer, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {
        }else if stat == Disconnect {
        }else if stat == Data {
            //因为马上又在使用所以不能调用PoolSetByte(content)

            //go func() {

            buf := NewR(content)
            buf.ReadUint32()
            //log.Printf("ser: [%v]\n", times)
            crc32V := buf.ReadUint32()
            data := buf.ReadUint32Bytes()
            tailShouldEmpty := buf.TailBytes()
            assert.Equal(t, 0, len(tailShouldEmpty))
            //log.Printf("[%v:%v]\n", times, data)

            datacrc32 := Crc32(data)
            assert.Equal(t, datacrc32, crc32V)


            ser.SendWithIp(ele.ConnIp(), content)
            //}()
        }
    }

    initSerAndClient2(t, 20002, allItem, allItem, cliCB, serCB)
}

func Test_Ser_SendMsg(t*testing.T) {

    cliCB := func(stat ConnStat, content []byte, cli*TcpCli, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {
            index := rand.Int() % len(kTestByte)
            //	log.Printf("[%d:%d]", index, len(kTestByte))
            dddd := []byte(kTestByte[index])
            buf := NewW()
            buf.WriteUint32(uint32(0))
            buf.WriteUint32Bytes(dddd)
            data := buf.Bytes()

            cli.SendWithIp(ele.ConnIp(), data)
        }else if stat == Disconnect {
        }else if stat == Data {

            defer PoolSetByte(content)

            buf := NewR(content)
            times := buf.ReadUint32()
            data := buf.ReadUint32Bytes()
            tailShouldEmpty := buf.TailBytes()
            assert.Equal(t, 0, len(tailShouldEmpty))
            //log.Printf("[%v:%v]\n", times, data)
            times++
            //log.Printf("[%v:%v]\n", times, maxMsgPerClient)
            dot()
            if times >= uint32(maxMsgPerClient) {
                //	log.Printf("OK [%v:%v]\n", times, maxMsgPerClient)
                serOrCliOk.Done()
                //				time.Sleep(time.Second * 2)
                return
            }

            bufW := NewW()
            bufW.WriteUint32(times)
            bufW.WriteUint32Bytes(data)
            dataW := bufW.Bytes()

            cli.SendWithIp(ele.ConnIp(), dataW)

        }
    }

    serCB := func(stat ConnStat, content []byte, ser*TcpSer, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {
        }else if stat == Disconnect {
        }else if stat == Data {
            //因为马上又在使用所以不能调用PoolSetByte(content)
            ser.SendWithIp(ele.ConnIp(), content)
        }
    }
    resetMaxCli()
    initSerAndClient2(t, 20002, maxMsgPerClient*maxClient, maxClient, cliCB, serCB)
}



func Test_Ser_AuthTime(t*testing.T) {
    cliCB := func(stat ConnStat, content []byte, cli*TcpCli, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {
        }else if stat == Disconnect {
        }else if stat == Data {
        }
    }
    serCB := func(stat ConnStat, content []byte, ser*TcpSer, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {
            ser.AuthInSecond(ele.ConnIp(), 2)
        }else if stat == Disconnect {
            serOrCliOk.Done()
            dot()
        }else if stat == Data {
        }
    }
    resetMaxCli()
    initSerAndClient2(t, 20002, maxClient, maxClient, cliCB, serCB)
}

func Test_Ser_AuthOk(t*testing.T) {
    cliCB := func(stat ConnStat, content []byte, cli*TcpCli, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {
        }else if stat == Disconnect {
        }else if stat == Data {
        }
    }
    serCB := func(stat ConnStat, content []byte, ser*TcpSer, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT) {
        if stat == Connect {
            ser.AuthInSecond(ele.ConnIp(), 2)
            go func() {
                time.Sleep(time.Second * 1)
                ser.AuthSuc(ele.ConnIp(), "abc")
                dot()
                serOrCliOk.Done()
            }()
        }else if stat == Disconnect {

        }else if stat == Data {
        }
    }
    resetMaxCli()
    initSerAndClient2(t, 20002, maxClient, maxClient, cliCB, serCB)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////// INIT ////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////

func resetMaxCli() {
    hostName, _ := os.Hostname()
    log.Printf("name: %v\n", hostName)
    if (hostName == "zxMBP.local" || hostName == "zxmbp.local" ||
    hostName == "mini.local") {
        maxClient = 100
        maxMsgPerClient = 1000
    }
}


func reportCurrentGo(title string) int {
    time.Sleep(time.Second)
    curGo := runtime.NumGoroutine()
    log.Printf("GO:%d, %v\n", curGo, title)
    return curGo
}

type DOT func()
type BaseCliCbTest func(stat ConnStat, content []byte, cli*TcpCli, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT)
type BaseSerCbTest func(stat ConnStat, content []byte, ser*TcpSer, ele*TcpBase, serOrCliOk*sync.WaitGroup, dot DOT)

func initSerAndClient2(t*testing.T, port int, testItem int, missionItem int, cliCB BaseCliCbTest, serCB BaseSerCbTest) {

    kOutGo := 2*maxClient
    maxCli := maxClient
    serIp := fmt.Sprintf("127.0.0.1:%d", port)

    bar := initProgress(testItem)

    incBar := func() {
        bar.Increment()
    }

    serOrCliOk := &sync.WaitGroup{}
    connectWG := &sync.WaitGroup{}
    connectWG.Add(maxCli)
    serOrCliOk.Add(missionItem)
    startGo := runtime.NumGoroutine()

    reportCurrentGo("init")
    serStartGo := reportCurrentGo("serCreate")

    //init server
    serCBBase := func(stat ConnStat, content []byte, ser*TcpSer, ele*TcpBase) {
        if stat == Connect {
            connectWG.Done()
        }
        serCB(stat, content, ser, ele, serOrCliOk, incBar)
    }
    ser := NewTcpSer(serIp, nil, serCBBase, kOutGo, 100, 2, 60)

    //init client
    allClientConnectOk := false
    cliCBBase := func(stat ConnStat, content []byte, ser*TcpCli, ele*TcpBase) {
        if allClientConnectOk {
            cliCB(stat, content, ser, ele, serOrCliOk, incBar)
        }
    }

    connStartGo := reportCurrentGo("connStart")
    var clis []*TcpCli
    for i := 0; i < maxCli; i++ {
        cli := NewTcpCli(cliCBBase, true, kOutGo, 100, 60)
        cli.Tag = i
        clis = append(clis, cli)
    }

    for i := 0; i < maxCli; i++ {
        cli := clis[i]
        suc := cli.Connect(serIp)
        if suc != nil {
            log.Printf("not conenct suc: %v\n", serIp)
        }
    }

    connectWG.Wait()
    log.Printf("conns [%d] connnectd\n", maxCli)
    allClientConnectOk = true

    missionStart := reportCurrentGo("mssionStart")

    //重新发送连接成功的消息
    for i := 0; i < maxCli; i++ {

        cli := clis[i]
        cliItem := cli.ConnByIp(serIp)
        go func() {
            cliCB(Connect, nil, cli, cliItem, serOrCliOk, incBar)
            //cliCBBase(Connect, nil, cli, cliItem)
        }()
    }

    serOrCliOk.Wait()

    missionEnd := reportCurrentGo("mssionComplete")

    for i := 0; i < maxCli; i++ {
        cli := clis[i]
        cli.Close()
    }
    connEndGo := reportCurrentGo("connEnd")

    ser.CloseAllConns()
    ser.Close()
    serEndGo := reportCurrentGo("serClose")

    time.Sleep(time.Second * 2)
    endGo := runtime.NumGoroutine()
    //log.Printf("close cli conn,%d, %d, %d, %d\n", beginGo, serGo, startGo, closeAllGo)

    //return
    assert.Equal(t, missionStart, missionEnd)
    assert.Equal(t, connStartGo, connEndGo)
    assert.Equal(t, serStartGo, serEndGo)
    assert.Equal(t, startGo, endGo)

    bar.Finish()
}




func Benchmark_getAdmin(b *testing.B) {
    for i := 0; i < b.N; i++ { //use b.N for looping
        //getAdmin(1)
    }
}





func initProgress(count int) *pb.ProgressBar {
    bar := pb.New(count)

    // show percents (by default already true)
    bar.ShowPercent = true

    // show bar (by default already true)
    bar.ShowPercent = true

    // no need counters
    bar.ShowCounters = true

    bar.ShowTimeLeft = true

    // and start
    bar.Start()
    return bar
    //	for i := 0; i < count; i++ {
    //		bar.Increment()
    //		time.Sleep(time.Millisecond)
    //	}
    //	bar.FinishPrint("The End!")
}
