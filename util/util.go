package util

import (
    "encoding/binary"
    "math/rand"
    "time"
    "os"
    "strings"
    "sync"
    "runtime/debug"
    "github.com/golang/glog"
    "encoding/json"
    "log"
// "reflect"
//"fmt"
    "github.com/mitchellh/mapstructure"
    "github.com/fatih/structs"
//   "fmt"
    "reflect"
)

func Uint32InBytes(v uint32, b[]byte) {
    b[0] = byte(v >> 24)
    b[1] = byte(v >> 16)
    b[2] = byte(v >> 8)
    b[3] = byte(v)
    //	binary.BigEndian.PutUint32(b, v)
}

func Uint32ToBytes(i uint32) []byte {
    var buf = make([]byte, 4)
    binary.BigEndian.PutUint32(buf, i)
    return buf
}

func BytesToUint32(buf []byte) uint32 {
    return binary.BigEndian.Uint32(buf)
}

func RandInt(max int) uint32 {
    rand.Seed(time.Now().UTC().UnixNano())
    return uint32(rand.Intn(max))
}

func IsBigEndian() bool {
    var i int32 = 0x12345678
    var b byte = byte(i)
    if b == 0x12 {
        return true
    }
    return false
}

func AppName() string {
    if (len(os.Args) == 0) {
        return "NA"
    }
    appPath := os.Args[0]
    args := strings.Split(appPath, "/")

    if (len(args) == 0) {
        return "NA"
    }


    appName := args[len(args) - 1]
    return appName
}

type GoFun func()

func GoDone(num int, fun GoFun) {
    done := &sync.WaitGroup{}
    done.Add(int(num))
    for i := 0; i < num; i++ {
        go func() {
            done.Done()
            defer func() {
                if err := recover(); err != nil {
                    debug.PrintStack()
                    glog.Error("handleOutput error(%v)\nthis hash's client will not use, it's VERY bad\n", err)
                }
            }()
            fun()
        }()
    }
    done.Wait()
}

type GoLaunchFun func(args ...interface{})

func GoMustBeLaunch(fun GoLaunchFun, args ...interface{}) {
    done := &sync.WaitGroup{}
    done.Add(1)
    go func() {
        done.Done()
        fun(args...)
    }()
    done.Wait()
}

type GoSelectFun func(args interface{})

func GoSelect(num int, fun GoSelectFun, goCall bool, c chan interface{}) {
    done := &sync.WaitGroup{}
    done.Add(num)
    for i := 0; i < num; i++ {
        go func() {
            done.Done()
            select {
            case data := <-c: {
                if goCall {
                    go fun(data)
                }else {
                    fun(data)
                }
            }
            }
        }()
    }
    done.Wait()
}


func Obj2Json(obj interface{}, pretty bool) string {
    if pretty {
        body, err := json.MarshalIndent(obj, "", "    ")
        if err != nil {
            log.Printf("\njsonObj:%v\n\n", obj)
            panic(err.Error())
            return ""
        }
        return string(body)
    }else {
        body, err := json.Marshal(obj)
        if err != nil {
            log.Printf("\njsonObj:%v\n\n", obj)
            panic(err.Error())
            return ""
        }
        return string(body)
    }
}

func Json2Ojb(jsonStr string, data interface{}) bool {
    //    var data interface{}
    err := json.Unmarshal([]byte(jsonStr), data)
    if err != nil {
        log.Printf("\njsonStr:%v\n\n", jsonStr)
        panic(err.Error())
        return false
    }
    return true
}


//Struct Att MUST be CanSet. Big prefix
func Struct2Map(obj interface{}) map[string]interface{} {
    return structs.Map(obj)
}

//Struct Att MUST be CanSet. Big prefix
func Map2Struct(data interface{}, obj interface{}) error {
    return mapstructure.Decode(data, obj)
}

func JsonStrEuqal(str1, str2 string) bool {
    map1 := make(map[string]interface{})
    map2 := make(map[string]interface{})
    Json2Ojb(str1, &map1)
    Json2Ojb(str2, &map2)

    return reflect.DeepEqual(map1, map2)
}




//func ByteBool(num uint32,t uint32)

//type GoFun func(ok chan <- bool)
//
//func GoDone(fun GoFun) {
//	ok := make(chan bool)
//	go fun(ok)
//	<-ok
//	close(ok)
//}
//
//done.Add(KListenGoroutineCount)
//for i := 0; i < KListenGoroutineCount; i++ {
//go tcp.listenAction(listener, done)
//}
//done.Wait()
//fmt.Printf("NewTcpSer start ok\n")


//func Map2Json(obj interface{}) string {
//    return Obj2Json(obj, false)
//}
//
//func Json2Map(jsonStr string) interface{} {
//    return Json2Ojb(jsonStr)
//}


//func Obj2JsonPretty(obj interface{}) string {
//    var body []byte
//    body, err := json.MarshalIndent(obj, "", "    ")
//    if err != nil {
//        log.Printf("\njsonObj:%v\n\n", obj)
//        panic(err.Error())
//        return ""
//    }
//    return string(body)
//}

//func Uint32ToBytes(length uint32)[]byte{
//	value:=length
//	headBuf := make([]byte, 4)
//	headBuf[3] = byte(value & 0xff)
//	value >>= 8
//	headBuf[2] = byte(value & 0xff)
//	value >>= 8
//	headBuf[1] = byte(value & 0xff)
//	value >>= 8
//	headBuf[0] = byte(value & 0xff)
//	return headBuf
//}
//func BytesToUint32(headBuf []byte)(uint32){
//	length := uint32(headBuf[0])<<24|uint32(headBuf[1])<<16|uint32(headBuf[2])<<8|uint32(headBuf[3])
//	return length
//}
