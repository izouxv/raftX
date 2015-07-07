package util

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "fmt"
    "strconv"
    "github.com/lunny/log"
)


type MyData struct {
    Item   string    `json:"item"`
    Amount  float32   `json:"amount"`
}

func Test_JsonMap(t *testing.T) {
    data := make(map[string]interface{})
    data["Item"] = "abc"
    data["Amount"] = 123


    myData := &MyData{}
    Map2Struct(data, myData)
    ddd := Struct2Map(myData)
    assert.Equal(t, data, ddd)
    log.Printf("m: %v\n", ddd)
}

func Test_numToByte(t *testing.T) {
    var n uint32
    for i := 0; i < 100000; i++ {
        n = RandInt(1000000)
        b := Uint32ToBytes(n)
        num := BytesToUint32(b)
        assert.Equal(t, n, num)
    }
}

//func Test_GoDone(t *testing.T) {
//	call:=make(chan bool)
//	GoDone(func(ok chan<- bool){
//		ok<-true
//		call<-true
//	})
//	<-call
//}

func Benchmark_Crc32(b *testing.B) {
    ip := "192.168.1.1:5000"
    for i := 0; i < b.N; i++ { //use b.N for looping
        Crc32([]byte(ip))
    }
}




func Test_GoSelect(t*testing.T) {
    return
    kMaxNum := 10000000
    ok := make(chan bool)
    c := make(chan interface{}, 1000)
    goSelect := func(args interface{}) {
        data := args.(string)
        fmt.Printf("Test_GoSelect data: %v", data)
        i, _ := strconv.Atoi(data)
        if i == kMaxNum-1 {
            ok <- true
        }
    }
    GoSelect(2, goSelect, true, c)

    for i := 0; i < kMaxNum; i++ {
        str := fmt.Sprintf("%d", i)
        c <- str
    }
    <-ok
    fmt.Printf("OK\n")

}
