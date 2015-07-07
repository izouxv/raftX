package request

import (
	"sync/atomic"
	"fmt"
	"os"
	"runtime/debug"
	."github.com/izouxv/raftX/common/protobuf"
)

var IncreateTag uint64

const (
	KGetMyName   = "getMyName" //userId
	KGetHeartBeat   = "getHeartBeat" //userId
	KSerListPath = "/raftX/*"  //userId
)

func Tag() *string {
	num := atomic.AddUint64(&IncreateTag, 1)
	str := fmt.Sprintf("%d", num)
	return &str
}

func NewReq(path string, value string, verb uint32) *Request {

	if len(path) == 0 {
		debug.PrintStack()
		os.Exit(1)
	}

	return &Request{
		Verb: &verb,
		Path:&path,
		Tag:Tag(),
		Value:&value,
	}
}


func NewRespWatch(path string, value string, tag string, cnt string, updateVerb Verb) *Response {
	verb := uint32(updateVerb | Verb_WATCH_PUSH)
	err := Err_OK
	errDetail := Err_name[int32(err)]

	if len(path) == 0 {
		debug.PrintStack()
		os.Exit(1)
	}

	return &Response{
		Tag:&tag,
		Verb: &verb,
		Path:&path,
		Value:&value,
		Cnt:&cnt,
		ErrCode:&err,
		ErrDes:&errDetail,
	}

}

func NewRespByReq(req *Request, value string, err Err) *Response {
	verb := req.GetVerb()
	path := req.GetPath()
	tag := req.GetTag()
	errDetail := Err_name[int32(err)]

	if len(path) == 0 {
		debug.PrintStack()
		os.Exit(1)
	}

	return &Response{
		Tag:&tag,
		Verb: &verb,
		Path:&path,
		Value:&value,
		ErrCode:&err,
		ErrDes:&errDetail,
	}
}

type ClientRequestItem struct {
	Req *Request
	ReqChan chan *Response
}

type ServerRequestItem struct {
	Req *Request
	Addr string
}

type ServerResponceItem struct {
	Resp *Response
	Addr string
}


