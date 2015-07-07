package main

import (
	"github.com/goraft/raft"
	"testing"
	"log"
	"fmt"
	"os"
	"runtime"
	"time"
	"sync"
	"math/rand"
	"github.com/stretchr/testify/assert"
	"strconv"
	"github.com/cheggaaa/pb"
	"github.com/izouxv/raftX/server"
	"github.com/izouxv/raftX/server/command"
	"github.com/izouxv/raftX/client"
	"github.com/izouxv/raftX/common/protobuf"
)

type DOT func()

var (
	kPath     = "/Users/zouxu/node.1"

	kMaxCli   = 50
	loopTimes = 100
	kIncPath  = "/abcd/123"
	kIncPathF = "/abcd/INC%d"
	kLockPath = "/abc/def"
	kGetPath  = "/abc/defhhh"
	kGetPathF = "/abc/defhasdfi%d"

	kGetAndWatch = "/abc/getAndWatch"
)

func TestClientGetAndWatch(t *testing.T) {

	initClient(t, kMaxCli, kMaxCli*loopTimes, func(c *client.Client, d DOT) {

			done := &sync.WaitGroup{}
			done.Add(loopTimes)

			pathArray := make(map[string]string)
			for i := 0; i < loopTimes; i++ {
				path := fmt.Sprintf("%v/%d", kGetAndWatch, i)
				pathArray[path] = path
				c.MkFile(path, path)
			}
			watchPath := kGetAndWatch + "/*"



			c.GetAndWatch(watchPath, true, func(key string, value string, verb protobuf.Verb, stopWatch* bool) {
			//		log.Printf("[%v:%v]", key, value)
					if _, ok := pathArray[key]; ok {
						delete(pathArray, key)
						done.Done()
						d()
					}
				})
			done.Wait()
			c.RemoveWatch(watchPath)
		}, func(c *client.Client, d DOT) {
		}, nil)
}

func TestClientGetSet(t *testing.T) {
	initClient(t, kMaxCli, kMaxCli*loopTimes, func(c *client.Client, d DOT) {

			pathItem := fmt.Sprintf(kGetPathF, c.Tag)
			c.MkFile(pathItem, "0")
			for i := 0; i < loopTimes; i++ {
				value := fmt.Sprintf("abc123:%d", i)
				respOneSet := c.Set(kGetPath, value)
				respOneGet := c.Get(kGetPath)
				assert.Equal(t, respOneSet.GetErrCode(), protobuf.Err_OK, "TestClientGetSet\n")
				assert.Equal(t, respOneGet.GetErrCode(), protobuf.Err_OK, "TestClientGetSet\n")

				respSet := c.Set(pathItem, value)
				respGet := c.Get(pathItem)
				assert.Equal(t, respSet.GetErrCode(), protobuf.Err_OK, "TestClientGetSet\n")
				assert.Equal(t, respGet.GetErrCode(), protobuf.Err_OK, "TestClientGetSet\n")
				assert.Equal(t, respGet.GetValue(), value, "TestClientGetSet\n")
				d()
			}
			respDel := c.Del(pathItem)
			assert.Equal(t, respDel.GetErrCode(), protobuf.Err_OK, "TestClientGetSet\n")
			respGet := c.Get(pathItem)
			assert.Equal(t, respGet.GetErrCode(), protobuf.Err_NOT_EXIST, "TestClientGetSet\n")
			respSet := c.Set(pathItem, "abc")
			assert.Equal(t, respSet.GetErrCode(), protobuf.Err_NOT_EXIST, "TestClientGetSet\n")
			//assert.Equal(t, respSet.GetErrCode(), Err_OK, "TestClientGetSet\n")

		}, func(c *client.Client, d DOT) {
			c.MkFile(kGetPath, "0")
		}, func(c *client.Client, d DOT) {
		})
}

func TestClientInc(t *testing.T) {
	initClient(t, kMaxCli, kMaxCli*loopTimes, func(c *client.Client, d DOT) {
			pathItem := fmt.Sprintf(kIncPathF, c.Tag)
			startNum := 100000
			c.Del(pathItem)
			c.CreateInt(pathItem, startNum)
			for i := 0; i < loopTimes; i++ {
				c.Inc(kIncPath)
				resp := c.Inc(pathItem)
				str := strconv.Itoa(i + startNum + 1)
				value := resp.GetValue()
				assert.Equal(t, value, str, "TestClientInc: %v[%v,%v]\n", pathItem, value, str)
				d()
			}
		}, func(c *client.Client, d DOT) {
			c.Del(kIncPath)
			c.CreateInt(kIncPath, 0)
		}, func(c *client.Client, d DOT) {
			resp := c.Get(kIncPath)
			value := resp.GetValue()
			str := strconv.Itoa(kMaxCli * loopTimes)
			assert.Equal(t, resp.GetValue(), strconv.Itoa(kMaxCli*loopTimes), "%v[%v,%v]\n", kIncPath, value, str)
		})
}

func TestClientWatch(t *testing.T) {
	initClient(t, kMaxCli, kMaxCli*loopTimes, func(c *client.Client, d DOT) {
			watchPath := fmt.Sprintf(kIncPathF, c.Tag)
			done := &sync.WaitGroup{}
			done.Add(loopTimes)
			c.Watch(watchPath, true, func(key string, value string, verb protobuf.Verb, stopWatch* bool) {
					if verb&protobuf.Verb_SET > 0 {
						done.Done()
						d()
					}
				})
			c.CreateInt(watchPath, 0)
			for i := 0; i < loopTimes; i++ {
				c.Inc(watchPath)
			}
			done.Wait()
			c.RemoveWatch(watchPath)
		}, func(c *client.Client, d DOT) {
		}, nil)
}
func TestClientLock(t *testing.T) {
	path := kLockPath
	initClient(t, kMaxCli, kMaxCli*loopTimes, func(c *client.Client, d DOT) {
			for i := 0; i < loopTimes; i++ {
				c.Lock(path)
				num := (rand.Int() % 100 + 1)
				time.Sleep(time.Duration(int(time.Microsecond) * num))
				c.ULock(path)
				d()
			}
		}, func(c *client.Client, d DOT) {
			c.ULock(path)
		}, nil)
}

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////
/////////////////////// INIT ///////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////
func init() {
	log.Printf("LogInit")
	runtime.GOMAXPROCS(runtime.NumCPU())
	startSer()
	time.Sleep(time.Second * 3)
}

func startSer() {

	// Setup commands.
	raft.RegisterCommand(&command.SetCommand{})
	raft.RegisterCommand(&command.RemoveTmpDataCommand{})

	path := kPath
	if err := os.MkdirAll(path, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}

	log.SetFlags(log.LstdFlags)
	s := server.New(path, host, port)

	addr := fmt.Sprintf("%s:%d", host, tcpSerPort)

	s.ListenAndServe(join, addr)
}

type CB func(c*client.Client, d DOT)

func initClient(t* testing.T, maxClient int, count int, testCB CB, startCB CB, endCB CB) {
	lock := &sync.Mutex{}
	var clis []*client.Client
	done := &sync.WaitGroup{}
	done.Add(maxClient)
	bar := initProgress(count)

	incBar := func() {
		bar.Increment()
	}

	for i := 0; i < maxClient; i++ {
		num := i
		go func() {
			addr := fmt.Sprintf("%s:%d", host, tcpSerPort)
			addrs := []string{addr}
			cli := client.New(addrs)
			cli.Start()
			cli.Tag = num
			lock.Lock()
			defer lock.Unlock()
			clis = append(clis, cli)
			done.Done()
		}()
	}
	done.Wait()

	assert.Equal(t, len(clis), maxClient, "initCLiErr\n")

	done.Add(maxClient)
	for i := 0; i < len(clis); i++ {
		num := i
		go func() {
			if startCB != nil {
				startCB(clis[num], incBar)
			}
			done.Done()
		}()
	}
	done.Wait()

	done.Add(maxClient)
	for i := 0; i < len(clis); i++ {
		num := i
		go func() {
			testCB(clis[num], incBar)
			done.Done()
		}()
	}
	done.Wait()

	done.Add(maxClient)
	for i := 0; i < len(clis); i++ {
		num := i
		go func() {
			if endCB != nil {
				endCB(clis[num], incBar)
			}
			done.Done()
		}()
	}
	done.Wait()

	for i := 0; i < len(clis); i++ {
		c := clis[i]
		c.Close()
	}
	bar.Finish()

	//	bar.FinishPrint("The End!")
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
