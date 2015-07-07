package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"strconv"
	"time"
	"math/rand"
	"log"
	"encoding/json"
	."fish.red/zouxu/goall/raftx/common/protobuf"
	"sync"
	"runtime"
)

var testGetPatPatList = []string{
	"/a/**",
	"/a/b*/c*",
	"/a/b?/c?",
	"/a/?/c?",
}

var testGetPatValueList = []string{
	"/a/b1",
	"/a/b2/c1",
	"/a/b3/c1",
	"/a/b2/c3",
	"/a/b34/c1",
	"/a/b/c3",
}

var testDirList = []string{
	"/a",
	"/a/b1",
	"/a/b2/c1",
	"/a/b3/c1",
	"/a/b2/c3",
	"/a/b34/c1",
	"/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1/a/b34/c1",
	"/a/b/c3",
}

const (
	kABCXYZ = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)
const (
	TempDataId = "localhost:9999-abcdf"
)

//var kFsPath = "/Users/zouxu/testDir"

type TestType int

const (
	TestTypeGet TestType = iota
	TestTypeDel
)

func TestDir(t *testing.T) {
	loadDirData(t, testDirList, func(fs* Fs) {
			//	fmt.Printf("%v", fs.Encode())
		})
}

func TestGetRoot(t *testing.T) {
	loadData(t, testGetPatValueList, func(fs* Fs) {
			_, _, err1 := fs.Get("/**", true)
			//log.Printf("\n\n\nroot1:\n%v:\n\n%v\n\n", root1)
			assert.Equal(t, err1, Err_OK)
		})
}

func byteToJson(bytes[]byte) interface{} {
	var obj interface{}
	err := json.Unmarshal(bytes, &obj)
	if err != nil {
		//fmt.Printf("setFsData: \n%v\n\n\n", string(bytes))
		panic(err.Error())
		return err
	}
	return obj
}

func TestGetFsData(t *testing.T) {

	loadData(t, testGetPatValueList, func(fs1* Fs) {
			data1, err1 := fs1.GetFsData()
			assert.Equal(t, err1, Err_OK)

			fs2 := NewFs(nil)
			err2 := fs2.SetFsData(data1)
			assert.Equal(t, err2, Err_OK)
			data2, err20 := fs2.GetFsData()
			assert.Equal(t, err20, Err_OK)
			assert.Equal(t, byteToJson(data2), byteToJson(data1), "\n\n%v\n\n%v\n\n\n", string(data2), string(data1))

			root1, _, err100 := fs1.Get("/**", true)
			root2, _, err200 := fs2.Get("/**", true)

			assert.Equal(t, err200, err100)
			assert.Equal(t, root1, root2)

			if false {
				fs1.RemoveTempData(TempDataId)
				fs2.RemoveTempData(TempDataId)

				root1, _, err100 := fs1.Get("/**", true)
				root2, _, err200 := fs2.Get("/**", true)

				assert.Equal(t, err200, err100)
				assert.Equal(t, root1, root2)
			}

			log.Printf("end SnapShot\n\n\n")
		})
}



func TestMaxItem(t *testing.T) {
	var cur int
	max := 1000000 * 100
	//max = 1000000

	lock := &sync.Mutex{}

	loadData(t, testGetPatValueList, func(fs1* Fs) {
			done := &sync.WaitGroup{}
			done.Add(max)
			for i := 0; i < 8; i++ {
				go func() {
					for {
						path := randPath()
						value := randValue(100)
						//path = "/ab/ce/ds/"+value

						lock.Lock()
						if cur >= max {
							lock.Unlock()
							return
						}
						fs1.Create(path, value, Verb_SET|Verb_CREATE, "")
						cur++
						if cur%1000000 == 0 {
							log.Printf("item: %d  [%d:%d]\n", cur, fs1.DirNum(), fs1.FileNum())
						}
						lock.Unlock()

						done.Done()
					}
				}()
			}
			done.Wait()
		})
}


func TestGetMut(t *testing.T) {

	//test get set mutGet inc
	loadData(t, testGetPatValueList, func(fs* Fs) {

			testItem(t, fs, TestTypeGet, 0, []int{0, 1, 2, 3, 4, 5, })
			testItem(t, fs, TestTypeGet, 1, []int{1, 2, 3, 4, 5, })
			testItem(t, fs, TestTypeGet, 2, []int{1, 2, 3, })
			testItem(t, fs, TestTypeGet, 3, []int{5, })

			kIncPath := "/inc/b/inc"
			fs.Create(kIncPath, "0", Verb_NUMBER, "")
			for i := 0; i < 100; i++ {
				value, err := fs.Add(kIncPath, "1")
				assert.Equal(t, Err_OK, err)
				assert.Equal(t, value, strconv.Itoa(i+1))
			}

			delPath := testGetPatValueList[0]
			ret := fs.Del(delPath)
			assert.Equal(t, Err_OK, ret, "%v", delPath)
			value, _, err2 := fs.Get(delPath, false)
			assert.Equal(t, Err_NOT_EXIST, err2, "%v - %v", delPath, value)

		})

	//test mutdel
	loadData(t, testGetPatValueList, func(fs* Fs) {
			testItem(t, fs, TestTypeDel, 0, []int{0, 1, 2, 3, 4, 5, })
		})
	loadData(t, testGetPatValueList, func(fs* Fs) {
			testItem(t, fs, TestTypeDel, 1, []int{1, 2, 3, 4, 5, })
		})
	loadData(t, testGetPatValueList, func(fs* Fs) {
			testItem(t, fs, TestTypeDel, 2, []int{1, 2, 3, })
		})
	loadData(t, testGetPatValueList, func(fs* Fs) {
			testItem(t, fs, TestTypeDel, 3, []int{5, })
		})
}

func testItem(t *testing.T, fs* Fs, testType TestType, getDelPatIndex int, getDelListIndex []int) {
	switch testType{
	case TestTypeGet, TestTypeDel: {
		pat := testGetPatPatList[getDelPatIndex]
		value := make(map[string]string)
		var err Err

		var pathDir map[string]interface{}
		if testType == TestTypeDel {
			pathDir, err = fs.DelWithPat(pat)
		}else if testType == TestTypeGet {
			pathDir, err = fs.GetWithPat(pat)
		}

		for k, v := range pathDir {
			filesMap := v.(map[string]*File)
			for name, file := range filesMap {
				//fs.notifyValueChange(true, k+name, file.Value, false)
				value[k+name] = file.Value
			}
		}

		assert.Equal(t, Err_OK, err, "path: %v", pat)
		assert.Equal(t, len(value), len(getDelListIndex), "%v:%v", getDelPatIndex, getDelListIndex)

		for _, v := range getDelListIndex {
			path := testGetPatValueList[v]
			_, ok := value[path];
			assert.Equal(t, true, ok)

			if testType == TestTypeDel {
				value, _, err2 := fs.Get(path, false)
				assert.Equal(t, Err_NOT_EXIST, err2, "%v:%v", path, value)
			}
		}
	}
	}
}

func loadData(t *testing.T, data []string, fn func(fs *Fs)) {
	fs := NewFs(nil)

	rand.Seed(time.Now().UnixNano())
	for _, path := range data {
		data := strconv.Itoa(rand.Int())
		str := ""
		if rand.Int()%2 == 0 {
			str = TempDataId
		}
		err := fs.Create(path, data, Verb_PERSISTENT, str)
		assert.Equal(t, Err_OK, err)
		value, _, err2 := fs.Get(path, false)
		assert.Equal(t, Err_OK, err2)
		assert.Equal(t, value, data)
	}
	fn(fs)
}

func loadDirData(t *testing.T, data []string, fn func(fs *Fs)) {
	fs := NewFs(nil)

	rand.Seed(time.Now().UnixNano())
	for _, path := range data {
		data := strconv.Itoa(rand.Int())
		suc := fs.MkDir(path, data)
		assert.Equal(t, Err_OK, suc)
		ret , _ := fs.IsDir(path)
		assert.Equal(t, true, ret, "path: %v", path)
	}
	fn(fs)
}


func randPath() string {
	maxDirLen := 4
	maxDirNameLen := 1
	maxFileNameLen := 10

	path := ""
	dirLen := rand.Int() % maxDirLen + 1
	for i := 0; i < dirLen; i++ {
		pathNameLen := rand.Int() % maxDirNameLen + 1
		if i == dirLen-1 {
			pathNameLen = rand.Int()%maxFileNameLen+1
		}
		path = path+"/"+randValue(pathNameLen)
	}
	return path
}


func randValue(length int) string {
	str := ""
	azLen := len(kABCXYZ)
	for i := 0; i < length; i++ {
		pathNum := rand.Int() % azLen
		c := kABCXYZ[pathNum:pathNum+1]
		//	log.Printf("c: %v\n", c)
		str = str+c
	}
	return str
}


func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}





























