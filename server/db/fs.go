package db

import (
	"sync"
	"regexp"
	"strings"
	"fmt"
	"encoding/json"
	//"log"
	"strconv"
	."github.com/izouxv/raftX/common/protobuf"
	."github.com/izouxv/raftX/server/glob"
)

//可以考虑获取dir[]＋file的方式来做
//var Any = MustCompileGlob("/**")

const charPat = `[a-zA-Z0-9.\-]`

var pathRe = mustBuildRe(charPat)

type OpType int

const (
	OpMkDirAll OpType = iota
	OpMkAutoMkDirAndLastFile
	OpGet
	OpDel
	OpPatDel
	OpPatGet
)

type ActType uint32

const (
	kTypeIdle ActType = iota
	kTypeExist
	kTypeCreate
)

type FsNotify func( key string, value string, tag string, verb Verb, ip string, watchPath string)

func BBOOL(TYPE Verb, verb Verb) bool {
	return verb&TYPE > 0
}

func mustBuildRe(p string) *regexp.Regexp {
	return regexp.MustCompile(`^/$|^(/` + p + `+)+$`)
}
func checkPath(k string) Err {
	if !pathRe.MatchString(k) {
		return Err_BAD_PATH
	}
	return Err_OK
}
func split(path string) []string {
	if path == "/" {
		return []string{}
	}
	return strings.Split(path[1:], "/")
}
func getFolder(path string) ([]string, Err) {
	if err := checkPath(path); err != Err_OK {
		fmt.Printf("errFolder: [%v]\n", path)
		return nil, err
	}
	return split(path), Err_OK
}

type File struct {
	Value string `json:"value"`
	Name  string  `json:"name"`
	Attr  Verb `json:"attr"`
	superDir*     Dir
}

func NewFile(name string , value string) *File {
	return &File{
		Name:name,
		Value:value,
	}
}

func (file *File) AddFileNum(num int) {
	file.superDir.AddFileNum(num)
}

func (file* File) RemoveFromDir() {
	if file.superDir != nil {
		file.superDir.removeFile(file.Name)
		file.AddFileNum(-1)
		file.superDir = nil
	}
}


func (file *File) GetPath() string {
	return file.superDir.Abs + file.Name
}

func (file *File) setDir(dir* Dir) {
	file.superDir = dir
	file.AddFileNum(1)
}

func NewDir(abs string, value string) *Dir {
	return &Dir{
		Files:make(map[string]interface{}),
		Abs:abs,
		Value:value,
		watchs:make(map[string]int),
	}
}

//watch is only for files, not for dir
type Dir struct {
	fileNum        int
	dirNum         int
	superDir*    Dir
	Value          string `json:"value"`
	Abs            string `json:"abs"`
	Attr           Verb `json:"attr"`
	Files    map[string]interface{} `json:"files"`
	watchs   map[string]int //`json:"wat"`

	watchFile map[string]string
	watchDir map[string]string
}

func (dir *Dir) setDir(superdir* Dir) {
	dir.superDir = superdir
	superdir.AddDirNum(1)
}

func (dir *Dir) AddFileNum(num int) {
	dir.fileNum+=num
	if dir.superDir != nil {
		dir.superDir.AddFileNum(num)
	}
}
func (dir *Dir) AddDirNum(num int) {
	dir.dirNum+=num
	if dir.superDir != nil {
		dir.superDir.AddDirNum(num)
	}
}

func (dir *Dir) removeFile(name string) bool {
	if _, ok := dir.Files[name]; ok {
		delete(dir.Files, name)
		return true
	}
	return false
}

func (dir *Dir) Set(path string, value string) Err {

	folders, err := getFolder(path)
	if err != Err_OK {
		return err
	}

	fd, err, _ := dir.setDirOrFile(folders, OpGet, nil)

	switch fd.(type) {
	case *File: {
		file := fd.(*File)

		if BBOOL(file.Attr, Verb_NUMBER) /*|| attr&KSignInt > 0*/ {
			num, err := strconv.Atoi(value) //value.(uint64)
			if err != nil {
				return Err_DATA_MUST_BE_NUMBER
			}
			if num < 0 {
				return Err_UNSIGNINT_ERR
			}
		}
		file.Value = value
		return Err_OK
	}
	case *Dir: {
		d := fd.(*Dir)
		d.Value = value
		return Err_OK
	}
	}
	return Err_NOT_EXIST
}

func (dir *Dir) Create(path string, value string, attr Verb) Err {

	if attr&Verb_NUMBER > 0 {
		num, err := strconv.Atoi(value) //value.(uint64)
		if err != nil {
			return Err_DATA_MUST_BE_NUMBER
		}
		if num < 0 {
			return Err_UNSIGNINT_ERR
		}
	}

	//fmt.Printf("Set: path: %v  \n", path)
	folders, err := getFolder(path)
	if err != Err_OK {
		return err
	}

	fd, err, act := dir.setDirOrFile(folders, OpMkAutoMkDirAndLastFile, nil)
	if act == kTypeExist {
		return Err_EXIST
	}
	if act != kTypeCreate {
		return Err_UNKNOWN
	}

	switch fd.(type) {
	case *File: {
		file := fd.(*File)

		file.Attr = attr
		file.Value = value
	}
	case *Dir: {
		d := fd.(*Dir)
		d.Value = value
		d.Attr = attr
	}
	}
	return err;
}


func (dir *Dir) MkDir(path string, value string) (Err) {
	//fmt.Printf("Set: path: %v  \n", path)
	folders, err := getFolder(path)
	if err != Err_OK {
		return err
	}

	fd, err, act := dir.setDirOrFile(folders, OpMkDirAll, nil)

	if act == kTypeExist {
		return Err_EXIST
	}
	//fmt.Printf("SetRet: path: %v - %v\n", fd, err)
	switch fd.(type) {
	case *Dir: {
		d := fd.(*Dir)
		d.Value = value
		//fmt.Printf("SetDir: path: %v - %v\n", path, folders)
		return Err_OK
	}
	}
	return err;
}

func (dir *Dir) IsDir(path string) (bool, Err) {

	folders, err := getFolder(path)
	if err != Err_OK {
		return false, err
	}

	fd, err, _ := dir.setDirOrFile(folders, OpGet, nil)

	//fmt.Printf("SetRet: path: %v - %v\n", fd, err)
	switch fd.(type) {
	case *Dir: {
		//fmt.Printf("SetDir: path: %v - %v\n", path, folders)
		return true, Err_OK
	}
	}
	return false, err;

}


func (dir *Dir) GetWithPat(path string) (map[string]interface{}, Err) {
	//TODO: 需要鉴别path pation合法性
	folders := split(path)
	datas := make(map[string]interface{})
	_, err, _ := dir.setDirOrFile(folders, OpPatGet, datas)

	//	fmt.Printf("getWithPath: %v\n\n\n%v\n%v\n\n\n\n\n", path, datas, err)
	return datas, err
}

func (dir *Dir) Add(path string, add int) (string, Err) {

	folders, err := getFolder(path)
	if err != Err_OK {
		return "", err
	}

	fd, err, act := dir.setDirOrFile(folders, OpGet, nil)
	if err != Err_OK {
		return "", err
	}
	if act != kTypeExist {
		return "", Err_NOT_EXIST
	}

	switch fd.(type) {
	case *File: {
		file := fd.(*File)

		if file.Attr&Verb_NUMBER == 0 /*&& file.Attr&KSignInt == 0 */ {
			return "", Err_FILE_ATTR_NOT_IS_INCREASE
		}

		num, err := strconv.Atoi(file.Value) //value.(uint64)
		if err != nil {
			fmt.Printf("IncError: path: %v:%v , it will reset to 0 \n", path, file.Value)
			return file.Value, Err_FILE_ATTR_NOT_IS_INCREASE
		}

		if add+num < 0 && file.Attr&Verb_NUMBER > 0 {
			return file.Value, Err_RET_LESS_OF_ZERO
		}
		num+=add

		file.Value = strconv.Itoa(num)
		return file.Value, Err_OK
	}
	}
	return "", Err_NOT_EXIST
}

func (dir *Dir) Get(path string) (string, Verb, Err) {

	folders, err := getFolder(path)
	if err != Err_OK {
		return "", Verb_NONE, err
	}

	fd, err, _ := dir.setDirOrFile(folders, OpGet, nil)
	switch fd.(type) {
	case *File: {
		file := fd.(*File)
		return file.Value, file.Attr, Err_OK
	}
	case *Dir: {
		d := fd.(*Dir)
		return d.Value, d.Attr, Err_OK
	}
	}
	return "", Verb_NONE, Err_NOT_EXIST
}

func (dir *Dir) DelWithPat(path string) (map[string]interface{} , Err) {
	//folders, err := getFolder(path)
	//TODO: 需要鉴别path pation合法性
	folders := split(path)

	datas := make(map[string]interface{})
	_, err, _ := dir.setDirOrFile(folders, OpPatDel, datas)

	for _, v := range datas {
		filesMap := v.(map[string]*File)
		for _, file := range filesMap {
			file.RemoveFromDir()
		}
	}

	return datas, err
}

func (dir *Dir) Del(path string) (  Err) {

	folders, err := getFolder(path)
	if err != Err_OK {
		fmt.Printf("path format is error\n")
		return err
	}
	fd, err, _ := dir.setDirOrFile(folders, OpMkAutoMkDirAndLastFile, nil)
	switch fd.(type) {
	case *File: {
		file := fd.(*File)
		file.RemoveFromDir()
		return Err_OK
	}
	case *Dir: {
		fmt.Printf("dir can't be remove\n")
		return Err_DIR_CANT_REMOVE
	}
	}
	return Err_NOT_EXIST
}

// The key-value database.
type Fs struct {
	//watch 需要再搞个watchRoot
	Root  *Dir
	mutex  sync.RWMutex
	watch_Ip_PathArray   map[string]map[string]string
	watch_Path_Ip_Tag   map[string]map[string] string
	watchImmediate_Path_Ip_Tag   map[string]map[string] string
	notify FsNotify
	tempData map[string]interface{} //[id-server][pathArray]//for client disconnect , delete the temp data

	//	path string
	//	Ver uint64
}

func (fs *Fs) Dir() (*Dir) {
	return fs.Root
}

// Creates a new database.
func NewFs(notify FsNotify) *Fs {
	//	os.Mkdir(path, os.ModePerm)

	return &Fs{
		Root: NewDir("/", ""),
		watch_Ip_PathArray:make(map[string]map[string]string),
		watch_Path_Ip_Tag:make(map[string]map[string] string),
		watchImmediate_Path_Ip_Tag:make(map[string]map[string] string),
		tempData:make(map[string]interface{}), //{clientID:[{path:[bool]}]}
		notify:notify,
		//		path:path + "/fileSystem",
	}

}

//// Retrieves the value for a given key.
//func (fs *Fs) Get(key string) (string, error) {
//	fs.mutex.RLock()
//	defer fs.mutex.RUnlock()
//	return fs.Root.Get(key)
//}

//func (fs *Fs) GetWithPatStr(key string) (string, error) {
//	obj, err := fs.GetWithPat(key)
//	if err == nil {
//		data , err := json.Marshal(obj)
//		if err == nil {
//			return string(data), err
//		}
//	}
//	return "", err
//}
//

//// Retrieves the value for a given key.
func (fs *Fs) GetWithPat(key string) (map[string]interface{}, Err) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()
	ret, err := fs.Root.GetWithPat(key)
	return ret, err
}

// Retrieves the value for a given key.
func (fs *Fs) Get(key string, pat bool) (string, Verb, Err) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()
	if pat {
		//obj, err := fs.GetWithPat(key)
		obj, err := fs.Root.GetWithPat(key)
		if err == Err_OK {
			data , err := json.Marshal(obj)
			if err != nil {
				return "", Verb_NONE, Err_JSON_MARSHAL
			}
			return string(data), Verb_GET, Err_OK
		}
	}else {
		return fs.Root.Get(key)
	}
	//log.Printf("GetErr: Key:%v Pat:%v", key, pat)
	return "", Verb_NONE, Err_NOT_EXIST
}

func (fs* Fs) RemoveTempData(tempDataId_IP string) {
	//log.Printf("ip: %v, tempData: %v", tempDataId_IP, fs.tempData)
	if paths, ok := fs.tempData[tempDataId_IP]; ok {
		pathArray := paths.(map[string]int8)
		for kPath, _ := range pathArray {
			fs.Del(kPath)
		}
		delete(fs.tempData, tempDataId_IP)
	}
}

func (fs *Fs) setTempIndex(key string, tempDataId string) {
	paths, ok := fs.tempData[tempDataId]
	if !ok {
		paths = make(map[string]int8)
		fs.tempData[tempDataId] = paths
	}
	pathArray := paths.(map[string]int8)
	pathArray[key] = 0
}


func (fs *Fs) Create(key string, value string, attr Verb, tempDataId_IP string) Err {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if BBOOL(attr, Verb_EPHEMERAL) && len(tempDataId_IP) > 0 {
		fs.setTempIndex(key, tempDataId_IP)
	}

	err := fs.Root.Create(key, value, attr)
	fs.notifyValueChange(err == Err_OK, key, value, Verb_CREATE)
	return err
}

// Sets the value for a given key.
func (fs *Fs) Set(key string, value string) Err {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	err := fs.Root.Set(key, value)
	fs.notifyValueChange(err == Err_OK, key, value, Verb_SET)
	return err
}

// Sets the value for a given key.
func (fs *Fs) MkDir(key string, value string) (  Err) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	err := fs.Root.MkDir(key, value)

	fs.notifyValueChange(err == Err_OK, key, value, Verb_CREATE|Verb_DIR)

	return err
}

// Sets the value for a given key.
func (fs *Fs) IsDir(key string) (bool, Err) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	return fs.Root.IsDir(key)
}

//Inc: fist set k:v with Inc Attr, and then call inc
// Sets the value for a given key.
func (fs *Fs) Add(key string, numStr string) (string, Err) {
	num, err := strconv.Atoi(numStr) //value.(uint64)
	if err != nil {
		return numStr, Err_DATA_MUST_BE_NUMBER
	}

	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	str, err1 := fs.Root.Add(key, num)
	if err1 == Err_OK {
		fs.notifyValueChange(true, key, str, Verb_SET)
	}
	return str, err1
}

// Sets the value for a given key.
func (fs *Fs) Del(key string) (  Err) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	err := fs.Root.Del(key)
	if err == Err_OK {
		fs.notifyValueChange(true, key, "", Verb_DEL)
	}
	return err
}

func (fs *Fs) DirNum() ( int) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	return fs.Root.dirNum
}

func (fs *Fs) FileNum() ( int) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	return fs.Root.fileNum
}

func (fs *Fs) DelWithPatStr(key string) (string, Err) {
	obj, err := fs.DelWithPat(key)
	if err == Err_OK {
		data , err := json.Marshal(obj)
		if err != nil {
			return "", Err_JSON_MARSHAL
		}
		return string(data), Err_OK
	}
	return "", Err_UNKNOWN
}

// Sets the value for a given key.
func (fs *Fs) DelWithPat(key string) (map[string]interface{}, Err) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	ret, err := fs.Root.DelWithPat(key)
	if err == Err_OK {
		for k, v := range ret {
			filesMap := v.(map[string]*File)
			for name, file := range filesMap {
				fs.notifyValueChange(true, k+name, file.Value, Verb_DEL)
			}
		}
	}
	return ret, err
}

func (fs *Fs) notifyValueChange(notify bool, key string, value string, verb Verb) {
	if !notify {
		return
	}
	//return

	for patPath, ips := range fs.watch_Path_Ip_Tag {
		matchOk := false
		if patPath == key {
			matchOk = true
		}else {
			glob, err := CompileGlob(patPath)
			if (err != nil) {
				fmt.Printf("getFile error: path: %v\n", patPath)
				return
			}
			matchOk = glob.Match(key)
		}
		//matchOk = true


		if matchOk {
			for ip, tag := range ips {
				fs.notify(key, value, tag, verb, ip, patPath)
			}
		}
	}

	for patPath, ips := range fs.watchImmediate_Path_Ip_Tag {
		matchOk := false
		if patPath == key {
			matchOk = true
		}else {
			glob, err := CompileGlob(patPath)
			if (err != nil) {
				fmt.Printf("getFile error: path: %v\n", patPath)
				return
			}
			matchOk = glob.Match(key)
		}
		//matchOk = true

		if matchOk {
			for ip, tag := range ips {
				fs.notify(key, value, tag, verb, ip, patPath)
			}
		}
	}
}

// Sets the value for a given key.
func (fs *Fs) SetWatch(path string, ip string, tag string, immediate bool) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	//	fs.data[key] = value

	pathArray, ok := fs.watch_Ip_PathArray[ip];
	if !ok {
		pathArray = make(map[string]string)
		fs.watch_Ip_PathArray[ip] = pathArray
	}
	pathArray[path] = path

	if immediate {
		if recvChan, ok := fs.watchImmediate_Path_Ip_Tag[path]; ok {
			recvChan[ip] = tag
		}else {
			recvChan := make(map[string]string)
			recvChan[ip] = tag
			fs.watchImmediate_Path_Ip_Tag[path] = recvChan
		}
	}else {
		if recvChan, ok := fs.watch_Path_Ip_Tag[path]; ok {
			recvChan[ip] = tag
		}else {
			recvChan := make(map[string]string)
			recvChan[ip] = tag
			fs.watch_Path_Ip_Tag[path] = recvChan
		}
	}
	//fmt.Printf("SetWatch\n")
}
func (fs *Fs) DelWatchAllWithIp(ip string) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	if pathArray, ok := fs.watch_Ip_PathArray[ip]; ok {
		for k, _ := range pathArray {
			fs.delWatch(k, ip)
		}
		delete(fs.watch_Ip_PathArray, ip)
	}
}

func (fs *Fs) DelWatch(path string, ip string) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	fs.delWatch(path, ip)
}

// Sets the value for a given key.
func (fs *Fs) delWatch(path string, ip string) {

	if recvChan, ok := fs.watch_Path_Ip_Tag[path]; ok {
		delete(recvChan, ip)
	}
	if recvChan, ok := fs.watchImmediate_Path_Ip_Tag[path]; ok {
		delete(recvChan, ip)
	}
}

func (fs *Fs) Apply(path string , value string , verb uint32, addr string) (interface{}, error) {

	opCreate := verb&uint32(Verb_CREATE) > 0
	opSet := verb&uint32(Verb_SET) > 0
	opDel := verb&uint32(Verb_DEL) > 0
	opDir := verb&uint32(Verb_DIR) > 0
	opEphemeral := verb&uint32(Verb_EPHEMERAL) > 0
	opNumber := verb&uint32(Verb_NUMBER) > 0
	opPat := strings.ContainsAny(path, "*?")

	err := Err_UNKNOWN
	//	var ret interface{}
	ret := ""

	if opCreate {
		if opDir {//create dir
			err = fs.MkDir(path, value)
		}else {//create file
			//	fmt.Printf("Create: [%v:%v]\n", path, value)
			err = fs.Create(path, value, Verb(verb), addr)
		}
	} else if opDel {
		if opPat {
			ret, err = fs.DelWithPatStr(path)
		}else {
			err = fs.Del(path)
		}
	}else if opCreate {
		if opEphemeral {
			err = fs.Create(path, value, Verb(verb), addr)
		}else if opNumber {
			err = fs.Create(path, value, Verb(verb), addr)
		}else {
			err = fs.Create(path, value, Verb(verb), addr)
		}
	}else if opSet {
		if opNumber {
			ret, err = fs.Add(path, value)
			//	fmt.Printf("opAdd: %v\n", ret)
		}else {
			err = fs.Set(path, value)
		}
		//	fmt.Printf("opSet: %v  %v\n", err, path)
	}
	if err == Err_UNKNOWN || err != Err_OK {
		//fmt.Printf("ApplyErr  %v, [%v:%v]  %v\n ", err, path, value, verb)
	}



	return map[string]interface{}{"ret":ret, "err":err}, nil
}

// Sets the value for a given key.
func (fs *Fs) Encode() string {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	body, err := json.Marshal(fs)
	if err != nil {
		panic(err.Error())
	}
	return string(body)
}

func (fs *Fs) GetFsData() ([]byte, Err) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	//[dirPath]{[fileName]value}
	datas, err := fs.GetWithPat("/**")
	if err != Err_OK {
		return nil, err
	}

	fsData := map[string]interface{}{"data":datas, "tempData":fs.tempData}
	//fmt.Printf("GetFsData: \n%v\n\n\n", fsData)

	body, errJson := json.Marshal(fsData)
	if errJson != nil {
		panic(errJson.Error())
		return nil, Err_JSON_MARSHAL
	}

	//TODO: need add tempArray
	return body, Err_OK
}

func (fs *Fs) SetFsData(bytes []byte) Err {
	//fmt.Printf("\nSetFsData: %d\n", len(bytes))

	var fsData map[string]interface{}
	err := json.Unmarshal(bytes, &fsData)
	if err != nil {
		fmt.Printf("setFsData: \n%v\n\n\n", string(bytes))
		panic(err.Error())
		return Err_JSON_MARSHAL
	}

	//restore from
	tempDataJson := fsData["tempData"]
	tempData := tempDataJson.(map[string]interface{})
	for tempDataId, paths := range tempData {
		pathArray, ok := paths.(map[string]interface{})
		if !ok {
			//fmt.Printf("pathArrayEEEEEERRRRRRRR  %v: \n%v\n%v\n\n", tempDataId, pathArray, paths)
			//	fmt.Printf("tempData: \n%v\n\n\n", tempData)
		}
		if ok {
			for path, _ := range pathArray {
				fs.setTempIndex(path, tempDataId)
			}
		}
	}

	data := fsData["data"].(map[string]interface{})
	for k, v := range data {
		filesMap, _ := v.(map[string]interface{})
		for name, file := range filesMap {

			fileMap := file.(map[string]interface{})
			//		fmt.Printf("file: \n%v\n\n\n", file)
			//		fmt.Printf("fileMap[value]: \n%v\n\n\n", fileMap["value"])
			path := k + name
			value := fileMap["value"].(string)
			attrNum := fileMap["attr"].(float64)
			attr := Verb(attrNum)
			fs.Create(path, value, attr, "")
		}
	}
	return Err_OK
}


func (dir *Dir) getAllDirOrFile(fd interface{}, pat map[string]interface{}) {
	switch fd.(type) {
	case *File: {
		file := fd.(*File)
		dirPath := file.superDir.Abs
		fileArray, ok := pat[dirPath].(map[string]*File)
		if !ok {
			fileArray = make(map[string]*File)
			pat[dirPath] = fileArray
		}
		fileArray[file.Name] = file
	}
	case *Dir: {
		ddd := fd.(*Dir)
		for _, valueFd := range ddd.Files {
			switch valueFd.(type) {
			case *File: {
				ddd.getAllDirOrFile(valueFd, pat)
			}
			case *Dir: {
				newDDD := valueFd.(*Dir)
				newDDD.getAllDirOrFile(valueFd, pat)
			}
			}
		}
	}
	}
}

func (dir *Dir) getDirOrGetFile(fd interface{}, lastPathItem bool, paths []string, opType OpType, pat map[string]interface{}) ( interface{}, Err, ActType) {
	switch fd.(type) {
	case *File: {
		if opType == OpMkDirAll {
			return nil, Err_ONE_ITEM_OF_DIR_IS_FILE, kTypeIdle
		}
		//fmt.Printf("File: %v-%v\n", path, value)
		if lastPathItem {

			file := fd.(*File)

			if opType == OpPatGet || opType == OpPatDel {
				dirPath := file.superDir.Abs
				fileArray, ok := pat[dirPath].(map[string]*File)
				if !ok {
					fileArray = make(map[string]*File)
					pat[dirPath] = fileArray
				}
				fileArray[file.Name] = file
			}
			//fmt.Printf("reset: %v - %v\n", path, value)
			return file, Err_OK, kTypeExist
		}else {//中间的一个目录已经是文件了
			return nil, Err_ONE_ITEM_OF_DIR_IS_FILE, kTypeIdle
		}
	}
	case *Dir: {
		d := fd.(*Dir)
		if lastPathItem {
			if opType == OpMkDirAll {
				return d, Err_OK, kTypeExist
			} else if opType == OpMkAutoMkDirAndLastFile {
				return nil, Err_LAST_FILE_IS_DIR_ALREADY, kTypeIdle//last file name is dir
			} else if opType == OpGet || opType == OpDel {
				return d, Err_OK, kTypeExist
			}
		}else {
			//	fmt.Printf("Dir: %v-%v, %v-%v\n", pwd, value, pwd, path)
			return d.setDirOrFile(paths[1:], opType, pat)
		}
	}
	}
	panic("getDirOrGetFile type NA\n")
	return nil, Err_UNKNOWN, kTypeIdle
}
func (dir *Dir) setDirOrFile(paths []string, opType OpType, pat map[string]interface{}) ( interface{}, Err, ActType ) {
	pathLen := len(paths)
	if pathLen == 0 {
		//set root value
		return dir, Err_OK, kTypeExist
	}

	path := paths[0]
	lastPathItem := pathLen == 1

	if opType == OpPatGet || opType == OpPatDel {

		glob, err := CompileGlob("/" + path)
		if (err != nil) {
			fmt.Printf("getFile error: path: %v\n", paths)
			return nil, Err_BAD_PATH_PAT, kTypeIdle
		}

		for key, valueFd := range dir.Files {
			matchOk := false
			sameName := false
			if path == key {
				sameName = true
			}else {
				matchOk = glob.Match("/"+key)
			}
			if matchOk {
				if lastPathItem && strings.HasSuffix(path, "**") {
					//d.getOrDel(nil, files, allPath, true, pat, get)
					dir.getAllDirOrFile(valueFd, pat)
				}else {
					dir.getDirOrGetFile(valueFd, lastPathItem, paths, opType, pat)
				}
			}else if sameName {
				dir.getDirOrGetFile(valueFd, lastPathItem, paths, opType, pat)
			}
		}
		if len(pat) > 0 {
			return nil, Err_OK, kTypeExist
		}
	}else {
		//fmt.Printf("set: %v->%v:%v\n", allPath, path, value)
		fd, found := dir.Files[path]
		if found {
			return dir.getDirOrGetFile(fd, lastPathItem, paths, opType, pat)
		}

		if opType == OpGet || opType == OpDel {
			return nil, Err_NOT_EXIST, kTypeIdle//ErrDirNotExist
		}

		if opType == OpMkDirAll || opType == OpMkAutoMkDirAndLastFile {

			newPwd := dir.Abs + path + "/"

			createDirOrFile := false
			if lastPathItem {//到文件尾部
				createDirOrFile = true
			}else {
				//fmt.Printf("createDir2: %v:%v\n", allPath, newPwd)
				//fmt.Printf("mkdir: %v\n, %v\n", newPwd, paths)
				newItem := NewDir(newPwd, "")
				dir.Files[path] = newItem
				newItem.setDir(dir)
				//fmt.Printf("mkDir   %v  %v\n", allPath, newPwd)
				return dir.setDirOrFile(paths, opType, pat)
			}

			if createDirOrFile {
				if opType == OpMkDirAll {
					//	fmt.Printf("mkDir   %v  %v\n", allPath, paths)
					fileOrDir := NewDir(newPwd, "")
					dir.Files[path] = fileOrDir
					fileOrDir.setDir(dir)
					return fileOrDir, Err_OK, kTypeCreate
				}else if opType == OpMkAutoMkDirAndLastFile {
					//	fmt.Printf("mkFile   %v  %v\n", allPath, paths)
					fileOrDir := NewFile(path, "")
					dir.Files[path] = fileOrDir
					fileOrDir.setDir(dir)
					return fileOrDir, Err_OK, kTypeCreate
				}
			}
		}
	}
	return nil, Err_UNKNOWN, kTypeIdle
}

/*
// compact the log before index (including index)
func (fs *Fs) save(path string, ver uint64) error {

	// create a new log file and add all the entries
	new_file_path := path + ".new"
	file, err := os.OpenFile(new_file_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	datas, err := fs.GetWithPat("/**")
	if err != nil {
		return err
	}
	//fmt.Printf("datas: %v\n\n\n", datas)
	str, err := json.Marshal(datas)
	if err != nil {
		panic(err.Error())
	}
	//	str := fs.encode()

	if _, err = fmt.Fprintf(file, "%8x\n", ver); err != nil {
		return err
	}

	if _, err = fmt.Fprintf(file, "%8x\n", len(str)); err != nil {
		return err
	}

	if _, err = file.Write(str); err != nil {
		file.Close()
		os.Remove(new_file_path)
		return err
	}

	file.Sync()

	// rename the new log file
	err = os.Rename(new_file_path, path)
	if err != nil {
		file.Close()
		os.Remove(new_file_path)
		return err
	}
	file.Close()

	return nil
}

// compact the log before index (including index)
func (fs *Fs) load(path string) error {


	file, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	var ver uint64
	_, err = fmt.Fscanf(file, "%8x\n", &ver)
	if err != nil {
		return err
	}

	var length int
	_, err = fmt.Fscanf(file, "%8x\n", &length)
	if err != nil {
		return err
	}

	bytes := make([]byte, length)

	if _, err = file.Read(bytes); err != nil {
		file.Close()
		return err
	}

	var data map[string]string
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		panic(err.Error())
	}

	for k, v := range data {
		fs.Set(k, v)
	}
	fs.Ver = ver

	file.Close()
	return nil
}

func (fs *Fs) LoadSnapshot() (error) {
	return fs.load(fs.path)
}

func (fs *Fs) TakeSnapshot(ver uint64) (error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	fs.Ver = ver
	return fs.save(fs.path, ver)

}
*/
