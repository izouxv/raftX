package server

import (
	//	"io/ioutil"
	//	"os"
	//	log "code.google.com/p/log4go"
	"fish.red/zouxu/goall/raftx/server/db"
)

type FsStateMachine struct {
	fs* db.Fs
}

// NewDbStateMachine returns a StateMachine for capturing and restoring
// the state of an sqlite database.
func NewFsStateMachine(fs* db.Fs) *FsStateMachine {
	d := &FsStateMachine{
		fs: fs,
	}
	//	log.Trace("New DB state machine created with path: %s", path)
	return d
}

// Save captures the state of the database. The caller must ensure that
// no transaction is taking place during this call.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (d *FsStateMachine) Save() ([]byte, error) {
	//	log.Trace("Capturing database state from path: %s", d.dbpath)
	//	b, err := ioutil.ReadFile(d.dbpath)
	//	if err != nil {
	//		log.Error("Failed to save state: ", err.Error())
	//		return nil, err
	//	}
	//	log.Trace("Database state successfully saved to %s", d.dbpath)
	//	return b, nil
	data, _ := d.fs.GetFsData()
	return data, nil
	//	return nil, nil
}

// Recovery restores the state of the database using the given data.
func (d *FsStateMachine) Recovery(b []byte) error {
	//	log.Trace("Restoring database state to path: %s", d.dbpath)
	//	err := ioutil.WriteFile(d.dbpath, b, os.ModePerm)
	//	if err != nil {
	//		log.Error("Failed to recover state: ", err.Error())
	//		return err
	//	}
	//	log.Trace("Database restored successfully to %s", d.dbpath)
	d.fs.SetFsData(b)
	return nil
	//	return nil
}
