package command

import (
	"github.com/goraft/raft"
	"github.com/izouxv/raftX/server/db"
	//"log"
)

// This command writes a value to a key.
type RemoveTmpDataCommand struct {
	Key   string `json:"key"` //user
}

// Creates a new write command.
func NewRemoveTmpDataCommand(key string) *RemoveTmpDataCommand {
	return &RemoveTmpDataCommand{
		Key:   key,
	}
}

// The name of the command in the log.
func (c *RemoveTmpDataCommand) CommandName() string {
	return "rmtd"
}

// Writes a value to a key.
func (c *RemoveTmpDataCommand) Apply(server raft.Server) (interface{}, error) {
	fs := server.Context().(*db.Fs)
	//log.Printf("RemoveTempDataIp: %v\n", c.Key)
	fs.RemoveTempData(c.Key)
	return nil, nil
}
