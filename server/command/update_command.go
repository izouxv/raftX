package command

import (
	"github.com/goraft/raft"
	"fish.red/zouxu/goall/raftx/server/db"
)

// This command writes a value to a key.
type SetCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Tag   string `json:"tag"`
	Verb  uint32 `json:"verb"`
	Oper  string `json:"oper"` //Operator for template data
}

// Creates a new write command.
func NewSetCommand(key string, value string, tag string, verb uint32, oper string) *SetCommand {
	return &SetCommand{
		Key:   key,
		Value: value,
		Tag:tag,
		Verb: verb,
		Oper: oper,
	}
}

// The name of the command in the log.
func (c *SetCommand) CommandName() string {
	return "set"
}

// Writes a value to a key.
func (c *SetCommand) Apply(server raft.Server) (interface{}, error) {
	fs := server.Context().(*db.Fs)

	return fs.Apply(c.Key, c.Value, c.Verb, c.Oper)
}
