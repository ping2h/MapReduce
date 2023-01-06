package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//


type ExampleArgs struct {
	Job JobType
	Seq int
}

type ExampleReply struct {
	Job JobType
	Filename string
	NReduce int
	Seq int
	NMap int

}

type JobType int
const (
	MAPJOB JobType = iota
	REDUCEJOB
	WAITJOB
	COMPLETEJOB
)
// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
