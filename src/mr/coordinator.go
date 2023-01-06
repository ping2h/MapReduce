package mr

import "log"
import "net"
// import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

func init () {
	log.SetFlags(log.Ldate|log.Ltime|log.Lshortfile)
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	files []string
	tasks []Task
	nMap int          //num of files
	nReduce int       //mapper output n files
	phase MRPhase


}
type MRPhase int
const (
	MAP MRPhase = iota
	REDUCE 
	DONE
)


type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus  //idle, in-progress, or completed
	
} 

type TaskStatus int
const (
	IDLE TaskStatus = iota 
	INPROGRESS
	COMPLETED
)

// Your code here -- RPC handlers for the worker to call.

//
// handler: worker asks for a task
// and detect whether is there any overtimed task 
//
func (c *Coordinator) TellMeWTD(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	log.Printf("allocating a new task...")
	checkCrash(c)
	if c.phase == MAP {
		task := returnMapTask(c)
		
		if task != nil {
			reply.Filename = task.fileName
			reply.NReduce = c.nReduce
			reply.Job = MAPJOB
			reply.Seq = task.id
			return nil
		}
	} else if c.phase == REDUCE {
		task :=returnReduceTask(c)
		if task != nil {
			reply.Filename = task.fileName
			reply.NReduce = c.nReduce
			reply.Job = REDUCEJOB
			reply.Seq = task.id
			reply.NMap = c.nMap
			return nil
		}
	} else {
		reply.Job = COMPLETEJOB
		return nil
	}
	reply.Job = WAITJOB
	return nil
}

//
// handler: worker report after finishing task
// and change phase
//
func (c *Coordinator) ImDone(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
    defer c.mu.Unlock()
	changePhase := true
	if args.Job == MAPJOB {										//map -> reduce
		c.tasks[args.Seq].status = COMPLETED
		log.Printf("worker has finished a map task")
		for _, v := range c.tasks {
			if v.status == IDLE || v.status == INPROGRESS {
				changePhase = false
			}
		}
		if changePhase == true {	
			log.Printf("going to reduce phase")					
			c.phase = REDUCE
			tasks := initReduceTask(c.nReduce)
			c.tasks = append(c.tasks, tasks...)
		}
															  //reduce -> done
	} else if args.Job == REDUCEJOB {
		c.tasks[args.Seq+c.nMap].status = COMPLETED
		log.Printf("worker has finished a reduce task")
		for _, v := range c.tasks[c.nMap:] {
			if v.status == IDLE || v.status == INPROGRESS {
				changePhase = false
			}
		}
		if changePhase == true {	
			log.Printf("Done!")								
			c.phase = DONE
		}
	}
	return nil
}



func returnMapTask(c *Coordinator) *Task {
	for i, v := range c.tasks {
		if i > c.nMap {
			log.Fatal("task phase wrong")
		}
		if v.status == IDLE {
			log.Printf("find a map task ")

			v.status = INPROGRESS
			v.startTime = time.Now()
			return &v
		}
	}
	log.Printf("no idle map task")
	return nil 
}

func returnReduceTask(c *Coordinator) *Task {
	if len(c.tasks) == c.nMap {
		log.Fatal("task phase wrong")
	}
	for _, v := range c.tasks[c.nMap:] {
		if v.status == IDLE {
			v.startTime = time.Now()
			v.status = INPROGRESS
			return &v
		}
	}
	return nil 
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("listh at socket")
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if c.phase == DONE {
		ret = true
	}


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := initMapTask(files)   
	c := Coordinator {
		files: files,
		nMap: len(files),       
		nReduce: nReduce,       
		phase: MAP,
		tasks: tasks,
	}
	
	


	c.server()
	return &c
}

func initMapTask(files []string) []Task {
	tasks := []Task{}
	for i, v := range files {
		task := Task{fileName: v, id: i, status: IDLE}
		tasks = append(tasks, task)
	}
	log.Println("initializing Map tasks")
	return tasks
}

func initReduceTask(nReduce int) []Task {
	tasks := []Task{}
	i := 0
	for i < nReduce {
		task := Task{id: i, status: IDLE}
		tasks = append(tasks, task)
		i++
	}

	log.Println("initializing Reduce tasks")
	return tasks
}

func checkCrash(c *Coordinator) {
	for _, v := range c.tasks {
		if v.status == INPROGRESS {
			if time.Now().Second() - v.startTime.Second()>10 {
				v.status = IDLE
			}
		}
	}

}