# 6.824 Lab 1: MapReduce

In this lab I've built a MapReduce system similar to the [MapReduce paper](http://research.google.com/archive/mapreduce-osdi04.pdf). The implementation has passed all tests.  In addition there is another version meeting the challenge exercise which allows coordinator and workers to run on separate machines. S3 is used as the file system. 

## Usage

1. Fetch the rep to local computer.

   ```git
   git clone https://github.com/ping2h/dslab3.git
   cd 6.824/src/main
   ```

2. Build mr app plugin 

   ```go
   go build -race -buildmode=plugin ../mrapps/wc.go
   ```

   note: if the error occurs : cannot load plugin wc.so, you could add -gcflags="all=-N -l" flag and make sure add the flag too when you run worker process.

3. Run the coordinator

   ``` go
   go run -race mrcoordinator.go pg-*.txt
   ```

4. Run workers in other terminals

   ```go
   go run -race mrworker.go wc.so
   ```

   When the workers and coordinator have finished, look at the output in **mr-out-* **. 

   ``` bash
   cat mr-out-* | sort | more
   A 509
   ABOUT 2
   ACT 8
   ```

5. Run the test script

   ```bash
   bash test-mr.sh
   ```

   note:  due to script setting, change the file path before run test

   ```go
   func doMapTask(mapf func(string, string) []KeyValue, reply ExampleReply) {
   	log.Printf("doing this map task...")
   	filename := reply.Filename //"../main/" + 
   	file, err := os.Open(filename)
   ```

## How it works

### Coordinator

* Data structures

  ```go
  type Coordinator struct {
  	// Your definitions here.
  	mu sync.Mutex
  	files []string
  	tasks []Task
  	nMap int          //num of files
  	nReduce int       //mapper output n files
  	phase MRPhase
  
  
  }
  
  type Task struct {
  	fileName  string
  	id        int
  	startTime time.Time
  	status    TaskStatus  //idle, in-progress, or completed
  	
  } 
  ```

* The coordinator is responsible for keeping track of the status of tasks and the phase of mr task. There are 3 phases in the implementation: **Map -> Reduce -> Done**. 

  1. When the coordinator is created, it is initially in map phase and initialize the map tasks.

     ``` go
     func initMapTask(files []string) []Task {
     	tasks := []Task{}
     	for i, v := range files {
     		task := Task{fileName: v, id: i, status: IDLE}
     		tasks = append(tasks, task)
     	}
     	log.Println("initializing Map tasks")
     	return tasks
     }
     ```

  2. The coordinator is running as an rpc server to handle the worker's rpc call

  3. A worker will use the rpc call to get a task whenever its last task is done until mr is finished and coordinator exited.

     ```go
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
     ```

     the coordinator assigns tasks depending on the job phases

  4. The worker will call another rpc `func (c *Coordinator) ImDone(args *ExampleArgs, reply *ExampleReply)` . The coordinator change phase in this function depending on the status of tasks **map -> reduce       reduce -> done**

  5. Fault tolerance. A in progress task takes over 10s will be considered as a crash. The task's status will be changed to IDLE and will be scheduled again. How? A task has a starting time which will be  assigned when the work calls `func (c *Coordinator) TellMeWTD(args *ExampleArgs, reply *ExampleReply)`  and the coordinator checks crash at this time by call this function:

     ``` go
     func checkCrash(c *Coordinator) {
     	for _, v := range c.tasks {
     		if v.status == INPROGRESS {
     			if time.Now().Second() - v.startTime.Second()>10 {
     				v.status = IDLE
     			}
     		}
     	}
     
     }
     ```

     This is a lazy schema but there is no need to add a goroutine ,but can reduce the complexity of the code.

  ### Worker

  The worker is relatively simple just looping and requesting tasks.

  ```go
  func Worker(mapf func(string, string) []KeyValue,
  	reducef func(string, []string) string) {
  
  	// TODO 1
  	for {
  		response := doHeartbeat()
  		log.Printf("Worker: receive coordinator's task %v \n", response)
  		switch response.Job {
  		case MAPJOB:
  			doMapTask(mapf, response)
  		case REDUCEJOB:
  			doReduceTask(reducef, response)
  		case WAITJOB:
  			time.Sleep(1 * time.Second)
  		case COMPLETEJOB:
  			return
  		default:
  			panic(fmt.Sprintf("unexpected jobType %v", response.Job))
  		}
  	}
  
  }
  ```

## Challenge part

```bash
git checkout challenge
cd 6.824/src/mr
go tidy
```

To run coordinator and workers separately, use the amazon S3 file system to store task input and output files.

