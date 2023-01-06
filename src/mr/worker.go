package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
	"strconv"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
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

func doHeartbeat() ExampleReply {
	args := ExampleArgs{}
	reply := ExampleReply{}
	ok := call("Coordinator.TellMeWTD", &args, &reply)
	if !ok {
		log.Fatal("call tellmewtd failed")
	}
	return reply

}

func doMapTask(mapf func(string, string) []KeyValue, reply ExampleReply) {
	log.Printf("doing this map task...")
	filename := reply.Filename //"../main/" + 
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	reduces := make([][]KeyValue, reply.NReduce)   //Shuffling
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {				  //output mr-x-y files
		fileName := reduceName(reply.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("can not creat file: %v", err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("json ecoding: %v", err)
			}

		}
		if err := f.Close(); err != nil {
			log.Fatalf("closing file: %v", err)
		}
	}
	

	ImDone(MAPJOB, reply.Seq)
}

func doReduceTask(reducef func(string, []string) string, reply ExampleReply) {
	kva := []KeyValue{}
	for idx := 0; idx < reply.NMap; idx++ {
		fileName := reduceName(idx, reply.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("can not open file: %v", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	oname := "mr-out-"+strconv.Itoa(reply.Seq)
	ofile, _ := os.Create(oname)

	
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	ImDone(REDUCEJOB, reply.Seq)

}

func ImDone(job JobType, seq int) {
	log.Printf("done with this map task")
	args := ExampleArgs{Job: job, Seq: seq}
	reply := ExampleReply{}
	ok := call("Coordinator.ImDone", &args, &reply)
	if !ok {
		log.Fatal("call tellmewtd failed")
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}