package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"bytes"
	"sort"
	"time"
	"strconv"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
var s3Svc = s3.New(session.New())

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

	content, err:= getContentFromS3(filename)    //get 
	if err != nil {
		log.Fatalf("cannot load %v", filename)
	}

	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	reduces := make([][]KeyValue, reply.NReduce)   //Shuffling
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {				  //output mr-x-y files
		fileName := reduceName(reply.Seq, idx)
		err := uploadContentToS3(fileName, l)
		if err != nil {
			log.Fatalf("Error uploading file to S3:", err)
			
		}

	}
	

	ImDone(MAPJOB, reply.Seq)
}

func doReduceTask(reducef func(string, []string) string, reply ExampleReply) {
	kva := []KeyValue{}
	for idx := 0; idx < reply.NMap; idx++ {
		fileName := reduceName(idx, reply.Seq)
		result, err := s3Svc.GetObject(&s3.GetObjectInput{  
			Bucket: aws.String("mrfileschalmers"),
			Key:    aws.String(fileName),
		})
		if err != nil {
			log.Fatalf("can not load file: %v", err)
		}
		defer result.Body.Close()


		dec := json.NewDecoder(result.Body)
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
	
	/////////////////////////////////////////// upload
	buf := new(bytes.Buffer)
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
		fmt.Fprintf(buf, "%v %v\n", kva[i].Key, output)

		i = j
	}
	bt, _ := ioutil.ReadAll(buf)
	reader := bytes.NewReader(bt)
	result, err := s3Svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("mrfileschalmers"),
		Key:    aws.String(oname),
		Body:   reader,
	})
	if err != nil {
		log.Fatalf("can not upload file: %v", err)
	}
	log.Println("Successfully uploaded reduce outcome file to S3. ETag:", *result.ETag)

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
	c, err := rpc.DialHTTP("tcp", ":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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


func getContentFromS3(filename string) (string, error) {
	result, err := s3Svc.GetObject(&s3.GetObjectInput{  
		Bucket: aws.String("mrfileschalmers"),
		Key:    aws.String(filename),
	})
	if err != nil {
		log.Printf("Error downloading object from bucket ", err)
		return  "", err
	}
	defer result.Body.Close()
	content, err := ioutil.ReadAll(result.Body)
	return string(content), err
}

func uploadContentToS3(filename string, l []KeyValue) error {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	for _, kv := range l {
		if err := enc.Encode(&kv); err != nil {
			return err
		}

	}
	bt, _ := ioutil.ReadAll(buf)
	reader := bytes.NewReader(bt)
	result, err := s3Svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("mrfileschalmers"),
		Key:    aws.String(filename),
		Body:   reader,
	})
	if err != nil {
		return err
	}

	log.Println("Successfully uploaded map outcome file to S3. ETag:", *result.ETag)
	return nil
}