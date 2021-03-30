package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func isExist(path string)(bool){
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return true
}
//
// main/mrworker.go calls this function.
//

func informMasterFinished(filename string, Type int){
	fmt.Println("Worker has finished " + filename)
	finishArg := FinishArgs{}
	finishArg.FinishedFile = filename
	finishArg.Type = Type
	finishReply := FinishReply{}
	call("Master.FinishTask", &finishArg, &finishReply)

}
func DoMapTask(Tasks Tasks,mapf func(string, string) []KeyValue){
	filename := Tasks.MapTask
	MapFile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open2 %v", filename)
	}
	content, err := ioutil.ReadAll(MapFile)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	MapFile.Close()
	kva := mapf(filename, string(content))

	nReduce := Tasks.REDUCENUMBER
	interFiles := []*os.File{}
	for i:= 0 ; i < nReduce ; i++{
		filename := "mr-"+strconv.Itoa(Tasks.MapTaskNumber)+"-"+strconv.Itoa(i)
		if isExist(filename){
			os.Remove(filename)
		}
		file, err := os.OpenFile(filename, os.O_WRONLY | os.O_CREATE, os.ModeAppend|os.ModePerm)
		interFiles = append(interFiles, file)
		if err != nil {
			log.Fatalf("cannot open3 %v", filename)
		}
	}
	for _, kv := range kva {
		index := (ihash(kv.Key)) % nReduce
		//index ,_:=  strconv.Atoi(kv.Key)
		enc := json.NewEncoder(interFiles[index])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encoder %v", kv)
		}
	}
	for i:= 0 ; i < nReduce ; i++{
		interFiles[i].Close()
	}
}
func DoReduceTask(Tasks Tasks,reducef func(string, []string) string){
	intermediate := []KeyValue{}
	for i:=0; i<len(Tasks.ReduceTask);i++{
		filename := Tasks.ReduceTask[i]
		file, err := os.Open(filename)
		if err != nil{
			log.Fatalf("can not open 444 %v",filename)
		}
		dec := json.NewDecoder(file)
		for {
			var hh KeyValue
			if err := dec.Decode(&hh); err != nil {
				break
			}
			intermediate = append(intermediate, hh)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-"+Tasks.ReduceTaskNumber
	if isExist(oname){
		os.Remove(oname)
	}
	ofile, err:= os.Create(oname)
	if err != nil{
		log.Fatalf("can not create %v" , oname)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for{
		Args := ExampleArgs{}
		Tasks := Tasks{}
		call("Master.AssignTask", &Args, &Tasks)
		if Tasks.AllFinished == true{
			break
		}
		if Tasks.MapTask == "" && Tasks.ReduceTask == nil{

		} else if Tasks.ReduceTask == nil{   // map task
			DoMapTask(Tasks,mapf)
			informMasterFinished(Tasks.MapTask,0)

		}else{							// reduce task
			DoReduceTask(Tasks,reducef)
			informMasterFinished(Tasks.ReduceTaskNumber,1)

		}
		time.Sleep(1*time.Second)

	}

}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)

	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
