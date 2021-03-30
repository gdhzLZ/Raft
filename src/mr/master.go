package mr
import (
	"fmt"
	"log"
	"strconv"
	"time"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"container/list"
)

type Master struct {
	REDUCENUMBER int
	FILENUM int
	MAPFILENUMBER map[string]int
	mu   sync.Mutex
	MapPhaseFinish bool
	ReducePhaseFinish bool
	MapFilesQueue  list.List
	ReduceFilesQueue list.List
	FinishedMapTasks map[string]string
	FinishedReduceTasks map[string]string

}

func (m *Master) FinishTask(args *FinishArgs, reply *FinishReply) error{
	reply.Message = "FinishedTasks"
	if args.Type == 0{
		m.mu.Lock()
		m.FinishedMapTasks[args.FinishedFile] = "1"
		if len(m.FinishedMapTasks) == m.FILENUM{
			m.MapPhaseFinish = true
		}
		fmt.Println("Master receive finished11 "+ strconv.Itoa(len(m.FinishedMapTasks)))
		m.mu.Unlock()
	} else{
		m.mu.Lock()
		m.FinishedReduceTasks[args.FinishedFile] = "1"
		if len(m.FinishedReduceTasks) == m.REDUCENUMBER{
			m.ReducePhaseFinish = true
		}
		fmt.Println("Master receive finished22 "+ strconv.Itoa(len(m.FinishedReduceTasks)))
		m.mu.Unlock()

	}

	return nil
}
func Monitor(m *Master,task string,Type int){
	second := 10
	var i int
	if Type == 0 {
		for i = 0; i < second; i++ {
			time.Sleep(time.Second)
			if m.FinishedMapTasks[task] != "" {
				break
			}
		}
		if i >= second {
			m.mu.Lock()
			m.MapFilesQueue.PushBack(task)
			m.mu.Unlock()
		}
	} else{
		for i=0 ; i < second ; i++ {
			time.Sleep(time.Second)
			if m.FinishedReduceTasks[task] != ""{
				break
			}
		}
		if i >= second{
			m.mu.Lock()
			m.ReduceFilesQueue.PushBack(task)
			m.mu.Unlock()
		} else{

		}
	}
}
func AssignNoTask(reply *Tasks,AllFinished bool) {
	reply.AllFinished = AllFinished
	reply.ReduceTask = nil
	reply.MapTask = ""
}
func AssignMapTask(m *Master,reply *Tasks) string{
	reply.AllFinished = false
	reply.ReduceTask = nil
	task := fmt.Sprintf("%v", m.MapFilesQueue.Front().Value)
	m.MapFilesQueue.Remove(m.MapFilesQueue.Front())
	reply.MapTask = task
	reply.REDUCENUMBER = m.REDUCENUMBER
	reply.MapTaskNumber = m.MAPFILENUMBER[task]
	return task
}
func AssignReduceTask(m *Master,reply *Tasks) string{
	reply.AllFinished = false
	reply.MapTask = ""
	task := fmt.Sprintf("%v", m.ReduceFilesQueue.Front().Value)
	m.ReduceFilesQueue.Remove(m.ReduceFilesQueue.Front())
	for i :=0 ; i < m.FILENUM ; i++{
		reply.ReduceTask = append(reply.ReduceTask, "mr-"+strconv.Itoa(i)+"-"+task)
	}
	reply.ReduceTaskNumber = task
	return task
}
func (m *Master) AssignTask(args *ExampleArgs, reply *Tasks) error {
	m.mu.Lock()
	if m.MapPhaseFinish == false{   // map task
		if m.MapFilesQueue.Front() == nil{  // there is no maptask to do , but some maptasks has not been finished
			AssignNoTask(reply,false)
			m.mu.Unlock()
			return nil
		}else{
			task := AssignMapTask(m,reply)
			m.mu.Unlock()
			go Monitor(m,task,0)
			return nil
		}

	}else if  m.ReducePhaseFinish == false{    //reduce task
		if m.ReduceFilesQueue.Front() == nil{// there is no reducetask to do , but some reducetasks has not been finished
			AssignNoTask(reply,false)
			m.mu.Unlock()
			return nil
		}else{
			task:=AssignReduceTask(m,reply)
			m.mu.Unlock()
			go Monitor(m,task,1)
			return nil

		}
	}else{
		AssignNoTask(reply,true)
		m.mu.Unlock()
		return nil
	}


}



//
// start a thread that listens for RPCs from worker.go
//


func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname :=masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return (m.MapPhaseFinish&&m.ReducePhaseFinish)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.REDUCENUMBER = nReduce
	m.FinishedMapTasks  = make(map[string]string)
	m.FinishedReduceTasks = make(map[string]string)
	m.MAPFILENUMBER  = make(map[string]int)
	m.MapPhaseFinish = false
	m.ReducePhaseFinish = false
	for  i := 0; i < len(files) ; i++{
		m.MapFilesQueue.PushBack(files[i])
		m.MAPFILENUMBER[files[i]] = i
	}
	for i:=0 ; i < nReduce ; i++{
		m.ReduceFilesQueue.PushBack(strconv.Itoa(i))
	}
	m.FILENUM = len(files)
	m.server()
	return &m
}
