package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 快速地将string类型转化为[]byte类型
// func quickS2B(str string) []byte {
// 	base := *(*[2]uintptr)(unsafe.Pointer(&str))
// 	return *(*[]byte)(unsafe.Pointer(&[3]uintptr{base[0], base[1], base[1]}))
// }

// // 快速地将[]byte转化为string类型
// func quickB2S(bs []byte) string {
// 	base := (*[3]uintptr)(unsafe.Pointer(&bs))
// 	return *(*string)(unsafe.Pointer(&[2]uintptr{base[0], base[1]}))
// }

// // reduce任务完成时，需要将临时文件重命名（原子地）
// func renameReduceOutFile(tmpf string, taskN int) error {
// 	return os.Rename(tmpf, fmt.Sprintf("mr-out-%d", taskN))
// }

// // 拿到map worker里的中间文件
// func getIntermediateFIle(nMapTasks, nReduceTasks int) string {
// 	return fmt.Sprintf("mr-%d-%d", nMapTasks, nReduceTasks)
// }

// // 把map worker里的中间文件重命名（原子地）
// func renameIntermediateFIle(tmpf string, nMapTasks, nReduceTasks int) error {
// 	return os.Rename(tmpf, getIntermediateFIle(nMapTasks, nReduceTasks))
// }

func tmpMapOutFile(workId int, mapId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workId, mapId, reduceId)
}

func finalMapOutFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func tmpReduceOutFile(workerId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerId, reduceId)
}

func finalReduceOutFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := os.Getpid()
	lastTaskId := -1
	var lastTaskType TaskType
	for {
		args := ApplyForTaskArgs{
			WorkId:       id,
			LastTaskId:   lastTaskId,
			LastTaskType: lastTaskType,
		}
		reply := ApplyForTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)
		// if reply.TaskType == TaskTypeMap {
		// 	log.Printf("GetMasterReply! TaskTypeName:%d-%d, fileMapInput: %s, workId: %d\n", reply.TaskType, reply.TaskId, reply.MapInputFile, id)
		// } else if reply.TaskType == TaskTypeReduce {
		// 	log.Printf("GetMasterReply! TaskTypeName:%d-%d, workId: %d\n", reply.TaskType, reply.TaskId, id)
		// }

		switch reply.TaskType {
		case TaskTypeUnknow:
			//log.Printf("All tasks are finished!")
			goto End
		case TaskTypeMap:
			doMapTask(id, reply, mapf)
		case TaskTypeReduce:
			doReduceTask(id, reply, reducef)
		}

		lastTaskId = reply.TaskId
		lastTaskType = reply.TaskType
		//log.Printf("complete task %d-%d", reply.TaskType, reply.TaskId)
	}
End:
	//log.Printf("Worker %d finished tasks\n", id)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(id int, ReplyMapTask ApplyForTaskReply, mapf func(string, string) []KeyValue) {
	// read data
	//log.Printf("do Map Task, TaskName: %d-%d\n", ReplyMapTask.TaskType, ReplyMapTask.TaskId)
	file, err := os.Open(ReplyMapTask.MapInputFile)

	if err != nil {
		log.Fatalf("%s open failed!", ReplyMapTask.MapInputFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("%s file content read failed", ReplyMapTask.MapInputFile)
	}
	file.Close()

	kva := mapf(ReplyMapTask.MapInputFile, string(content))
	hashedKva := make(map[int][]KeyValue)

	for _, kv := range kva {
		hashed := ihash(kv.Key) % ReplyMapTask.NReduce
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}

	for i := 0; i < ReplyMapTask.NReduce; i++ {
		outFile, _ := os.Create(tmpMapOutFile(id, ReplyMapTask.TaskId, i))
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outFile.Close()
	}
}

func doReduceTask(id int, ReplyReduceTask ApplyForTaskReply, reducef func(string, []string) string) {
	//log.Printf("do Reduce Task, TaskName: %d-%d\n", ReplyReduceTask.TaskType, ReplyReduceTask.TaskId)
	var lines []string
	for i := 0; i < ReplyReduceTask.NMap; i++ {
		tmpfilename := finalMapOutFile(i, ReplyReduceTask.TaskId)
		file, err := os.Open(tmpfilename)
		if err != nil {
			log.Fatalf("file %s open failed", tmpfilename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("file %s read failed", tmpfilename)
		}

		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")

		kva = append(kva, KeyValue{
			Key:   split[0],
			Value: split[1],
		})
	}
	sort.Sort(ByKey(kva))
	outFile, _ := os.Create(tmpReduceOutFile(id, ReplyReduceTask.TaskId))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
