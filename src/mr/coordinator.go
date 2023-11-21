package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*type Coordinator struct {
	// Your definitions here.
	mapTasks    []*Task // map Task
	reduceTasks []*Task // reduce Task

	nReduce int
	nMap    int

	finishedMapNum    int // 已完成的map的任务数量
	finishedReduceNum int // 已完成的reduce的任务数量

	state coordinatorState // 协调者状态, 0 : 初始化, 1: map 任务全部完成, 2: reduce 任务完成
	mu    sync.Mutex
}*/

type Coordinator struct {
	state coordinatorState
	mu    sync.Mutex

	nReduce int
	nMap    int

	tasks     map[string]Task // 任务映射，主要是查看任务状态
	toDoTasks chan Task       // 待完成任务，采用通道实现，内置锁结构
}

type coordinatorState int

const (
	coodinatorStateInit        coordinatorState = 0
	coodinatorStateMapFinished coordinatorState = 1
	coodinatorStateFinished    coordinatorState = 2
)

// Your code here -- RPC handlers for the worker to call.
func createTaskId(taskType TaskType, Id int) string {
	return fmt.Sprintf("%d-%d", taskType, Id)
}

// GetTask worker获取任务
func (c *Coordinator) ApplyForTask(arg *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if arg.LastTaskId != -1 {
		c.mu.Lock()
		LastTaskId := createTaskId(arg.LastTaskType, arg.LastTaskId)
		if LastTask, ok := c.tasks[LastTaskId]; ok && LastTask.WorkId == arg.WorkId {
			log.Printf("%d finish %s task", arg.WorkId, LastTaskId)
			if arg.LastTaskType == TaskTypeMap {
				for i := 0; i < c.nReduce; i++ {
					err := os.Rename(tmpMapOutFile(arg.WorkId, arg.LastTaskId, i), finalMapOutFile(arg.LastTaskId, i))
					if err != nil {
						log.Fatalf("Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(arg.WorkId, arg.LastTaskId, i), err)
					}
				}
			} else if arg.LastTaskType == TaskTypeReduce {
				err := os.Rename(
					tmpReduceOutFile(arg.WorkId, arg.LastTaskId),
					finalReduceOutFile(arg.LastTaskId))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(arg.WorkId, arg.LastTaskId), err)
				}
			}
			//log.Printf("The number of remaining tasks is %d\n", len(c.tasks))
			if len(c.tasks) == 1 && c.state == coodinatorStateInit {
				c.state = coodinatorStateMapFinished
			} else if len(c.tasks) == 1 && c.state == coodinatorStateMapFinished {
				c.state = coodinatorStateFinished
			}
			delete(c.tasks, LastTaskId)
			if len(c.tasks) == 0 {
				c.cutover()
			}
		}
		c.mu.Unlock()
	}

	task, ok := <-c.toDoTasks
	if !ok {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	taskIdName := createTaskId(task.TaskType, task.Id)
	log.Printf("Assign %s task to work, the workId: %d \n", taskIdName, arg.WorkId)
	// update tasks
	task.WorkId = arg.WorkId
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.tasks[taskIdName] = task

	reply.TaskId = task.Id
	reply.TaskType = task.TaskType
	reply.MapInputFile = task.MapInputFile
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}
func (c *Coordinator) cutover() {
	if c.state == coodinatorStateMapFinished {
		//log.Printf("The number of remaining tasks is %d\n", len(c.tasks))
		log.Printf("All MAP tasks have already finished! REDUCE tasks begin ...")
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Id:       i,
				TaskType: TaskTypeReduce,
				WorkId:   -1,
			}
			c.tasks[createTaskId(task.TaskType, i)] = task
			c.toDoTasks <- task
		}
	} else if c.state == coodinatorStateFinished {
		log.Printf("All Reduce tasks have already finished! Done...")
		close(c.toDoTasks)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := c.state == coodinatorStateFinished
	defer c.mu.Unlock()

	// Your code here.
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state:     coodinatorStateInit,
		nMap:      len(files),
		nReduce:   nReduce,
		tasks:     make(map[string]Task),
		toDoTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// 初始化map任务

	for i, file := range files {
		task := Task{
			Id:           i,
			TaskType:     TaskTypeMap,
			MapInputFile: file,
			WorkId:       -1,
		}
		log.Printf("TaskId: %s, Type: %d,  MapTaskInuputFile: %s", createTaskId(task.TaskType, task.Id), task.TaskType, task.MapInputFile)
		c.tasks[createTaskId(task.TaskType, task.Id)] = task
		c.toDoTasks <- task
	}
	log.Printf("Coordinator start\n")
	c.server()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.mu.Lock()
			for _, task := range c.tasks {
				if task.WorkId != -1 && time.Now().After(task.DeadLine) {
					taskId := createTaskId(task.TaskType, task.Id)
					log.Printf("%d task %s run timeout, will run again", task.WorkId, taskId)
					task.WorkId = -1
					c.toDoTasks <- task
				}
			}
			c.mu.Unlock()
		}
	}()

	return &c
}
