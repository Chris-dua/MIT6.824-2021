package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// [1] TaskType, for example MapTask, ReduceTask;

// Task description
/*type Task struct {
	TaskType        TaskType // Task Type
	TaskId          int
	MapInputFile    string   // map 任务输入，文件名：pg-bing_ernest.txt
	ReduceInputFile []string // reduce 任务输入
	NumReduce       int
	// 以下字段 coordinator 使用
	Index     int       // 在 coodinator task 数组中的位置
	State     TaskState // 任务状态，0：未开始，1: 已分配，正在运行, 2: 任务完成
	StartTime int64     // 任务分配出去的时间，单位 s，超时使用
}*/

type Task struct {
	Id           int
	TaskType     TaskType
	MapInputFile string
	WorkId       int
	DeadLine     time.Time
	State        TaskState
}

type TaskType int

const (
	TaskTypeUnknow TaskType = 0
	TaskTypeMap    TaskType = 1
	TaskTypeReduce TaskType = 2
)

type TaskState int

const (
	TaskStateInit   TaskState = 0 // task is not started
	TaskStateRuning TaskState = 1 // task is running
	TaskStateDone   TaskState = 2 // task finished
)

type ApplyForTaskArgs struct {
	WorkId       int
	LastTaskId   int
	LastTaskType TaskType
}

type ApplyForTaskReply struct {
	TaskId       int
	TaskType     TaskType
	MapInputFile string
	NReduce      int
	NMap         int
}

/*type TaskDoneArgs struct {
	Task *Task
}

type TaskDoneReply struct {
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task *Task
}*/

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
