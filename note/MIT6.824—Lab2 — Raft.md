# MIT6.824—Lab2 — Raft 一致性协议篇

1. 问题分解：尽可能将问题分解为独立的可解决、可解释和可理解的模块。例如，将**领导者选举、日志复制、安全性、成员变更**拆分开。
2. 简化状态空间：通过减少需要考虑的状态数量，使系统更加清晰并消除存在的不确定性。具体来说，日志是连续的，并且避免了日志不一致的情况出现。

与此同时，相比于其他的分布式算法，Raft也有一些新的特性：

- 强领导者模式：使用了比其他共识算法更强的领导者模型。例如， Log entries 只会从领导者向其他节点同步，简化了日志复制管理。
- 领导者选举：在Raft中主要使用了一个随机的计时器来控制选举，比如Follower和Candidate的超时时间都是随机的，可以避免选举时的分裂问题。
- 成员变更：采用了联合共识（joint consensus）的机制，在集群配置变更（新增/删除节点）时，使得集群可以正常工作。



**接下来，将从RAFT四部分：领导者选举、日志同步和心跳维持、持久化、日志压缩和快照进行介绍。**

 ## 1、Lab2-A（领导者选举）+ Lab2-B（日志复制）

### 1.1 角色

- **Leader**：正常情况下，每个集群只有一个Leader。负责处理客户端的写请求、日志复制、向Follower定期发送心跳信息。也就是说，**数据是从 Leader向其他节点单向流动的**。
- **Candidate**：Candidate节点向其他节点发送请求投票的RPC消息，如果赢得了大多数选票，就成为 Leader。
- **Follower**：Follower是被动的，正常情况下不会主动发出请求；当超过一定时间没有收到来自Leader的心跳信息，就会time out，成为Candidate。

![image-20211229172959678](https://camo.githubusercontent.com/35f7dab6f81ef783a0fa0b871aafc4281a3f41547bf87c028273c34dbc5c8bad/687474703a2f2f67616e676875616e2e6f73732d636e2d7368656e7a68656e2e616c6979756e63732e636f6d2f696d672f696d6167652d32303231313232393137323935393637382d323032312d31322d32392e706e67 "状态转换图")



Lab2 的 A、B、C、D 都是按照论文里的这张图进行的。

![image-20211229170522923](https://camo.githubusercontent.com/2299bbaa9e402617192c70c4081bf5d8e077a729f57cacc5094fd817a8bfda4c/687474703a2f2f67616e676875616e2e6f73732d636e2d7368656e7a68656e2e616c6979756e63732e636f6d2f696d672f696d6167652d32303231313232393137303532323932332d323032312d31322d32392e706e67)

首先依据论文中的state，写出Raft的结构体：

```go
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state          NodeState
	currentTerm    int
	voteFor        int          // who to vote for
	electionTimer  *time.Timer  // election time
	heartbeatTimer *time.Ticker // heartbeat time

	logs        []Entry // 所有日志条目
	commitIndex int		//for 2b,已经提交的最新日志的 index, "已经提交" == 这条日志已经被大多数 server 所接受
	lastApplied int		//for 2b, 已应用到的上层应用日志最新的 index，后台定时根据 commitIndex 的增加去提交

	nextIndex  []int	//记录各个 Server 写入下一条 Log 的位置 index
	matchIndex []int	//各个 Server 已有 Log 的与 leader 保持一致最后的位置

	applyCh        chan ApplyMsg	//for 2b, 通过管道，需要把已经 committed 的 log apply 到 上层应用
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type NodeState int

const (
	StateLeader    = 1
	StateCandidate = 2
	StateFollower  = 3
)
```

### 1.2 选举

​	选举，首先需要知道什么时候需要进行选举，以及选举的一些细节问题。

**1.2.1 什么时候需要选举**

- Raft刚刚运行的时候，所有节点都会有一个随机的`electionTimer`，并且他们的`state`都是`follower`，当某个节点的选举时间到期，就要开始进行选举，并且转化自己的状态为`condidate`（候选者）
- 当leader失联，导致周期性地对所有对等点广播心跳消息未送达，因此直到某个`follower`的选举时间到期重新开始选举。

**1.2.2 选举的细节 — 两个RPC**

**`RequestVote`**

- 1、触发请求的时机

  选举超时，`follower`或者`condidate`在一段时间内没有收到leader的心跳RPC。

- 2、接收到请求，sever端的逻辑

​			i. 拒绝`Term`比自己小的请求；

​			ii. 收到`Term`比自己大的请求，更新`Term`，转成`follower`

​			iii. 判断是否投票 

​				a. 自己在该`Term`下没有投过票，

​				b. 只给对方日志领先（包括一致）自己日志的投票，确保包括自己的`commited`日志的leader。

​				c. **投票成功后重置选举时间。**

- 3、收到回复后，client端的逻辑

​			i. 如果reply的term比自己的term大，更新term，转成follower，直接返回

​			ii. 如果reply的`success`是`true`，则加一票

​			iii. 当加的票数大于总节点数量，则当选leader，**当选leader后需要立即发送心跳消息，重置其他节点的选举时间。**

```go
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.voteFor = args.Term, -1
	}
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) &&
		rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.voteFor = args.CandidateID
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		return
	}
	reply.Term, reply.VoteGranted = rf.currentTerm, false
}
```

- 选举的func

```go
func (rf *Raft) StartElection() {

	// 候选者投自己一票
	rf.persist()
	grantedVote := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.getLastLogIndex(),
			LastLogTerm:  rf.getLog(rf.getLastLogIndex()).Term,
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVote++
						if grantedVote > len(rf.peers)/2 {
							// 成为leader，并且发送AppendEntries心跳
							rf.ChangeState(StateLeader)
						}
					}
				} else if reply.Term > rf.currentTerm {
					rf.ChangeState(StateFollower)
					rf.currentTerm, rf.voteFor = reply.Term, -1
					rf.persist()
				}
			}
		}(peer)
	}
}
```

因为成为leader需要马上发送心跳消息，所以个人就把状态包装成一个`ChangeState`函数。

```go
func (rf *Raft) ChangeState(state NodeState) {
	if state == StateLeader {
		rf.state = StateLeader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
		rf.nextIndex[rf.me] = len(rf.logs)
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		rf.sendAppendLogsToAll()

	} else if state == StateCandidate {
		rf.state = StateCandidate
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.electionTimer.Reset(RandomizedElectionTimeout())

	} else if state == StateFollower {
		rf.state = StateFollower
	}
}
```

### 1.3 日志复制

由于日志和心跳消息都通过`AppendEntries`进行传输，因此合到同一个进行校验。

**`AppendEntries`**

- 1、触发时机

​			定时发送，只能由leader发送到follower

- 2、接收请求

​			i. 拒绝`Term`比自己小的请求

​			ii. 收到比`Term`自己大的请求，更新`Term`，转成`follower`，重置选举时间，继续往下处理

​			iii. 对比自己的`logs`和`args`请求的`PrevLogIndex`

​				a. 如果自己的log数量少于`PrevLogIndex`，则返回`false`

​				b. 如果在同一位置Index有日志，但是`Term`不一致，返回`false`

​			iv. 截取掉比`PrevLogIndex`多的log，并且`append`从`args`传来的`log`

​			v. 根据leader传过来的`LeaderCommit`和自己的`lastIndex`，更新自己的`commitindex`

- 3、收到回复，client的逻辑

  ​	i. 如果 reply 的 Term 比自己 Term 大，更新Term，并且转成follower，直接返回

  ​	ii. 如果收到reply的success为true，则更新`nextindex`和`matchindex`，

  ​	iii. 如果收到reply的success为false，则将重试的机制更改到下次，也就是不断的减少nextIndex[peer]，每次减1。

```go
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConfictTerm   int
	ConflictLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// rpc handler 通常是并发，加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// rpc handler 通用的规则: args 中 term 比 当前小，直接返回

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// rpc handler 通用规则: args 中的 term 比当前 server 大，当前 server 更新为 term，转成 follower
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
	}

	if len(rf.logs) <= args.PrevLogIndex {
		reply.ConfictTerm, reply.ConflictIndex, reply.ConflictLen = -1, -1, len(rf.logs)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		conflictIndex, conflictTerm := -1, rf.getLog(args.PrevLogIndex).Term
		for i := args.PrevLogIndex; i > rf.commitIndex; i-- {
			if rf.getLog(i).Term != conflictTerm {
				break
			}
			conflictIndex = i
		}
		reply.ConfictTerm, reply.ConflictIndex, reply.ConflictLen = conflictTerm, conflictIndex, len(rf.logs)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
```

- 为了减少重试的次数，即优化`AppendEntries RPC`被拒绝的次数，在返回的结构体中加入一个字段

​	1、当follower中的logs长度小于leader，即小于`PrevLogIndex`的时候，直接使用`follower`的`lastLogIndex`作为下一次的匹配点。

​	2、当follower的`lastIndexTerm`不等于leader的`lastLogTerm`，直接使用上一个Term的位置进行匹配。

```GO

	if len(rf.logs) <= args.PrevLogIndex {
		reply.ConfictTerm, reply.ConflictIndex, reply.ConflictLen = -1, -1, len(rf.logs)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		conflictIndex, conflictTerm := -1, rf.getLog(args.PrevLogIndex).Term
		for i := args.PrevLogIndex; i > rf.commitIndex; i-- {
			if rf.getLog(i).Term != conflictTerm {
				break
			}
			conflictIndex = i
		}
		reply.ConfictTerm, reply.ConflictIndex, reply.ConflictLen = conflictTerm, conflictIndex, len(rf.logs)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
```

收到回复的代码：

```go
func (rf *Raft) sendAppendLogsToAll() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[peer] - 1,
			PrevLogTerm:  0,
			Entries:      make([]Entry, 0),
			LeaderCommit: rf.commitIndex,
		}
		/*解决高并发场景下lab2C里索引越界的问题*/
		if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.logs) {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		} else {
			args.PrevLogIndex = 0
		}
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
		// deep copy
		args.Entries = append(args.Entries, rf.logs[rf.nextIndex[peer]:]...)
		go func(peer int) {
			reply := &AppendEntriesReply{}
			// 这个检查是因为当一个协程收到比当前 term 大的 reply，会转变成 follower. 后面协程不应该再以 leader 身份发送同步信息
			if ok := rf.sendAppendEntries(peer, args, reply); !ok {
				return
			}
            
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 所有 server 的规则
			if rf.state != StateLeader {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				rf.ChangeState(StateFollower)
				rf.persist()
				return
			}
			// double check
			 if rf.state != StateLeader || reply.Term != rf.currentTerm || reply.Term != args.Term {
			 	return
			 }
			if reply.Success {
				// rf.nextIndex[i] += len(args.Entries) 走过的一个 bug
				// rf.matchIndex[i] = rf.nextIndex[i] - 1
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				N := rf.commitIndex
				for _N := rf.commitIndex + 1; _N < len(rf.logs); _N++ {
					succeedNum := 0
					for i := 0; i < len(rf.peers); i++ {
						if _N <= rf.matchIndex[i] && rf.getLog(_N).Term == rf.currentTerm {
							succeedNum++
						}
					}
					if succeedNum > len(rf.peers)/2 {
						N = _N
					}
				}
				if N > rf.commitIndex {
					rf.commitIndex = N
					rf.replicatorCond[rf.me].Signal()
				}

			} else {
				// 实际上把重试的机制推到了下次心跳
                //当follower的logs小于PrevLogIndex的时候
				if reply.ConfictTerm == -1 && reply.ConflictIndex == -1 {
					rf.nextIndex[peer] = reply.ConflictLen
					return
				}
				ok := false

				for i, entry := range rf.logs {
					if entry.Term == reply.ConfictTerm {
						ok = true
						rf.nextIndex[peer] = i
					}
				}
				if !ok {
					rf.nextIndex[peer] = reply.ConflictIndex
				}

			}
		}(peer)
	}
}
```

- 优化点2：
  - 定期去检查commiteIndex+1的log是否复制到大多的机器上。
  - 定期去检查存在已经commit的log没有应用到上层应用中。
- 在收到回复后，需要应用到应用层

```go
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.replicatorCond[rf.me].L.Lock()
		rf.replicatorCond[rf.me].Wait()
		rf.replicatorCond[rf.me].L.Unlock()

		rf.mu.Lock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i,
			}
			rf.lastApplied++
		}
		rf.mu.Unlock()
	}
}
```

## 2、Lab2-C （ 持久性）



### 2.1 持久化的目的

- 持久化state，使得各个节点重启能够恢复原来的状态

### 2.2持久化的实现

- 需要持久化的字段

  - currentTerm：当前的任期
  - voteFor：避免在同一个任期内投两次票
  - logs：日志，重启恢复的时候需要重新执行一次命令

  ```go
  func (rf *Raft) readPersist(data []byte) {
  	if data == nil || len(data) < 1 { // bootstrap without any state?
  		return
  	}
      
  	status := new(PersistentStatus)
  
  	if err := labgob.NewDecoder(bytes.NewBuffer(data)).Decode(status); err != nil {
  		return
  	}
  	rf.logs = status.Logs
  	rf.currentTerm = status.CurrentTerm
  	rf.voteFor = status.VotedFor
  }
  ```

  持久化的代码

```go
func (rf *Raft) persist() {
	status := &PersistentStatus{
		Logs:        rf.logs,
		VotedFor:    rf.voteFor,
		CurrentTerm: rf.currentTerm,
	}
	w := new(bytes.Buffer)
	if err := labgob.NewEncoder(w).Encode(status); err != nil {
		return
	}
	rf.persister.SaveRaftState(w.Bytes())

}
```

需要加入持久化的地方

- 修改了节点状态的地方，`ChangState`的地方
- `Start()`，增加Entry的地方
- RPC hander收到`args`的`Term`比当前大，需要更新`Term`的地方
- RPC reply的Term比当前Term大，需要更新Term的时候
- 转变成follower，重置`voteFor`，一般是`reply.Term > rf.currentTerm`，需要将leader的state更改为follower的时候
- `AppenEntries`中append log的时候

