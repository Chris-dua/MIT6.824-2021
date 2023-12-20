package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
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
	// 2B

	logs        []Entry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond

	//2D
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte //保存最近的快照
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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

type PersistentStatus struct {
	Logs              []Entry
	CurrentTerm       int
	VotedFor          int
	LastIncludedIndex int
	LastIncludedTerm  int
}

const (
	HeartbeatTimeout = 50
	ElectionTimeout  = 300
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func newRandomSource() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func RandomizedElectionTimeout() time.Duration {
	//randSource := newRandomSource()
	return time.Duration(ElectionTimeout+(rand.Int63()%ElectionTimeout)) * time.Millisecond
}

func (rf *Raft) isLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	return lastLogTerm > rf.lastLogTerm() || (lastLogTerm == rf.lastLogTerm() && lastLogIndex >= rf.lastLogIndex())
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.logs) - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.logs) < 2 {
		return rf.lastIncludedTerm
	}
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) ChangeState(state NodeState) {
	if state == StateLeader {
		rf.state = StateLeader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.realLogLenth()
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
		rf.nextIndex[rf.me] = rf.lastLogIndex() + 1
		rf.matchIndex[rf.me] = rf.lastLogIndex()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
		//log.Printf("server: %v change state to Leader", rf.me)
		//log.Print(" ")
		go rf.sendAppendLogsToAll()
	} else if state == StateCandidate {
		rf.state = StateCandidate
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.electionTimer.Reset(RandomizedElectionTimeout())

	} else if state == StateFollower {
		rf.state = StateFollower
		//log.Printf("server: %v change state to Follower", rf.me)
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLog(index int) Entry {
	return rf.logs[index]
}

func (rf *Raft) logIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) realIndex(index int) int {
	return rf.lastIncludedIndex + index
}

func (rf *Raft) realLogLenth() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term, isleader = rf.currentTerm, rf.state == StateLeader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.persister.SaveStateAndSnapshot(rf.PersistStatusBytes(), rf.snapshot)

}

func (rf *Raft) PersistStatusBytes() []byte {
	status := &PersistentStatus{
		Logs:              rf.logs,
		VotedFor:          rf.voteFor,
		CurrentTerm:       rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	w := new(bytes.Buffer)
	if err := labgob.NewEncoder(w).Encode(status); err != nil {
		return nil
	}
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	//log.Printf("---------------- restore server persisted state -----------------")
	persistentStatus := &PersistentStatus{}
	if err := labgob.NewDecoder(bytes.NewBuffer(data)).Decode(persistentStatus); err != nil {
		return
	}
	rf.logs = persistentStatus.Logs
	rf.currentTerm = persistentStatus.CurrentTerm
	rf.voteFor = persistentStatus.VotedFor

	rf.lastIncludedIndex = persistentStatus.LastIncludedIndex
	rf.lastIncludedTerm = persistentStatus.LastIncludedTerm
	//rf.logs[0].Term = persistentStatus.LastIncludedTerm
	rf.commitIndex = persistentStatus.LastIncludedIndex
	rf.lastApplied = persistentStatus.LastIncludedIndex
	rf.snapshot = rf.persister.ReadSnapshot()
	//log.Printf("rf.logs: %+v, rf.lastIncludedIndex: %v, rf.lastIncludedTerm: %v", rf.logs, rf.lastIncludedIndex, rf.lastIncludedTerm)
	//log.Printf("---------------- finsihed restore server persisted state -----------------")

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	/* rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied > lastIncludedIndex || lastIncludedIndex < rf.lastIncludedIndex {
		return false
	}
	defer rf.persist()
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.snapshot = snapshot
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	for i := 1; i < len(rf.logs); i++ {
		if rf.realIndex(i) == lastIncludedIndex && rf.logs[i].Term == lastIncludedTerm {
			rf.logs = append([]Entry{{Term: lastIncludedTerm}}, rf.logs[i+1:]...)
			return true
		}
	}

	rf.logs = append([]Entry{{Term: lastIncludedTerm}}) */

	//log.Printf("server: %v, rf.lastIncludedTerm: %v, rf.lastIncludedIndex: %v", rf.me, rf.lastIncludedTerm, rf.lastIncludedIndex)
	/* if lastIncludedIndex > rf.lastIncludedIndex+len(rf.logs)-1 {
		rf.logs = append([]Entry{{Term: lastIncludedTerm}})
	} else {
		rf.logs = append([]Entry{{Term: rf.logs[rf.logIndex(lastIncludedIndex)].Term}}, rf.logs[rf.logIndex(lastIncludedIndex)+1:]...)
	}
	*/

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("server: %v, lastapplier: %v, new lastIncludeindex: %v", rf.me, rf.lastApplied, index)
	if index > rf.lastApplied || index <= rf.lastIncludedIndex {
		return
	}
	defer rf.persist()
	//log.Printf("==== Begin send snapshot ====")
	split := rf.logIndex(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[split].Term
	rf.logs = append([]Entry{{Term: rf.logs[split].Term}}, rf.logs[split+1:]...)
	rf.snapshot = snapshot
	/* if rf.state == StateFollower {
		log.Printf("server is follower")
	} else if rf.state == StateLeader {
		log.Printf("server is leader")
	} else {
		log.Printf("server is condidate")
	}
	log.Printf("server %v get the lastIncludeIndex: %v from applier", rf.me, index)
	log.Print("==== finish send snapshot ====")
	log.Printf(" ") */

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/* defer DPrintf(
	"{Node %v}'s state is {state %v, term %v, commitIndex %v, lastApplied %v, "+
		"firstLog %v, lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v",
	rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(),
	request, response) */
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	defer rf.persist()
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

	//log.Printf("Node %v reset electiontime and voted for %v", rf.me, rf.voteFor)
	reply.Term, reply.VoteGranted = rf.currentTerm, false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//log.Printf("========== follower %v begin add entries =============", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}

	rf.logs = append(rf.logs, Entry{
		Command: command,
		Term:    rf.currentTerm,
	})

	rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = rf.lastLogIndex(), rf.realLogLenth()
	index, term, isLeader = rf.lastLogIndex(), rf.currentTerm, true

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(StateCandidate)
			//log.Printf("election Time Out, Node %v Start Election", rf.me)

			rf.StartElection()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				go rf.sendAppendLogsToAll()
				rf.electionTimer.Reset(RandomizedElectionTimeout())
			}
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(HeartbeatTimeout+rand.Int()%ElectionTimeout) * time.Microsecond)

	}
}

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
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//log.Printf("Node %v receives RequestVoteResponse %v from Node %v after sending RequestVoteArg %+v in term %v",
				//	rf.me, response, peer, request, rf.currentTerm)
				if rf.currentTerm == args.Term && rf.state == StateCandidate {
					if reply.VoteGranted {
						grantedVote++
						if grantedVote > len(rf.peers)/2 {
							// log.Printf("Node %v receives majority votes in terms %v", rf.me, rf.currentTerm)
							// 成为leader，并且发送AppendEntries心跳
							rf.ChangeState(StateLeader)
						}
					}
				} else if reply.Term > rf.currentTerm {
					/* DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v",
					rf.me, peer, response.Term, rf.currentTerm) */

					rf.ChangeState(StateFollower)
					rf.currentTerm, rf.voteFor = reply.Term, -1
					rf.persist()
				}
			}
		}(peer)
	}
}

// 包括: start 和 end, 深拷贝，不然极限情况下会有 race 的 bug
func (rf *Raft) subLog(start int, end int) []Entry {
	if start == -1 {
		return append([]Entry{}, rf.logs[:end+1]...)
	} else if end == -1 {
		return append([]Entry{}, rf.logs[start:]...)
	} else {
		return append([]Entry{}, rf.logs[start:end]...)
	}
}

/* func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		if rf.state != StateLeader {
			return
		}
		rf.sendAppendLogsToAll()

		time.Sleep(HeartbeatTimeout * time.Millisecond)
	}
} */

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.replicatorCond[rf.me].L.Lock()
		rf.replicatorCond[rf.me].Wait()
		rf.replicatorCond[rf.me].L.Unlock()

		rf.mu.Lock()
		//log.Printf("================== Server %v get the applier signal begin to add lastApplied ================", rf.me)
		// log.Printf("rf.commitIndex = %v, rf.lastApplied = %v", rf.commitIndex, rf.lastApplied)
		msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{
				CommandValid:  true,
				Command:       rf.logs[rf.logIndex(i)].Command,
				CommandIndex:  i,
				SnapshotValid: false,
			})
			//log.Printf("server %v, lastapplier: %v, commandIndex: %v, command: %v,", rf.me, rf.lastApplied, i, rf.logs[rf.logIndex(i)].Command)
		}
		//log.Printf("================== Server %v get the applier signal begin to add lastApplied ================", rf.me)
		//log.Printf(" ")
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}

	}
}

func (rf *Raft) sendAppendLogsToAll() {
	//log.Printf("%d 在 term %d 发起同步", rf.me, rf.currentTerm) // 这里 CurrentTerm 没加锁，可能会有 race
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {

			// 需要发送最新的快照
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != StateLeader {
				return
			}
			//log.Printf("begin....")

			if rf.nextIndex[peer] <= rf.lastIncludedIndex {

				args := &InstallSnapShotArgs{
					Term:              rf.currentTerm,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					LeaderId:          rf.me,
					Data:              rf.snapshot,
				}
				if rf.state != StateLeader {
					return
				}
				go rf.handleSendInstallSnapshot(peer, args)
			} else {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PrevLogTerm:  0,
					Entries:      make([]Entry, 0),
					LeaderCommit: rf.commitIndex,
				}

				/* 	//解决高并发场景下lab2C里索引越界的问题
				if args.PrevLogIndex > rf.lastIncludedIndex && args.PrevLogIndex < rf.lastIncludedIndex+len(rf.logs) {
					args.PrevLogTerm = rf.logs[rf.logIndex(args.PrevLogIndex)].Term
				} else if args.PrevLogIndex == rf.lastIncludedIndex {
					args.PrevLogIndex = rf.logs[0].Term
				}

				// deep copy
				args.Entries = append(args.Entries, rf.logs[rf.logIndex(rf.nextIndex[peer]):]...) */
				if args.PrevLogIndex > rf.lastIncludedIndex &&
					args.PrevLogIndex < rf.lastIncludedIndex+len(rf.logs) /*下一个日志在leader log里，且前一个日志没在快照里，也在leader log里*/ {
					args.PrevLogTerm = rf.logs[rf.logIndex(args.PrevLogIndex)].Term
				} else if args.PrevLogIndex == rf.lastIncludedIndex /*下一个日志在leader log里，但上一个日志在快照里，没在leader log里*/ {
					//args.PrevLogIndex = rf.lastIncludedIndex
					args.PrevLogTerm = rf.logs[rf.logIndex(rf.lastIncludedIndex)].Term
				}

				//deep copy

				args.Entries = append(args.Entries, rf.logs[rf.logIndex(rf.nextIndex[peer]):]...)

				//log.Printf("leader %v send follower %v args: %+v", rf.me, peer, args)
				go rf.handleSendAppendEntries(peer, args)
			}
			log.Printf("leader: %v, matchIndex[%v]: %v, nextIndex[%v]: %v", rf.me, peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])
		}(peer)

	}

}

func (rf *Raft) handleSendInstallSnapshot(peer int, args *InstallSnapShotArgs) {
	reply := &InstallSnapShotReply{}
	//log.Printf("leader %v send follower %v args: %+v", rf.me, peer, args)
	if ok := rf.sendInstallSnapshot(peer, args, reply); ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		//prevLogIndex := rf.nextIndex[peer] - 1
		//log.Printf("======== follower %v begin install snapshot ========", peer)
		//log.Printf("old data leader: %v, matchIndex[%v]: %v, nextIndex[%v]: %v", rf.me, peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])
		//log.Printf("leader: %v, Term: %v, prevLogIndex: %v, lastIncludeIndex: %v", rf.me, rf.currentTerm, prevLogIndex, rf.lastIncludedIndex)
		if rf.currentTerm != args.Term || rf.state != StateLeader {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.ChangeState(StateFollower)
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
		//log.Printf("new data leader: %v, matchIndex[%v]: %v, nextIndex[%v]: %v", rf.me, peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])
		//log.Printf("======== follower %v install finished! ========", peer)
		//log.Print(" ")

	}

}

func (rf *Raft) handleSendAppendEntries(peer int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	//log.Printf("args: %+v", args)
	//log.Printf("Leader %v send follower %v to Append Entries", rf.me, peer)

	//log.Printf("the matchIndex[%v] = %v, the nextIndex[%v] = %v", peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])

	if ok := rf.sendAppendEntries(peer, args, reply); ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()
		//prevLogIndex := rf.nextIndex[peer] - 1
		//log.Printf("======== follower %v begin update entries ========", peer)
		//log.Printf("old data leader: %v, matchIndex[%v]: %v, nextIndex[%v]: %v", rf.me, peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])
		//log.Printf("leader: %v, Term: %v, prevLogIndex: %v, lastIncludeIndex: %v", rf.me, rf.currentTerm, prevLogIndex, rf.lastIncludedIndex)
		if rf.state != StateLeader || rf.currentTerm != args.Term {
			return
		}
		//log.Printf("reply: %+v", reply)
		if reply.Term > rf.currentTerm {
			rf.currentTerm, rf.voteFor = reply.Term, -1
			rf.ChangeState(StateFollower)
			rf.persist()
			return
		}
		// double check
		// if rf.state != StateLeader || reply.Term != rf.currentTerm || reply.Term != args.Term {
		// 	return
		// }
		if reply.Success {
			// rf.nextIndex[i] += len(args.Entries) 走过的一个 bug
			// rf.matchIndex[i] = rf.nextIndex[i] - 1
			//log.Printf("rf.matchIndex[rf.me] = %v, rf.nextIndex[rf.me] = %v", rf.matchIndex[rf.me], rf.nextIndex[rf.me])
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			rf.cheakAndCommitLogs()

		} else {
			rf.findNextIndex(peer, reply)
		}
		//log.Printf("=== End leader nextIndex %+v, matchIndex %+v ===", rf.nextIndex, rf.matchIndex)
		//log.Printf("new data leader: %v, matchIndex[%v]: %v, nextIndex[%v]: %v", rf.me, peer, rf.matchIndex[peer], peer, rf.nextIndex[peer])
		//log.Printf("========= follow %v finished the entries update ========", peer)
		//log.Print(" ")
	}
}

func (rf *Raft) cheakAndCommitLogs() {
	N := rf.commitIndex
	for _N := rf.commitIndex + 1; _N < rf.lastIncludedIndex+len(rf.logs); _N++ {
		succeedNum := 0
		for i := 0; i < len(rf.peers); i++ {
			if _N <= rf.matchIndex[i] && rf.logs[_N-rf.lastIncludedIndex].Term == rf.currentTerm {
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
}

func (rf *Raft) findNextIndex(peer int, reply *AppendEntriesReply) {
	// 实际上把重试的机制推到了下次心跳
	if reply.ConfictTerm == -1 && reply.ConflictIndex == -1 {
		rf.nextIndex[peer] = reply.ConflictLen
		return
	}
	ok := false

	for i, entry := range rf.logs {
		if entry.Term == reply.ConfictTerm {
			ok = true
			rf.nextIndex[peer] = rf.lastIncludedIndex + i
		}
	}
	if !ok {
		rf.nextIndex[peer] = reply.ConflictIndex
	}
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// rpc handler 通常是并发，加锁

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("========== follower %v begin add entries =============", rf.me)
	//log.Printf("%d 在 term %d 收到 %d 的 AppendEntries request %+v", rf.me, rf.currentTerm, args.LeaderID, args)
	// rpc handler 通用的规则: args 中 term 比 当前小，直接返回

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
	}

	// rpc handler 通用规则: args 中的 term 比当前 server 大，当前 server 更新为 term，转成 follower
	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	//log.Printf("leaderId: %v, follower: %v, rf.lastIncludedIndex: %v args.PrevLogIndex: %v", args.LeaderID, rf.me, rf.lastIncludedIndex, args.PrevLogIndex)

	if rf.lastIncludedIndex > args.PrevLogIndex {
		//"rf.lastIncludedIndex: %v > args.PrevLogIndex: %v", rf.lastIncludedIndex, args.PrevLogIndex)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.ConfictTerm, reply.ConflictIndex, reply.ConflictLen = -1, -1, rf.realLogLenth()
		reply.Term, reply.Success = rf.currentTerm, false
		//log.Print("rf.lastLogIndex() < args.PrevLogIndex")
		return
	}

	index := rf.logIndex(args.PrevLogIndex)
	if rf.logs[index].Term != args.PrevLogTerm {
		//log.Printf("len(rf.logs) = %v, rf.logs: %+v, logIndex: %v, args.Term: %v,args.PrevIndex = %v", len(rf.logs), rf.logs, index, args.Term, args.PrevLogIndex)
		//log.Printf("rf.logs[index].Term != args.PrevLogTerm")
		conflictIndex, conflictTerm := -1, rf.logs[index].Term
		for i := args.PrevLogIndex; i > rf.commitIndex; i-- {
			if rf.logs[rf.logIndex(i)].Term != conflictTerm {
				break
			}
			conflictIndex = i
		}
		reply.ConfictTerm, reply.ConflictIndex, reply.ConflictLen = conflictTerm, conflictIndex, rf.realLogLenth()
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	//log.Printf("leader Node, term: %v, %v and follower Node, term: %v, %v", args.LeaderID, args.Term, rf.me, rf.currentTerm)

	// 删除比 leader 多的 log
	/* rf.logs = rf.subLog(-1, args.PrevLogIndex)
	// 新增的 Entries 放入
	rf.logs = append(rf.logs, args.Entries...)
	// 更新成 Follower 的 commit */

	/** for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index < len(rf.logs) { //重叠
			if rf.logs[index].Term != entry.Term { //看是否发生冲突
				rf.logs = rf.logs[:index]        // 删除当前以及后续所有log
				rf.logs = append(rf.logs, entry) // 把新log加入进来
			}
			//没有冲突，那么就不添加这个重复的log
		} else if index == len(rf.logs) { //没有重叠，且刚好在下一个位置
			rf.logs = append(rf.logs, entry)
		}
	} */

	for i, entry := range args.Entries {
		index = rf.logIndex(args.PrevLogIndex + 1 + i)
		if index < len(rf.logs) { /*重叠*/
			if rf.logs[index].Term != entry.Term { /*看是否发生冲突*/
				rf.logs = rf.logs[:index]        // 删除当前以及后续所有log
				rf.logs = append(rf.logs, entry) // 把新log加入进来
			}
			/*没有冲突，那么就不添加这个重复的log*/
		} else if index == len(rf.logs) { /*没有重叠，且刚好在下一个位置*/
			rf.logs = append(rf.logs, entry)
		}
	}
	//log.Printf("follower %d add the Entry %v success!", rf.me, args.Entries)
	//log.Printf("follower commit Index: %v, leader commit Index: %v", rf.commitIndex, args.LeaderCommit)
	if args.LeaderCommit > rf.commitIndex {

		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())

		rf.replicatorCond[rf.me].Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
	//log.Printf("========== follower %v finish add entries =============", rf.me)
	//log.Print(" ")
	return
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok

}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("================ follower %v begin to install Snapshot =====================", rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		return
	}

	defer rf.persist()

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	for i := 1; i < len(rf.logs); i++ {
		if rf.realIndex(i) == args.LastIncludedIndex && rf.logs[i].Term == args.LastIncludedTerm {
			rf.logs = append([]Entry{{Term: args.LastIncludedTerm}}, rf.logs[i+1:]...)
			go func() {
				rf.applyCh <- msg
			}()

			reply.Term = rf.currentTerm
			return
		}
	}

	rf.logs = append([]Entry{{Term: args.LastIncludedTerm}})

	go func() {
		rf.applyCh <- msg
	}()

	reply.Term = rf.currentTerm
	//log.Printf("follower %v reply leader %v :%+v, args.Term: %v", rf.me, args.LeaderId, reply, args.Term)
	//log.Printf("================ follower %v finish to install Snapshot =====================", rf.me)
	//log.Print(" ")
	return

}

/* func (rf *Raft) BroadcastHeartbeat(isHeartBeart bool){
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeart {
			go rf.replicateOneRound(peer)
		}else {
			rf.replicatorCond[peer].Signal()
		}
	}
}
// leader 发送请求给 follower
func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state  != StateLeader {

	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if reply.Term  > rf.currentTerm {
		rf.currentTerm, rf.voteFor = reply.Term, -1
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	// 如果leader安装里snapshot，会出现rf.log.GetFirstLog() > PrevLogIndex的情况


	reply.Term, reply.Success = rf.currentTerm, true

} */

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		voteFor:        -1,
		logs:           make([]Entry, 1),
		heartbeatTimer: time.NewTicker(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		commitIndex:    0,
		lastApplied:    0,
	}
	// Your initialization code here (2A, 2B, 2C).
	//log.Printf("persister State %+v:", persister.ReadRaftState())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//rf.applyCond = sync.NewCond(&rf.mu)
	//lastLog := rf.getLastLog()

	for i := 0; i < len(peers); i++ {
		rf.replicatorCond[i] = sync.NewCond((&sync.Mutex{}))
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
