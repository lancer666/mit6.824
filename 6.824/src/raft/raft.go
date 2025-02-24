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
	"../labgob"
	"bytes"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

// raft server的当前状态
type ServerState int

// 枚举server状态类型
const (
	Follower  ServerState = iota // 跟随者
	Candidate                    // 候选者
	Leader                       // 领导者
)

// 日志条目结构体
type LogEntry struct {
	Command interface{} // 客户端要求的指令
	Term    int         // 此日志条目的term
	Index   int         // 此日志条目的index
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool // 当ApplyMsg用于apply指令时为true，其余时候为false
	Command      interface{}
	CommandIndex int
	CommandTerm  int // 指令执行时的term，便于kvserver的handler比较

	SnapshotValid     bool   // 当ApplyMsg用于传快照时为true，其余时候为false
	SnapshotIndex     int    // 本快照包含的最后一个日志的index
	StateMachineState []byte // 状态机状态，就是快照数据
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        // Raft server的当前任期
	votedFor    int        // 此server当前任期投票给的节点的ID。如果还没有投票，就为-1
	leaderId    int        // 该raft server知道的最新的leader id，初始为-1
	log         []LogEntry // 此Server的日志，包含了若干日志条目，类型是日志条目的切片，第一个日志索引是1

	// 所有servers上易变的状态
	commitIndex int // 已知的已提交的日志的最大index
	lastApplied int // 应用到状态机的日志的最大index

	// leader上易变的状态（在选举后被重新初始化）
	nextIndex  []int // 对于每一个server来说，下一次要发给对应server的日志项的起始index（初始化为leader的最后一个日志条目index+1）
	matchIndex []int // 对于每一个server来说，已知成功复制到该server的最高日志项的index（初始化为0,且单调递增）

	state  ServerState   // 这个raft server当前所处的角色/状态
	timer  *time.Timer   // 计时器指针
	ready  bool          // 标志candidate是否准备好再次参加选举，当candidate竞选失败并等待完竞选等待超时时间后变为true
	hbTime time.Duration // 心跳间隔（要求每秒心跳不超过十次）

	applyCh chan ApplyMsg //  根据Make()及其他部分的注释，raft server需要维护一个发送ApplyMsg的管道

	lastIncludedIndex int // 上次快照替换的最后一个条目的index
	lastIncludedTerm  int // 上次快照替换的最后一个条目的term

	passiveSnapshotting bool // 该raft server正在进行被动快照的标志（若为true则这期间不进行主动快照）
	activeSnapshotting  bool // 该raft server正在进行主动快照的标志（若为true则这期间不进行被动快照）

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// 根据Figure2来确定应该持久化的变量，对它们编码
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	//e.Encode(rf.lastIncludedIndex)
	//e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Raft server %d readPersist ERROR!\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		//rf.lastIncludedIndex = lastIncludedIndex
		//rf.lastIncludedTerm = lastIncludedTerm
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate的当前任期
	CandidatedId int // 请求投票的candidate的ID
	LastLogIndex int // candidate最后一个日志条目的index（确保安全性的选举限制用）
	LastLogTerm  int // candidate最后一个日志条目的term（确保安全性的选举限制用）
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm，用来更新candidate的term（如果需要的话）
	VoteGranted bool // 当candidate收到这张选票时为true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 见Figure2 "RequestVote RPC"
	// 根据Figure2的说明，当候选者term < 投票者的current term时直接返回false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm // 将自己的current term附在回复中
		return
	}

	// 候选者term >= 投票者的current term的情况，需要进一步判断
	// Raft通过比较最后一个日志条目的index和term来决定两个日志哪个更新

	// 见Figure2 "Rules for Servers“
	// 当RPC请求或回复中的term > 当前server的current term时
	// 1. set currentTerm = T
	// 2. 转换到follower状态
	if rf.currentTerm < args.Term {
		// 下台并采用更高的任期，这会重新设置votedFor，就拥有新任期的投票权
		rf.votedFor = -1           // 当term发生变化时，需要重置votedFor
		rf.leaderId = -1           // term改变，leaderId也要重置
		rf.state = Follower        // 变回Follower
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}

	// 判断candidate的log是否至少与这个待投票的server的log一样新，见论文5.4节，两个规则
	// 如果日志的最后条目具有不同的term，那么具有较后term的日志是更新
	// 如果日志的最后条目具有相同的term，那么哪个日志更长，哪个日志就更新
	uptodate := false
	voterLastLog := rf.log[len(rf.log)-1] // 获取投票者最后一个日志条目（如果是空日志，则获取到的是初始化时加在下标0的“占位”元素）

	// args.LastLogIndex >= voterLastLog.index需要加等号是因为第一届leader选举过程中，两个server都还没有日志
	// 它们比较的最后一个日志的term和index事实上都是初始化加入的占位元素的term和index，即term -1 == -1， index 0 == 0
	// 显然这种情况投票者是会投票给这个candidate的
	if (args.LastLogTerm > voterLastLog.Term) || (args.LastLogTerm == voterLastLog.Term && args.LastLogIndex >= voterLastLog.Index) {
		uptodate = true
	}

	// 见Figure2 "RequestVote RPC"
	if (rf.votedFor == -1 || rf.votedFor == args.CandidatedId) && uptodate { // 投票给这个candidate
		rf.votedFor = args.CandidatedId
		rf.leaderId = -1 // 你投票了，说明你不信之前的leader了
		reply.VoteGranted = true
		rf.persist()
		rf.timer.Stop()
		rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)
	} else { // 不符合的不予投票
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm // 其实这里reply.Term就等于请求投票的candidate的term
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
// 应用层传输指令到下层raft集群共识的入口
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState() // 直接调用GetState()来获取该server的当前任期以及是否为leader

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.log[len(rf.log)-1].Index + 1 // index：如果这条日志最后被提交，那么它将在日志中的索引（注意索引0处的占位元素也算在切片len里面）

	if isLeader == false { // 如果这个Server不是leader则直接返回false，不继续执行后面的追加日志
		return term, term, false
	}

	// 如果是leader则准备开始向其他server复制日志
	newLog := LogEntry{ // 将指令形成一个新的日志条目
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.log = append(rf.log, newLog) // leader首先将新日志条目追加到自己的日志中
	rf.persist()
	DPrintf("[Start]Client sends a new commad(%v) to Leader %d!\n", command, rf.me)
	// 客户端发来新的command，复制日志到各server，调用LeaderAppendEntries()
	// 如果追加失败（网络问题或日志不一致被拒绝），则重复发送由携带日志条目的周期性的心跳包来完成
	go rf.LeaderAppendEntries() // 由新日志触发AppendEntries RPC的发送
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	var mutex sync.Mutex
	rf.mu = mutex
	rf.currentTerm = 0
	rf.state = Follower // server刚开始为follower，且current term为0
	rf.votedFor = -1    // 初始时还没投票，就为-1
	rf.leaderId = -1    // 同上
	// 一开始没有日志条目
	// 由于合法日志索引从1开始，为了让日志index与切片下标对应，故先填充一个元素
	// 这个元素不是日志，term值和index值非法，在leader选举过程中也免去了单独讨论空日志的情况，不用怕空指针报错
	// 注意，这里占位日志的index应设为0而非其他值！！！
	rf.log = []LogEntry{{Term: -1, Index: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.ready = false
	rf.hbTime = 100 * time.Millisecond // 心跳间隔设为100ms一次
	rf.applyCh = applyCh

	// 一开始每个follower都会开始计时一个随机的选举超时时间，到点后如果没有收到leader或candidate的消息，则宣布竞选
	// 根据lab提示，选举超时设定应该比150ms~300ms更大但又不至于太大，这里选择250ms~400ms内的一个随机值
	// 因为限制心跳不超过每秒10次，且还要保证5s内选出leader
	// time.Duration(x) 是进行的类型转换，把整型x转换成了time.Duration类型
	rf.timer = time.NewTimer(time.Duration(getRandMS(300, 500)) * time.Millisecond)

	//rf.lastIncludedIndex = 0
	//rf.lastIncludedTerm = -1
	//
	//rf.passiveSnapshotting = false
	//rf.activeSnapshotting = false

	// initialize from state persisted before a crash
	// 从crash中恢复之前的状态
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnap(persister.ReadSnapshot()) // 从快照中恢复
	rf.persist()

	//DPrintf("Server %v (Re)Start and lastIncludedIndex=%v, rf.lastIncludedTerm=%v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 起一个goroutine循环处理超时
	go rf.HandleTimeout()

	// 起一个goroutine循环检查是否有需要应用到状态机日志
	go rf.applier()

	return rf

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
