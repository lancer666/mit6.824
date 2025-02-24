package raft

import (
	"sort"
	"sync"
	"time"
)

// 追加日志条目的RPC请求结构
// 心跳包是没有log entries的AppendEntries RPCs
type AppendEntriesArgs struct {
	Term         int // leader的任期
	LeaderId     int
	PreLogIndex  int        // 新条目之前的紧接着的日志的索引
	PreLogTerm   int        // 新条目之前的紧接着的日志的任期
	Entries      []LogEntry // 要存储/追加到server的日志条目，为了效率可一次追加多条（若为心跳包则此字段为空）
	LeaderCommit int        // leader提交到的日志索引位置
}

// 追加日志条目的RPC回复结构
type AppendEntriesReply struct {
	Term          int  // RPC接收server的current term，leader更新自己用（如果需要的话）
	Success       bool // 如果follower包含有匹配leader的preLogIndex以及preLogTerm的日志条目则返回true
	ConflictIndex int
	ConflictTerm  int
}

// 处理超时的协程，会一直循环检测超时
// 直至该server当选为leader后不再进行超时检测(因为leader只要不crash且一直保持与大多数成员的通信leader就不会下台，也就不需要超时检测)
func (rf *Raft) HandleTimeout() {
	for {
		select {
		case <-rf.timer.C: // 当timer超时后会向C中发送当前时间，此时case的逻辑就会执行，从而实现超时处理
			if rf.killed() { // 如果rf被kill了就不继续检测了
				return
			}
			rf.mu.Lock()
			nowState := rf.state // 记录下状态，以免switch访问rf.state时发送DATA RACE,若是整体加锁又可能引发死锁
			rf.mu.Unlock()

			switch nowState { // 根据当前的角色来判断属于哪种超时情况，执行对应的逻辑
			case Follower: // 如果是follower，则超时是因为一段时间没接收到leader的心跳或candidate的投票请求
				// 竞选前重置计时器（选举超时时间）
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)

				go rf.RunForElection() // follower宣布参加竞选
			case Candidate: // 如果是candidate，则超时是因为出现平票等造成上一任期竞选失败
				// 重置计时器
				// 对于刚竞选失败的candidate，这个计时是竞选失败等待超时设定
				// 对于已经等待完竞选失败等待超时设定的candidate，这个计时是选举超时设定
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)

				rf.mu.Lock()
				if rf.ready { // candidate等待完竞选等待超时时间准备好再次参加竞选
					rf.mu.Unlock()
					go rf.RunForElection()
				} else {
					rf.ready = true // candidate等这次超时后就可以再次参选
					rf.mu.Unlock()
				}
			case Leader: // 成为leader就不需要超时计时了，直至故障或发现自己的term过时
				return
			}

		}
	}
}

// 当follower一段时间没联系到leader时宣布自己成为candidate参加竞选，流程如下：
// 1. 自增current term
// 2. 给自己投一票
// 3. 重置选举计时器
// 4. 向所有其他servers发送请求投票RPC
func (rf *Raft) RunForElection() {
	rf.Convert2Candidate() // 转换成candidate宣布开始竞选

	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term，防止过程中rf.currentTerm改变导致args.Term不一致
	rf.ready = false           // 不管是follower第一次参选还是candidate再次参选，只要参加竞选就将ready设为false以便超时后的等待判定
	rf.mu.Unlock()

	// 使用条件变量来检查得到大多数票的条件
	votes := 1                    // 得票数（自己一开始给自己投的票要先算上，否则最后少了一张赞成票）
	finished := 1                 // 收到的请求投票回复数（自己的票也算）
	var voteMu sync.Mutex         // 用于保护votes和finished的锁
	cond := sync.NewCond(&voteMu) // 将条件变量与锁关联

	// candidate向除自己以外的其他server发送请求投票RPC
	for i, _ := range rf.peers { // i是目的server在rf.peers[]中的索引（id）

		if rf.killed() { // 如果在竞选过程中Candidate被kill了就直接结束
			return
		}

		rf.mu.Lock()
		if rf.state != Candidate { // 如果自己不再是Candidate则不继续请求投票
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		if i == rf.me { // 读到自己则跳过
			continue
		}

		// 利用协程并行地发送请求投票RPC，参考lec5的code示例vote-count-4.go
		go func(idx int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         sameTerm, // 此处用之前记录的currentTerm副本
				CandidatedId: rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}

			// 注意传的是args和reply的地址而不是结构体本身！
			ok := rf.sendRequestVote(idx, &args, &reply) // candidate向 server i 发送请求投票RPC
			if !ok {
				DPrintf("Candidate %d call server %d for RequestVote failed!\n", rf.me, idx)
			}

			// 如果candidate任期比其他server的小，则candidate更新自己的任期并转为follower，并跟随此server
			rf.mu.Lock()

			// 处理RPC回复之前先判断，如果自己不再是Candidate了则直接返回
			// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
			if rf.state != Candidate || rf.currentTerm != args.Term { // 注意第二个条件
				rf.mu.Unlock()
				return
			}

			if rf.currentTerm < reply.Term {
				rf.votedFor = -1            // 当term发生变化时，需要重置votedFor
				rf.state = Follower         // 变回Follower
				rf.currentTerm = reply.Term // 更新自己的term为较新的值
				rf.persist()
				rf.mu.Unlock()

				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)
				return // 这里只是退出了协程.
			}
			rf.mu.Unlock()

			vote := reply.VoteGranted // 查看是否收到选票。如果RPC发送失败，reply中的投票仍是默认值，相当于没收到投票
			voteMu.Lock()             // 访问votes和finished前加锁
			if vote {
				DPrintf("Candidate %d got a vote from server %d!\n", rf.me, idx)
				votes++
			}
			finished++
			voteMu.Unlock()
			cond.Broadcast() // // Broadcast 会清空队列，唤醒全部的等待中的 goroutine
		}(i) // 因为i随着for循环在变，因此将它作为参数传进去
	}

	sumNum := len(rf.peers)     // 集群中总共的server数
	majorityNum := sumNum/2 + 1 // 满足大多数至少需要的server数量

	voteMu.Lock() // 调用 Wait 方法的时候一定要持有锁
	// 检查是否满足“获得大多数选票”的条件
	for votes < majorityNum && finished != sumNum { // 投票数尚不够，继续等待剩余server的投票
		cond.Wait() // 调用该方法的 goroutine 会被放到 Cond 的等待队列中并阻塞，直到被 Signal 或者 Broadcast 方法唤醒

		// 当candidate收到比自己term大的rpc回复时它就回到follower，此时直接结束自己的竞选
		// 或者该candidate得不到多数票但又由于有server崩溃而得不到sumNum张选票而一直等待，此时只有当有leader出现并发送心跳让该candidate变回follower跳出循环
		// 所以每次都要检测该candidate是否仍然在竞选，如果它已经退选，就不用一直等待选票了
		rf.mu.Lock()
		stillCandidate := (rf.state == Candidate)
		rf.mu.Unlock()
		if !stillCandidate {
			voteMu.Unlock() // 如果提前退出，不要忘了把这个锁释放掉
			return
		}
	}

	if votes >= majorityNum { // 满足条件，直接当选leader
		rf.Convert2Leader() // 成为leader就不需要超时计时了，直至故障或发现自己的term过时
	} else { // 收到所有回复但选票仍不够的情况，即竞选失败
		DPrintf("Candidate %d failed in the election and continued to wait...\n", rf.me)
	}
	voteMu.Unlock()
}

// server转换状态成candidate
// 两种情况下会用到：
// 1. Follower未接到Leader的心跳或Candidate的投票请求直至超时，会转为candidate参加竞选，follower -> candidate
// 2. Candidate由于分票等原因本轮未选出leader，经过一个超时时间后参加下一轮竞选，candidate -> candidate
func (rf *Raft) Convert2Candidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate // 宣布自己成为candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderId = -1 // 参加竞选则leaderId更新为C待定（可之后更新为自己或其他server id）
	rf.persist()
	DPrintf("Candidate %d run for election! Its current term is %d\n", rf.me, rf.currentTerm)
}

// server转换状态成leader
// candidate收到大多数servers的投票后成功当选转为leader，candidate -> leader
func (rf *Raft) Convert2Leader() {
	rf.mu.Lock()
	DPrintf("Candidate %d was successfully elected as the leader! Its current term is %d\n", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.leaderId = rf.me // 成功当选，则leaderId是自己

	// leader上任时初始化nextIndex以及matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1 // 初始化为leader的最后一个日志条目index+1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0 // 初始化为0
	}
	rf.mu.Unlock()

	// 为了leader上任时获知之前commit到了哪里，让leader在其任期开始时先在日志中提交一个空白的无操作条目
	// 解决当前term无提交日志时不能服务读请求的问题
	// 但是这样的话会有部分lab 2的测试通过不了，原因是lab2的部分测试不会考虑空白日志占据一个日志位
	// 实际应用时可能需要应用层添加一个空白指令，此处为通过实验测试就不加了
	// rf.Start(0) // 代表空白指令

	// 起一个协程循环发送心跳包，心跳间隔为100ms
	go func() {
		for !rf.killed() { // leader没有被kill就一直发送
			rf.mu.Lock()
			stillLeader := (rf.state == Leader)
			rf.mu.Unlock()

			if stillLeader { // 如果leader仍然是leader
				go rf.LeaderAppendEntries()
				time.Sleep(rf.hbTime) // 心跳间隔
			} else { // 如果当前server不再是leader（变为了follower）则重启计时器并停止发送心跳包
				rf.timer.Stop()
				rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)
				go rf.HandleTimeout()
				return
			}
		}
	}()
}

// leader发送日志追加RPC或心跳包的相关逻辑
// leader调用LeaderAppendEntries()的两种情况：
// 1. 周期性发送心跳包
// 2. 客户端发来新的command，复制日志到各server
func (rf *Raft) LeaderAppendEntries() {
	rf.mu.Lock()
	sameTerm := rf.currentTerm // 记录rf.currentTerm的副本，在goroutine中发送RPC时使用相同的term
	// 由于leader永远不会覆盖或删除自己日志中的条目，因此matchIndex当然单调递增
	// 这里更新leader自己的matchIndex和nextIndex其实只是为了在判断“大多数复制成功”更新commitIndex时正确达成共识（毕竟leader不用往自己发送RPC复制日志）
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index // 更新leader自己的matchIndex，就等于其log最后一个日志条目的index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1     // 更新leader自己的nextIndex
	rf.mu.Unlock()

	// leader向除自己以外的其他server发送AppendEntries RPC
	for i, _ := range rf.peers { // i是目的server在rf.peers[]中的索引（id）

		if i == rf.me { // 读到自己则跳过
			continue
		}

		// 利用协程并行地发送AppendEntries RPC（包括心跳包）
		go func(idx int) {

			if rf.killed() { // 如果在发送AppendEntries RPC过程中leader被kill了就直接结束
				return
			}

			rf.mu.Lock()

			// 发送RPC之前先判断，如果自己不再是leader了则直接返回
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			appendLogs := []LogEntry{} // 若为心跳包则要追加的日志条目为空切片
			nextIdx := rf.nextIndex[idx]

			//if nextIdx <= rf.lastIncludedIndex { // 如果要追加的日志已经被截断了则向该follower发送快照
			//	go rf.LeaderSendSnapshot(idx, rf.persister.ReadSnapshot())
			//	rf.mu.Unlock()
			//	return
			//}

			// 根据Figure2的Leader Rule 3
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			// 如果leader的日志从nextIdx开始有要发送的日志，则此AppendEntries RPC需要携带从nextIdx开始的日志条目
			if rf.log[len(rf.log)-1].Index >= nextIdx {
				// copy:目标切片必须分配过空间且足够承载复制的元素个数，并且来源和目标的类型必须一致
				appendLogs = make([]LogEntry, len(rf.log)-nextIdx+rf.lastIncludedIndex)
				copy(appendLogs, rf.log[nextIdx-rf.lastIncludedIndex:]) // 将leader日志nextIdx及之后的条目复制到appendLogs
			}
			preLog := rf.log[nextIdx-rf.lastIncludedIndex-1] // preLog是leader要发给server idx的日志条目的前一个日志条目
			args := AppendEntriesArgs{
				Term:         sameTerm,
				LeaderId:     rf.me,
				PreLogIndex:  preLog.Index,
				PreLogTerm:   preLog.Term,
				Entries:      appendLogs, // 若为心跳包则要追加的日志条目为空切片，否则为携带日志的切片
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}

			DPrintf("Leader %d sends AppendEntries RPC(term:%d, Entries len:%d) to server %d...\n", rf.me, sameTerm, len(args.Entries), idx)
			// 注意传的是args和reply的地址而不是结构体本身！
			ok := rf.sendAppendEntries(idx, &args, &reply) // leader向 server i 发送AppendEntries RPC

			if !ok {
				// 如果由于网络原因或者follower故障等收不到RPC回复（不是follower将回复设为false）
				// 则leader无限期重复发送同样的RPC（nextIndex不前移），等到下次心跳时间到了后再发送
				DPrintf("Leader %d calls server %d for AppendEntries or Heartbeat failed!\n", rf.me, idx)
				return
			}

			// 如果leader收到比自己任期更大的server的回复，则leader更新自己的任期并转为follower，跟随此server
			rf.mu.Lock() //要整体加锁，不能只给if加锁然后解锁
			defer rf.mu.Unlock()

			// 处理RPC回复之前先判断，如果自己不再是leader了则直接返回
			// 防止任期混淆（当收到旧任期的RPC回复，比较当前任期和原始RPC中发送的任期，如果两者不同，则放弃回复并返回）
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}

			if rf.currentTerm < reply.Term {
				rf.votedFor = -1            // 当term发生变化时，需要重置votedFor
				rf.state = Follower         // 变回Follower
				rf.currentTerm = reply.Term // 更新自己的term为较新的值
				rf.persist()
				return // 这里只是退出了协程
			}

			// 如果出现follower的日志与leader的不一致，即append失败
			if reply.Success == false { // follower拒绝接受日志的情况（不一致）
				possibleNextIdx := 0 // 可能的nextIndex[idx]

				if reply.ConflictTerm == -1 { // 如果follower的日志中没有prevLogIndex
					possibleNextIdx = reply.ConflictIndex // 这里需要提前判断节省时间，否则后面2C部分测试会FAIL
				} else {
					foundConflictTerm := false

					// 从后往前找
					k := len(rf.log) - 1
					for ; k > 0; k-- {
						if rf.log[k].Term == reply.ConflictTerm {
							foundConflictTerm = true
							break
						}
					}

					if foundConflictTerm {
						possibleNextIdx = rf.log[k+1].Index // 若找到了对应的term，则找到对应term出现的最后一个日志条目的下一个日志条目
					} else {
						possibleNextIdx = reply.ConflictIndex
					}

				}
				if possibleNextIdx < rf.nextIndex[idx] && possibleNextIdx > rf.matchIndex[idx] {
					rf.nextIndex[idx] = possibleNextIdx
				} else { // 若不满足则视为过时，舍弃掉这次RPC回复
					return
				}

			} else { // 若追加成功
				// 更新对应follower的nextIndex和matchIndex

				// 根据guide，你不能假设server的状态在它发送RPC和收到回复之间没有变化。
				// 因为可能在这期间收到新的指令而改变了log和nextIndex
				// 通常 nextIndex = matchIndex + 1
				possibleMatchIdx := args.PreLogIndex + len(args.Entries)
				rf.matchIndex[idx] = max(possibleMatchIdx, rf.matchIndex[idx]) // 保证matchIndex单调递增，因为不可靠网络下会出现RPC延迟
				rf.nextIndex[idx] = rf.matchIndex[idx] + 1                     // matchIndex安全则nextIndex这样也安全

				// 根据Figure2 Leader Rule 4，确定满足commitIndex < N <= 大多数matchIndex[i]且在当前任期的N
				// 先对matchIndex升序排序，为了不影响到matchIndex原来的值，此处对副本排序
				sortMatchIndex := make([]int, len(rf.peers))
				copy(sortMatchIndex, rf.matchIndex)
				sort.Ints(sortMatchIndex)                         // 升序排序
				maxN := sortMatchIndex[(len(sortMatchIndex)-1)/2] // 满足N <= 大多数matchIndex[i] 的最大的可能的N
				for N := maxN; N > rf.commitIndex; N-- {
					if rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm {
						rf.commitIndex = N // 如果log[N]的任期等于当前任期则更新commitIndex
						DPrintf("Leader%d's commitIndex is updated to %d.\n", rf.me, N)
						break
					}
				}
			}

		}(i) // 因为i随着for循环在变，因此将它作为参数传进去
	}
}

// 由leader调用，向其他servers发送日志条目追加请求或心跳包（没有携带日志条目的AppendEntries RPCs）
// 入参server是目的server在rf.peers[]中的索引（id）
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) // 调用对应server的Raft.AppendEntries方法进行请求日志追加处理
	return ok
}

// AppendEntries RPC handler
// 其他servers收到leader的追加日志rpc或心跳包后进行逻辑处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 检查日志是匹配的才接收
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 根据Figure2 AppendEntries RPC的receiver实现规则
	// leader自己的term比RPC接收server的term还要小，则追加日志失败
	// 如果AppendEntries RPC中的任期过时，则不应该重启计时器！
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm // 将自己的current term附在回复中
		return
	}

	// 见Figure2 "Rules for Servers“及5.3节
	// 当RPC请求或回复中的term > 当前server的current term时
	// 1. set currentTerm = T
	// 2. 转换到follower状态
	// 另外，当candidate收到来自另一个声称是leader的server的RPC时，
	// 如果这个leader的term >= 这个candidate的current term，则candidate将承认这个leader是合法的，并返回到follower状态
	wasLeader := (rf.state == Leader) // 标志rf曾经是leader

	if args.Term > rf.currentTerm {
		rf.votedFor = -1           // 当term发生变化时，需要重置votedFor
		rf.currentTerm = args.Term // 更新自己的term为较新的值
		rf.persist()
	}

	// 这里实现了candidate或follower在收到leader的心跳包或日志追加RPC后重置计时器并维持follower状态
	rf.state = Follower         // 变回或维持Follower
	rf.leaderId = args.LeaderId // 将rpc携带的leaderId设为自己的leaderId，记录最近的leader（client寻找leader失败时用到）
	DPrintf("Server %d gets an AppendEntries RPC(term:%d, Entries len:%d) with a higher term from Leader %d, and its current term become %d.\n",
		rf.me, args.Term, len(args.Entries), args.LeaderId, rf.currentTerm)

	// 如果follower的term与leader的term相等（大多数情况），那么follower收到AppendEntries RPC后也需要重置计时器
	rf.timer.Stop()
	rf.timer.Reset(time.Duration(getRandMS(300, 500)) * time.Millisecond)

	if wasLeader { // 如果是leader收到AppendEntries RPC（虽然概率很小）
		go rf.HandleTimeout() // 如果是leader重回follower则要重新循环进行超时检测
	}

	// 如果args.PrevLogIndex < rf.lastIncludedIndex，对于参数中index < rf.lastIncludedIndex部分的log，按照index转换规则会导致index<0，不要处理这部分log
	if args.PreLogIndex < rf.lastIncludedIndex {
		// 如果要追加的日志段过于陈旧（该follower早就应用了更新的快照），则不进行追加
		if len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Index <= rf.lastIncludedIndex {
			// 这种情况下要是reply false则会导致leader继续回溯发送更长的日志，但实际应该发送更后面的日志段，因此reply true但不实际修改自己的log
			reply.Success = true
			reply.Term = rf.currentTerm
			return
		} else {
			args.Entries = args.Entries[rf.lastIncludedIndex-args.PreLogIndex:]
			args.PreLogIndex = rf.lastIncludedIndex
			args.PreLogTerm = rf.lastIncludedTerm
			// 之后执行下面匹配上了的else分支
		}
	}

	// 如果follower中没有leader在preLogIndex处相匹配的日志
	if rf.log[len(rf.log)-1].Index < args.PreLogIndex || rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term != args.PreLogTerm {
		// 日志回溯加速优化修改
		if rf.log[len(rf.log)-1].Index < args.PreLogIndex { // 如果follower的日志中没有prevLogIndex
			reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
			reply.ConflictTerm = -1
		} else { // 如果follower在其日志中确实有prevLogIndex，但是任期不匹配
			reply.ConflictTerm = rf.log[args.PreLogIndex-rf.lastIncludedIndex].Term
			i := args.PreLogIndex - 1 - rf.lastIncludedIndex
			for i >= 0 && rf.log[i].Term == reply.ConflictTerm { // 在其日志中搜索其条目中任期等于conflictTerm的第一个索引
				i--
			}
			reply.ConflictIndex = i + 1 + rf.lastIncludedIndex
		}

		reply.Success = false // 返回false
		reply.Term = rf.currentTerm
		return
	} else { // 匹配到了两个日志一致的最新日志条目
		// 日志一致性检查到leader让follower追加日志操作中，都用AppendEntries RPC，这样leader不用专门去恢复日志一致性
		// 即使是心跳包也无需特别处理，因为追加的日志为空，但注意心跳包也要通过一致性检查才会返回true

		// PreLogIndex与PrevLogTerm匹配到的情况，还要额外检查新同步过来的日志和已存在的日志是否存在冲突:
		// 如果一个已经存在的日志项和新的日志项冲突（相同index但是不同term），那么要删除这个冲突的日志项及其往后的日志，并将新的日志项追加到日志中。
		misMatchIndex := -1
		for i, entry := range args.Entries {
			if args.PreLogIndex+1+i > rf.log[len(rf.log)-1].Index || rf.log[args.PreLogIndex-rf.lastIncludedIndex+1+i].Term != entry.Term { // 找到第一个冲突项
				misMatchIndex = args.PreLogIndex + 1 + i
				break
			}
		}

		if misMatchIndex != -1 { // 已存在的日志与RPC中的Entries有冲突的情况
			newLog := rf.log[:misMatchIndex-rf.lastIncludedIndex]                       // 从头截取到misMatchIndex（但不包括）的是一致的日志
			newLog = append(newLog, args.Entries[misMatchIndex-args.PreLogIndex-1:]...) // 追加日志中没有的任何新条目（也即leader在preLogIndex之后的日志）
			rf.log = newLog
		}

		rf.persist()

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			// leader都还没有将所有日志提交，则follower最多提交到leader提交的位置
			// leader已经至少提交到了发给follower的最后一个日志，则follower就把自己现有的日志提交
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		}
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}

// 循环检查是否有需要apply的日志
func (rf *Raft) applier() {
	for !rf.killed() { // 如果server没有被kill就一直检测
		rf.mu.Lock()

		var applyMsg ApplyMsg
		needApply := false

		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++

			if rf.lastApplied <= rf.lastIncludedIndex { // 说明这条命令已经被做成snapshot了，不需要提交
				rf.lastApplied = rf.lastIncludedIndex // 直接将lastApplied提前到lastIncludedIndex
				rf.mu.Unlock()                        // continue前不要忘了先解锁！！！
				continue
			}

			applyMsg = ApplyMsg{
				CommandValid:  true, // true代表此applyMsg包含一个新的已提交的日志条目
				SnapshotValid: false,
				Command:       rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex:  rf.lastApplied,
				CommandTerm:   rf.log[rf.lastApplied-rf.lastIncludedIndex].Term,
			}
			needApply = true

		}
		rf.mu.Unlock()

		// 本日志需要apply
		if needApply {
			rf.applyCh <- applyMsg // 将ApplyMsg发送到管道
		} else {
			time.Sleep(10 * time.Millisecond) // 不要让循环一直连续执行，可能占用很多时间而测试失败
		}
	}
}
