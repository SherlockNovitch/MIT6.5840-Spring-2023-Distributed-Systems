package raft

//
// this is an outline of the API that raft must expose to the service (or tester).
// see comments below for each of these functions for more details.
//
// rf = Make(...)  create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)  start agreement on a new log entry
// rf.GetState() (term, isLeader) ask a Raft for its current term, and whether it thinks it is leader
// each time a new entry is committed to the log, each Raft peer should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"6.5840/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ApplyMsg :
// as each Raft peer becomes aware that successive log entries are committed,
// the peer should send an ApplyMsg to the service (or tester) on the same server, via the applyCh passed to Make().
// set CommandValid to true to indicate that the ApplyMsg contains a newly committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g., snapshots) on the applyCh,
// but set CommandValid to false for these other uses.
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

type LogEntry struct {
}

// Raft : A Go object implementing a single Raft peer.
type Raft struct {
	mu                   sync.Mutex          // Lock to protect shared access to this peer's state
	peers                []*labrpc.ClientEnd // RPC end points of all peers
	persister            *Persister          // Object to hold this peer's persisted state
	me                   int                 // this peer's index into peers[]
	dead                 int32               // set by Kill()
	heardLeaderHeartBeat bool                // have heard the leader's heartbeat in this term?
	currentTerm          int                 // latest term server has seen
	votedFor             int                 // candidateId that received vote in current term，-1 if none
	log                  []LogEntry          // log entries
	commitIndex          int                 // index of the highest log entry known to be committed
	lastApplied          int                 // index of the highest log entry applied to state  machine
	isLeader             bool                // if me is Leader
	voteCount            int                 // candidate's vote count
	nextIndex            []int               // for each server, index of the next log entry  to send to that server
	matchIndex           []int               // for each server, index of the highest log entry known to be replicated on server
	heartBeatInternal    int                 // ms
}

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.isLeader
}

// save Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
}

// Snapshot :
// the service says it has created a snapshot that has all info up to and including index.
// this means the service no longer needs the log through (and including) that index.
// Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) convertToFollower(Term int) {
	rf.currentTerm = Term
	rf.isLeader = false
	rf.votedFor = -1
	rf.voteCount = 0
}

func (rf *Raft) convertToLeader() {
	rf.isLeader = true
	go rf.sendHeartBeats()
}

func (rf *Raft) compareAndSetTerm(otherPeerTerm int) {
	if otherPeerTerm > rf.currentTerm {
		rf.convertToFollower(otherPeerTerm)
	}
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
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) startLeaderElection() {
	rf.currentTerm += 1 //Increment currentTerm
	rf.votedFor = rf.me //Vote for self
	rf.voteCount = 1
	for peerId := range rf.peers { //Send RequestVote RPCs to all other servers
		args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
		if peerId != rf.me {
			go rf.sendRequestVote(peerId, &args, &RequestVoteReply{})
		}
	}
	for rf.voteCount <= len(rf.peers)/2 {
		if rf.votedFor != rf.me {
			return
		}
		time.Sleep(time.Duration(rf.heartBeatInternal/5) * time.Millisecond)
	}
	if rf.votedFor == rf.me { //If votes received from the majority of servers: become leader
		rf.convertToLeader()
	}
}

func (rf *Raft) sendHeartBeats() {
	for rf.isLeader {
		for peerId := range rf.peers {
			if peerId != rf.me { //May TODO
				rf.sendAppendEntries(peerId, &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}, &AppendEntriesReply{})
			}
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.heardLeaderHeartBeat = false
		electionBaseTimeOut := 3 * rf.heartBeatInternal
		time.Sleep(time.Duration(electionBaseTimeOut+(rand.Int()%electionBaseTimeOut)) * time.Millisecond) // pause for a random amount of time between 3*heartBeatInternal and 6*heartBeatInternal.
		if !rf.isLeader && !rf.heardLeaderHeartBeat {                                                      // Check if a leader election should be started.
			go rf.startLeaderElection()
		}
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		rf.compareAndSetTerm(args.Term)
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //TODO
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
}

// send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// Call() is guaranteed to return (perhaps after a delay) *except* if the  handler function on the server side does not return.
// Thus, there is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); ok {
		rf.compareAndSetTerm(reply.Term)
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.voteCount += 1
			rf.mu.Unlock()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { //TODO
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.compareAndSetTerm(args.Term)
	rf.heardLeaderHeartBeat = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.compareAndSetTerm(reply.Term)
	}
}

// Make Function: the service or tester wants to create a Raft server.
// the ports of all the Raft servers (including this one) are in peers[].
// this server's port is peers[me]. all the servers' peers[] arrays  have the same order.
// persister is a place for this server to save its persistent state, and also initially holds the most recent saved state, if any.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.heartBeatInternal = 50
	rf.convertToFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
