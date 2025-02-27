// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	raftLog := newLog(c.Storage)

	r := &Raft{
		id: c.ID,
		// State: StateFollower,

		RaftLog: raftLog,
		Prs: make(map[uint64]*Progress),

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
	}

	for _, id := range c.peers {
		r.Prs[id] = &Progress{}
	}

	r.becomeFollower(r.Term, None)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// switch to Candidate if needed.
		r.tickElection()
	case StateCandidate:
		// election 
		r.tickElection()
	case StateLeader:
		// send heartBeat
		r.tickHeatbeat()
	}

}

func(r *Raft) tickElection(){
	r.electionElapsed ++ 
	if r.electionElapsed > r.electionTimeout {
		err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		if err != nil {
			log.Warnf("tickElection, error occurred during election: %v ", err)
		}
	}
}

func(r *Raft) tickHeatbeat(){
	
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(r.Term)

	r.Lead = lead
	r.Term = term 
	r.State = StateFollower

	// TODO: remove invalid entries from r.raftLog.entries.
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)

	r.State = StateCandidate
	r.Vote = r.id	// vote for myself
	r.votes = make(map[uint64]bool)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.reset(r.Term)
	r.State = StateLeader

	// TODO: propose a noop entry.
	// send msg to followers.
}

func (r *Raft) reset(term uint64){
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = 0

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	switch{
	case m.Term == 0:
		// local message ?
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgRequestVote {
			inLease := r.Lead != None && r.electionElapsed < r.electionTimeout
			if inLease {
				lastIndex := r.RaftLog.LastIndex()
				lastTerm  := r.RaftLog.LastTerm()
				log.Infof("%x [logterm: %d, index: %d] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
				r.id, lastTerm, lastIndex, m.MsgType, m.From, m.LogTerm, m.Index, m.Term, r.electionTimeout - r.electionElapsed)
				return nil
			}

			// in other case, deal with RequestVote.
		}

		switch {
			default:
				log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
				if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
					r.becomeFollower(m.Term, m.From)
				}else{
					r.becomeFollower(m.Term, None)
				}
		}

	case m.Term < r.Term:
		log.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)

		// ignoer all message types.
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgRequestVote:
		canVote :=  r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote {
			r.send(pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			r.electionElapsed = 0
			r.Vote = m.From
		}else{
			r.send(pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	default:
		switch r.State{
		case StateLeader:
			r.stepLeader(m)
		case StateCandidate:
			r.stepCandidate(m)
		case StateFollower:
			r.stepFollower(m)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	log.Infof("stepCandidate, %x, %s", r.id, m.MsgType)
	switch m.MsgType{
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgRequestVoteResponse:
		res := r.poll(m.From, !m.Reject)
		if res {
			r.becomeLeader()
		}
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgPropose:
		log.Infof("%d not leader at term %d; dropping proposal", r.id, r.Term)
	}
	return nil
}

func (r *Raft) hup(){
	if r.State == StateLeader {
		log.Warnf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	r.campaign()
}

// 发起新的一轮选举;
func (r *Raft)campaign(){
	r.becomeCandidate()

	lastIndexId := r.RaftLog.LastIndex()
	lastLogTerm := r.RaftLog.LastTerm()
	for id, _ := range r.Prs {
		r.send(pb.Message{From:r.id, To: id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVote, Index: lastIndexId, LogTerm: lastLogTerm})
	}
}

func (r *Raft) poll(from uint64, vote bool)bool{
	if !vote {
		return false
	}
	r.votes[from] = true
	if len(r.votes) > 1 + len(r.Prs)/2 {
		return true
	}

	return false
}

// messages need to send
func (r *Raft) send(m pb.Message) {
	m.From = r.id

	log.Infof("send msg, from %x to %x, %s", m.From, m.To, m.MsgType)
	r.msgs = append(r.msgs, m)
}

func (r *Raft) readMessages() []pb.Message {
	msgs := r.msgs
	r.msgs = make([]pb.Message, 0)
	return msgs
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
