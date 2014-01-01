var test = require('tap').test
var Log = require('../../log')
var MemoryStorage = require('../memory-storage')
var MemoryStateMachine = require('../memory-state-machine')

var log = new Log(new MemoryStorage(), new MemoryStateMachine())

test(
	'load: loads state and entries from storage',
	function (t) {
		var storage = new MemoryStorage()
		storage.data = {
			currentTerm: 5,
			votedFor: 3
		}
		storage.entries = [{ term: 4}, { term: 5}]
		var l = new Log(storage)
		l.once(
			'loaded',
			function () {
				t.equal(l.currentTerm, 5, 'loaded currentTerm')
				t.equal(l.votedFor, 3, 'loaded votedFor')
				t.equal(l.entries.length, 2, 'loaded entries')
				t.end()
			}
		)
		l.load()
	}
)

test(
	'appendEntries: Reply false if term < currentTerm (§5.1)',
	function (t) {
		log.currentTerm = 2
		log.appendEntries(
			{ term: 1 },
			function (err, success) {
				log.currentTerm = 0
				t.equal(success, false, 'denied')
				t.end()
			}
		)
	}
)

test(
	'appendEntries: Reply false if log doesn’t contain an entry at prevLogIndex ' +
	'whose term matches prevLogTerm (§5.3)',
	function (t) {
		log.entries = [{ term: 1 }, { term: 2 }]
		log.appendEntries(
			{
				term: 3,
				prevLogIndex: 1,
				prevLogTerm: 3
			},
			function (err, success) {
				t.equals(success, false, 'denied')
				t.end()
			}
		)
	}
)

test(
	'appendEntries: If an existing entry conflicts with a new one (same index ' +
	'but different terms), delete the existing entry and all that follow it (§5.3)',
	function (t) {
		log.entries = [{ term: 1 }, { term: 2 }, { term: 3 }, { term: 4 }, { term: 5 }]
		log.appendEntries(
			{
				term: 2,
				prevLogIndex: 1,
				prevLogTerm: 2,
				entries: {
					startIndex: 2,
					values: [{ term: 2 }]
				}
			},
			function (err, success) {
				t.equal(success, true, 'succeeded')
				t.equal(log.entries.length, 3, 'correct length')
				t.equal(log.entries[2].term, 2, 'correct term')
				t.end()
			}
		)
	}
)

test(
	'appendEntries: Append any new entries not already in the log',
	function (t) {
		log.entries = [{ term: 1 }, { term: 2 }]
		log.appendEntries(
			{
				term: 2,
				prevLogIndex: 1,
				prevLogTerm: 2,
				entries: {
					startIndex: 2,
					values: [{ term: 2 }, { term: 2 }, { term: 2 }]
				}
			},
			function (err, success) {
				t.equal(success, true, 'succeeded')
				t.equal(log.entries.length, 5, 'correct length')
				t.equal(log.entries[4].term, 2, 'correct term')
				t.end()
			}
		)
	}
)

test(
	'appendEntries: If leaderCommit > commitIndex, '+
	'set commitIndex = min(leaderCommit, lastIndex)',
	function (t) {
		log.commitIndex = -1
		log.entries = [{ term: 1 }, { term: 2 }]
		log.appendEntries(
			{ term: 2, prevLogIndex: 1, prevLogTerm: 2, leaderCommit: 1 },
			function (err, success) {
				t.equal(success, true)
				t.equal(log.commitIndex, 1, 'updated commitIndex')
				t.end()
			}
		)
	}
)

test(
	'appendEntries: If log is empty the first entry gets added',
	function (t) {
		log.entries = []
		log.appendEntries(
			{
				term: 1,
				prevLogIndex: -1,
				prevLogTerm: 0,
				entries: {
					startIndex: 0,
					values: [{ term: 2 }, { term: 2 }, { term: 2 }]
				}
			},
			function (err, success) {
				t.equal(success, true)
				t.equal(log.entries.length, 3, 'correct length')
				t.end()
			}
		)
	}
)

test(
	'requestVote: Reply false if term < currentTerm (§5.1)',
	function (t) {
		log.currentTerm = 2
		log.requestVote(
			{ term: 1, candidateId: 1, lastLogIndex: 1, lastLogTerm: 1},
			function (err, voteGranted) {
				t.equal(voteGranted, false)
				t.end()
			}
		)
	}
)

test(
	'requestVote: Reply false if lastLogTerm = currentTerm and lastLogIndex < mine',
	function (t) {
		log.currentTerm = 1
		log.entries = [{ term: 1 }, { term: 1 }]
		log.requestVote(
			{ term: 1, candidateId: 1, lastLogIndex: 0, lastLogTerm: 1},
			function (err, voteGranted) {
				t.equal(voteGranted, false)
				t.end()
			}
		)
	}
)

test(
	'requestVote: Reply false if votedFor != candidateId',
	function (t) {
		log.currentTerm = 0
		log.votedFor = 2
		log.requestVote(
			{ term: 1, candidateId: 1, lastLogIndex: 0, lastLogTerm: 1},
			function (err, voteGranted) {
				t.equal(voteGranted, false)
				t.end()
			}
		)
	}
)

test(
	'requestVote: Reply true if votedFor is null and term > currentTerm',
	function (t) {
		log.currentTerm = 0
		log.votedFor = 0
		log.requestVote(
			{ term: 1, candidateId: 1, lastLogIndex: 1, lastLogTerm: 1},
			function (err, voteGranted) {
				t.equal(voteGranted, true)
				t.end()
			}
		)
	}
)

test(
	'execute: executes the correct entries',
	function (t) {
		var log = new Log(new MemoryStorage(), new MemoryStateMachine())
		log.lastApplied = 0
		log.entries = [{ term: 1 }, { term: 5 }, { term: 5 }, { term: 9 }]
		// only term 5 entries should get executed here
		log.execute(
			2,
			function () {
				t.equal(log.lastApplied, 2)
				// MemoryStateMachine increments it's state for each execution.
				// It starts at 1 plus the 2 exepected calls should make it 3.
				t.equal(log.stateMachine.state, 3)
				t.end()
			}
		)
	}
)

test(
	'updateCommitIndex: commitIndex is never set higher than lastIndex',
	function (t) {
		log.commitIndex = 1
		log.entries = [{ term: 1 }, { term: 5 }, { term: 5 }, { term: 9 }]
		log.updateCommitIndex(80)
		t.equal(log.commitIndex, 3)
		t.end()
	}
)

test(
	'updateCommitIndex: commitIndex is never set lower',
	function (t) {
		log.commitIndex = 1
		log.entries = [{ term: 1 }, { term: 5 }, { term: 5 }, { term: 9 }]
		log.updateCommitIndex(0)
		t.equal(log.commitIndex, 1)
		t.end()
	}
)
