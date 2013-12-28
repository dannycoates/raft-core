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
		l.load()
			.then(
				function () {
					t.equal(l.currentTerm, 5, 'loaded currentTerm')
					t.equal(l.votedFor, 3, 'loaded votedFor')
					t.equal(l.entries.length, 2, 'loaded entries')
					t.end()
				}
			)
	}
)

test(
	'appendEntries: Reply false if term < currentTerm (§5.1)',
	function (t) {
		log.currentTerm = 2
		log.appendEntries({ term: 1 })
		.then(
			function (success) {
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
			}
		)
		.then(
			function (success) {
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
			}
		)
		.then(
			function (success) {
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
			}
		)
		.then(
			function (success) {
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
			{ term: 2, prevLogIndex: 1, prevLogTerm: 2, leaderCommit: 1 }
		)
		.then(
			function (success) {
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
			}
		)
		.then(
			function (success) {
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
			{ term: 1, candidateId: 1, lastLogIndex: 1, lastLogTerm: 1}
		)
		.then(
			function (voteGranted) {
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
			{ term: 1, candidateId: 1, lastLogIndex: 0, lastLogTerm: 1}
		)
		.then(
			function (voteGranted) {
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
			{ term: 1, candidateId: 1, lastLogIndex: 0, lastLogTerm: 1}
		)
		.then(
			function (voteGranted) {
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
			{ term: 1, candidateId: 1, lastLogIndex: 1, lastLogTerm: 1}
		)
		.then(
			function (voteGranted) {
				t.equal(voteGranted, true)
				t.end()
			}
		)
	}
)
