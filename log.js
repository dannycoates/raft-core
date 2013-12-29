var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var P = require('p-promise')

function Log(storage, stateMachine) {
	//<persistent>
	this.entries = []
	this.votedFor = 0
	this.currentTerm = 0
	this.storage = storage
	//</persistent><volatile>
	this.commitIndex = -1
	this.lastApplied = -1
	this.stateMachine = stateMachine
	//</volatile>
	// How can lastApplied be volatile and still work?
	// Ok, so if stateMachine is volatile then lastApplied must be volatile.
	// Snapshotting will require lastApplied and stateMachine to be persistent
	this.onLoaded = onLoaded.bind(this)
}
inherits(Log, EventEmitter)

Log.prototype.lastTerm = function () {
	return this.termAt(this.lastIndex())
}

Log.prototype.lastIndex = function () {
	return this.entries.length - 1
}

Log.prototype.termAt = function (index) {
	return (this.entries[index] || { term: 0 }).term
}

Log.prototype.entryAt = function (index) {
	return this.entries[index]
}

/*/
	Loads the log state and entries from storage
/*/
Log.prototype.load = function () {
	return this.storage.load()
		.then(this.onLoaded)
}
function onLoaded(data) {
	this.currentTerm = data.currentTerm || 0
	this.votedFor = data.votedFor || 0
	this.entries = data.entries || []
	return this
}

/*/
	Raft protocol RPC call

	Rules:
	1. Reply false if term < currentTerm (§5.1)
	2. Reply false if log doesn’t contain an entry at prevLogIndex
	   whose term matches prevLogTerm (§5.3)
	3. If an existing entry conflicts with a new one
	   (same index but different terms), delete the existing entry
	   and all that follow it (§5.3)
	4. Append any new entries not already in the log
	5. If leaderCommit > commitIndex,
	   set commitIndex = min(leaderCommit, last log index)

	info: {
		term: Number,
		leaderId: Number,
		prevLogIndex: Number,
		prevLogTerm: Number,
		leaderCommit: Number,
		entries: {
			startIndex: 5,
			values: [{ term: Number }, ...]
		}
	}
	returns: P(Boolean)
/*/
Log.prototype.appendEntries = function (info) {
	if (info.term < this.currentTerm) {
		return P(false)
	}
	var newEntries = info.entries || { startIndex: 0, values: [] }
	var prevEntry = this.entryAt(info.prevLogIndex)
	if (
		 this.lastIndex() < info.prevLogIndex ||
		(prevEntry && prevEntry.term !== info.prevLogTerm)
	) {
		// we are out of date, go back
		return P(false)
	}

	if (newEntries.values.length === 0) {
		// nothing new. probably a heartbeat
		this.updateCommitIndex(info.leaderCommit)
		return P(true)
	}

	return this.storage.appendEntries(
		newEntries.startIndex,
		newEntries.values,
		{
			currentTerm: this.currentTerm,
			votedFor: this.votedFor
		}
	)
	.then(
		function () {
			this.entries.splice(newEntries.startIndex)
			this.entries = this.entries.concat(newEntries.values)
			this.updateCommitIndex(info.leaderCommit)
			return true
		}.bind(this)
	)
}

/*/
	Raft protocol RPC call

	Rules:
	1. Reply false if term < currentTerm (§5.1)
	2. If votedFor is null or candidateId, and candidate's log is at
	   least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	info: {
		term: Number,
		candidateId: Number,
		lastLogIndex: Number,
		lastLogTerm: Number
	}
	returns: P(Boolean)
/*/
Log.prototype.requestVote = function (info) {
	if (info.term < this.currentTerm) {
		return P(false)
	}
	if (!this.votedFor || this.votedFor === info.candidateId) {
		var myLastTerm = this.lastTerm()
		if (
			info.lastLogTerm > myLastTerm ||
			(
				info.lastLogTerm === myLastTerm &&
				info.lastLogIndex >= this.lastIndex()
			)
		) {
			this.votedFor = info.candidateId
			return this.storage.set(
				{
					votedFor: this.votedFor,
					currentTerm: this.currentTerm
				}
			)
			.then(function () { return true })
		}
	}
	return P(false)
}

/*/
	Get the entries after the given index.

	index: Number
	returns: {
		startIndex: Number,
		values: [{ term: Number, op: Object }] // Array of entries
	}
/*/
Log.prototype.entriesSince = function (index) {
	return {
		startIndex: index + 1,
		values: this.entries.slice(index + 1)
	}
}

/*/
	Move the commitIndex up to match the leader and execute those entries

	newIndex: Number (this.entries index)
/*/
Log.prototype.updateCommitIndex = function (newIndex) {
	if (newIndex > this.commitIndex) {
		this.commitIndex = Math.min(newIndex, this.lastIndex())
		this.execute(this.commitIndex)
	}
}

/*/
	Execute the entries up to and including 'index' on the stateMachine

	index: Number (this.entries index)
	returns: P(Number) index of last entry executed
/*/
Log.prototype.execute = function (index) {
	if (index <= this.lastApplied) { return P() }
	var chain = P()
	for (var i = this.lastApplied + 1; i <= index; i++) {
		chain = chain.then(this.executeEntry.bind(this, i))
	}
	return chain
}

/*/
	Execute a single entry on the stateMachine

	index: Number (this.entries index)
/*/
Log.prototype.executeEntry = function (index) {
	var entry = this.entryAt(index)
	if (entry.noop) { return P(index) }
	return this.stateMachine.execute(entry.op)
		.then(
			function (result) {
				this.lastApplied = index
				this.emit('executed', index, entry, result)
				return index
			}.bind(this)
		)
}

module.exports = Log
