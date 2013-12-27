var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var P = require('p-promise')

function Log(storage, stateMachine) {
	this.votedFor = 0
	this.entries = []
	this.currentTerm = 0
	this.commitIndex = -1
	this.lastApplied = -1
	this.storage = storage
	this.stateMachine = stateMachine
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
	Raft protocol RPC call

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
		{ currentTerm: this.currentTerm }
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

	leaderCommit: Number (this.entries index)
/*/
Log.prototype.updateCommitIndex = function (leaderCommit) {
	if (leaderCommit > this.commitIndex) {
		this.execute(Math.min(leaderCommit, this.lastIndex()))
	}
}

/*/
	Execute the entries up to index on the stateMachine

	index: Number (this.entries index)
	returns: P(Number) index of last entry executed
/*/
Log.prototype.execute = function (index) {
	if (index <= this.commitIndex) { return P() }
	this.commitIndex = index
	var chain = this.stateMachine.execute(this.entryAt(this.lastApplied + 1))
	for (var i = this.lastApplied + 1; i < index; i++) {
		chain = chain.then(
			function (idx, result) {
				this.executed(idx, result)
				return this.stateMachine.execute(this.entryAt(idx + 1))
			}.bind(this, i)
		)
	}
	return chain.then(this.executed.bind(this, index))
}

/*/
	Called after an entry has been executed on the stateMachine.

	index: Number (this.entries index)
	result: Anything (the result from the stateMachine)
	returns: index
	emits: 'executed'
/*/
Log.prototype.executed = function (index, result) {
	this.lastApplied = index
	this.emit('executed', index, this.entryAt(index), result)
	return index
}

module.exports = Log
