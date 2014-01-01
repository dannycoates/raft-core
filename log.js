var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter

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
	this.storage.load(this.onLoaded)
}
function onLoaded(err, data) {
	this.currentTerm = data.currentTerm || 0
	this.votedFor = data.votedFor || 0
	this.entries = data.entries || []
	this.emit('loaded', data)
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
Log.prototype.appendEntries = function (info, callback) {
	var nope = callback.bind(null, null, false)
	var yep = callback.bind(null, null, true)

	if (info.term < this.currentTerm) {
		// you are out of date, go away
		return process.nextTick(nope)
	}
	var newEntries = info.entries || { startIndex: 0, values: [] }
	var prevEntry = this.entryAt(info.prevLogIndex)
	if (
		 this.lastIndex() < info.prevLogIndex ||
		(prevEntry && prevEntry.term !== info.prevLogTerm)
	) {
		// we are out of date, go back
		return process.nextTick(nope)
	}

	if (newEntries.values.length === 0) {
		// nothing new. probably a heartbeat
		this.updateCommitIndex(info.leaderCommit)
		return process.nextTick(yep)
	}

	// The currentTerm could only have changed if votedFor = 0 & votedFor can't
	// change in a term, so we can use it as a heuristic for when to write state.
	var state = this.votedFor ? {} : { currentTerm: this.currentTerm, votedFor: 0 }

	this.storage.appendEntries(
		newEntries.startIndex,
		newEntries.values,
		state,
		function (err) {
			if (err) { return callback(err, false) }
			this.entries.splice(newEntries.startIndex)
			this.entries = this.entries.concat(newEntries.values)
			this.updateCommitIndex(info.leaderCommit)
			yep()
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
Log.prototype.requestVote = function (info, callback) {
	var nope = callback.bind(null, null, false)

	if (info.term < this.currentTerm) {
		return process.nextTick(nope)
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
				},
				function (err) {
					callback(err, !err)
				}
			)
		}
	}
	process.nextTick(nope)
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
		this.execute(this.commitIndex, function () {})
	}
}

/*/
	Execute the entries up to and including 'index' on the stateMachine

	index: Number (this.entries index)
	returns: P(Number) index of last entry executed
/*/
Log.prototype.execute = function (index, callback) {
	if (index <= this.lastApplied) { return callback() }
	var entries = []
	for (var i = this.lastApplied + 1; i <= index; i++) {
		entries.push(i)
	}
	return this.chain(entries, callback)
}
Log.prototype.chain = function (entries, callback, err) {
	if (err) { return callback(err) }
	if (!entries.length) { return callback() }
	this.executeEntry(entries.shift(), this.chain.bind(this, entries, callback))
}

/*/
	Execute a single entry on the stateMachine

	index: Number (this.entries index)
/*/
Log.prototype.executeEntry = function (index, callback) {
	var entry = this.entryAt(index)
	if (entry.noop) { return callback() }
	this.stateMachine.execute(
		entry.op,
		afterExecute.bind(this, callback, index, entry)
	)
}
function afterExecute(cb, index, entry, err, result) {
	if (err) {
		this.emit('error', new Error('state machine error')) //TODO
		return cb(err)
	}
	this.lastApplied = index
	this.emit('executed', index, entry, result)
	cb()
}

module.exports = Log
