var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter

function Leader(log, peerIds) {
	this.name = 'leader'
	this.log = log
	this.peerIds = peerIds // array
	this.nextIndex = {}
	this.matchIndex = {}
	this.heartbeatInterval = 100
	this.heartbeatTimer = null
	this.broadcastEntries = broadcastEntries.bind(this)
}
inherits(Leader, EventEmitter)

// A leader denies all RPC requests
function noop() {}
function nope(x, callback) { process.nextTick(callback.bind(null, null, false)) }
Leader.prototype.countVote = noop
Leader.prototype.requestVote = nope
Leader.prototype.appendEntries = nope

/*/
	Sends an appendEntries RPC to all peers

	Upon election: send initial empty AppendEntries RPCs
	(heartbeat) to each server; repeat during idle periods
	to prevent election timeouts (§5.2)
/*/
function broadcastEntries() {
	clearTimeout(this.heartbeatTimer)
	for (var i = 0; i < this.peerIds.length; i++) {
		this.sendAppendEntries(this.peerIds[i])
	}
	this.heartbeatTimer = setTimeout(this.broadcastEntries, this.heartbeatInterval)
}

/*/
	Sends an appendEntries RPC to a peer.

	If last log index ≥ nextIndex for a follower:
	send AppendEntries RPC with log entries starting at nextIndex
/*/
Leader.prototype.sendAppendEntries = function (peerId) {
	var info = {
		term: this.log.currentTerm,
		prevLogIndex: -1,
		prevLogTerm: 0,
		leaderCommit: this.log.commitIndex,
		entries: {}
	}
	info.prevLogIndex = (this.nextIndex[peerId] || this.log.lastIndex()) - 1
	info.prevLogTerm = this.log.termAt(info.prevLogIndex)
	info.entries = this.log.entriesSince(info.prevLogIndex)
	this.emit('appendEntries', peerId, info)
}

/*/
	Sends a 'noop' entry to establish log precedence.
	This entry will persist in the log, but not be executed by the stateMachine
/*/
Leader.prototype.noop = function () {
	this.request(
		{
			term: this.log.currentTerm,
			prevLogIndex: this.log.lastIndex(),
			prevLogTerm: this.log.lastTerm(),
			leaderCommit: this.log.commitIndex,
			entries: {
				startIndex: this.log.lastIndex() + 1,
				values: [{term: this.log.currentTerm, noop: true, op: {}}]
			}
		},
		noop
	)
}

/*/
	Updates the term if we're no longer leader.

	If RPC request or response contains term T > currentTerm:
	set currentTerm = T, convert to follower (§5.1)
/*/
Leader.prototype.assertRole = function (info) {
	if (info.term > this.log.currentTerm) {
		clearTimeout(this.heartbeatTimer)
		this.log.currentTerm = info.term
		this.log.votedFor = 0
		this.emit('changeRole', 'follower')
	}
}

/*/
	Follow the protocol rules for updating the commitIndex.

	If there exists an N such that N > commitIndex,
	a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	set commitIndex = N (§5.3, §5.4).
/*/
Leader.prototype.updateCommitIndex = function () {
	var lastIndex = this.log.lastIndex()
	if (this.log.commitIndex === lastIndex) { return }

	var majority = Math.floor(this.peerIds.length / 2)
	var matchIndices = [lastIndex]
	for (var i = 0; i < this.peerIds.length; i++) {
		matchIndices.push(this.matchIndex[this.peerIds[i]] || -1)
	}
	matchIndices.sort()
	var majorityIndex = matchIndices[majority]
	if (this.log.termAt(majorityIndex) === this.log.currentTerm) {
		this.log.updateCommitIndex(majorityIndex)
	}
}

/*/
	Handles the response from an appendEntries RPC

	peerId: Number,
	request: Object - the original request,
	response: {
		term: Number,
		success: Boolean
	}
/*/
Leader.prototype.entriesAppended = function (peerId, request, response) {
	if (response.success) {
		// If successful: update nextIndex and matchIndex for follower (§5.3)
		var matchIndex = (request.entries.startIndex + request.entries.values.length) - 1
		this.matchIndex[peerId] = matchIndex
		this.nextIndex[peerId] = matchIndex + 1
		if (matchIndex > this.log.commitIndex) {
			this.updateCommitIndex()
		}
	}
	else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		this.nextIndex[peerId] = request.prevLogIndex
		this.sendAppendEntries(peerId)
	}
}

/*/
	Adds a client entry to the log.

	If command received from client: append entry to local log,
	respond after entry applied to state machine (§5.3)
/*/
Leader.prototype.request = function (info, callback) {
	return this.log.appendEntries(info, afterRequest.bind(this, callback))
}
function afterRequest(cb, err, success) {
	if (err || !success) { return cb(err, success) }
	this.broadcastEntries()
	cb(null, true)
}

module.exports = Leader
