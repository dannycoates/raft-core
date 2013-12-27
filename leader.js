var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var P = require('p-promise')

function Leader(log, peerIds) {
	this.name = 'leader'
	this.log = log
	this.peerIds = peerIds // array
	this.nextIndex = {}
	this.matchIndex = {}
	this.heartbeatInterval = 100
	this.heartbeatTimer = null
	this.broadcastEntries = broadcastEntries.bind(this)
	this.noop()
}
inherits(Leader, EventEmitter)

function broadcastEntries() {
	clearTimeout(this.heartbeatTimer)
	for (var i = 0; i < this.peerIds.length; i++) {
		this.sendAppendEntries(this.peerIds[i])
	}
	this.heartbeatTimer = setTimeout(this.broadcastEntries, 100)
}

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
	sends a 'noop' entry to establish log precedence
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
		}
	)
}

Leader.prototype.updateTerm = function (info) {
	if (info.term > this.log.currentTerm) {
		clearTimeout(this.heartbeatTimer)
		this.log.currentTerm = info.term
		this.emit('changeRole', 'follower')
	}
}

Leader.prototype.updateCommitIndex = function () {
	var majority = Math.floor(this.peerIds.length / 2)
	var matchIndices = [this.log.lastIndex()]
	for (var i = 0; i < this.peerIds.length; i++) {
		matchIndices.push(this.matchIndex[this.peerIds[i]] || -1)
	}
	matchIndices.sort()
	// ensure a majority has an entry from my term
	var majorityIndex = matchIndices[majority]
	if (this.log.termAt(majorityIndex) === this.log.currentTerm) {
		this.log.execute(majorityIndex)
	}
}

Leader.prototype.countVote = function () {} //noop

Leader.prototype.entriesAppended = function (peerId, request, response) {
	if (response.success) {
		var matchIndex = (request.entries.startIndex + request.entries.values.length) - 1
		this.matchIndex[peerId] = matchIndex
		this.nextIndex[peerId] = matchIndex + 1
		if (matchIndex > this.log.commitIndex) {
			this.updateCommitIndex()
		}
	}
	else {
		this.nextIndex[peerId] = request.prevLogIndex
		this.sendAppendEntries(peerId)
	}
}

Leader.prototype.requestVote = function (vote) {
	return P(false)
}

Leader.prototype.appendEntries = function (info) {
	return P(false)
}

Leader.prototype.request = function (info) {
	return this.log.appendEntries(info)
		.then(this.broadcastEntries)
}

module.exports = Leader
