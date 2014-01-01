var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter

function Follower(log) {
	this.name = 'follower'
	this.log = log
	this.leaderId = 0
	this.beginElection = beginElection.bind(this)
	this.electionTimeout = 200
	this.electionTimer = null
}
inherits(Follower, EventEmitter)

// Followers don't send RPC calls so results don't matter
Follower.prototype.countVote = function () {}
Follower.prototype.entriesAppended = function () {}

function beginElection() {
	this.emit('changeRole', 'candidate')
}

Follower.prototype.resetElectionTimeout = function () {
	clearTimeout(this.electionTimer)
	this.electionTimer = setTimeout(
		this.beginElection,
		this.electionTimeout + Math.random() * this.electionTimeout
	)
}

Follower.prototype.assertRole = function (info) {
	if (info.term > this.log.currentTerm) {
		this.log.currentTerm = info.term
		this.log.votedFor = 0
	}
}

Follower.prototype.requestVote = function (vote, callback) {
	return this.log.requestVote(vote, afterRequestVote.bind(this, callback))
}
function afterRequestVote(cb, err, voteGranted) {
	if (voteGranted) {
		this.resetElectionTimeout()
	}
	cb(err, voteGranted)
}

Follower.prototype.appendEntries = function (info, callback) {
	this.leaderId = info.leaderId
	this.resetElectionTimeout()
	this.log.appendEntries(info, callback)
}

Follower.prototype.request = function (entry, callback) {
	process.nextTick(
		callback.bind(null, { message: 'not the leader', leaderId: this.leaderId })
	)
}

module.exports = Follower
