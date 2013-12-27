var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var P = require('p-promise')

function Follower(log) {
	this.name = 'follower'
	this.log = log
	this.leaderId = 0
	this.beginElection = beginElection.bind(this)
	this.electionTimeout = 200
	this.electionTimer = null
	this.checkVoteResult = checkVoteResult.bind(this)
	this.resetElectionTimer()
}
inherits(Follower, EventEmitter)

function beginElection() {
	this.emit('changeRole', 'candidate')
}

Follower.prototype.resetElectionTimer = function () {
	clearTimeout(this.electionTimer)
	this.electionTimer = setTimeout(
		this.beginElection,
		this.electionTimeout + Math.random() * this.electionTimeout
	)
}

Follower.prototype.updateTerm = function (info) {
	if (info.term > this.log.currentTerm) {
		this.log.currentTerm = info.term
	}
}

Follower.prototype.countVote = function () {} // noop

Follower.prototype.entriesAppended = function () {} // noop

function checkVoteResult(voteGranted) {
	if (voteGranted) {
		this.resetElectionTimer()
	}
	return voteGranted
}

Follower.prototype.requestVote = function (vote) {
	return this.log.requestVote(vote)
		.then(this.checkVoteResult)
}

Follower.prototype.appendEntries = function (info) {
	if (info.leaderId) { this.leaderId = info.leaderId }
	this.resetElectionTimer()
	return this.log.appendEntries(info)
}

Follower.prototype.request = function (entry) {
	return P.reject({ message: 'not the leader', leaderId: this.leaderId })
}

module.exports = Follower
