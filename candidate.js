var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter
var P = require('p-promise')

function Candidate(log) {
	this.name = 'candidate'
	this.log = log
	this.votes = {}
	this.votes['self'] = true
	this.beginElection = beginElection.bind(this)
	this.electionTimeout = 200
	this.electionTimer = null
}
inherits(Candidate, EventEmitter)

function beginElection() {
	this.emit('changeRole', 'candidate')
}

Candidate.prototype.clearElectionTimeout = function () {
	clearTimeout(this.electionTimer)
}

Candidate.prototype.resetElectionTimeout = function () {
	this.clearElectionTimeout()
	this.electionTimer = setTimeout(
		this.beginElection,
		this.electionTimeout + Math.random() * this.electionTimeout
	)
}

Candidate.prototype.assertRole = function (info, rpc) { //TODO rpc is ugly
	var currentTerm = this.log.currentTerm
	if (
		info.term > currentTerm ||
		(rpc === 'appendEntries' && info.term === currentTerm)
	) {
		this.log.currentTerm = info.term
		this.log.votedFor = 0
		this.clearElectionTimeout()
		this.emit('changeRole', 'follower')
	}
}

Candidate.prototype.countVote = function (vote, totalPeers) {
	if (vote.voteGranted) {
		this.votes[vote.id] = true

		if (Object.keys(this.votes).length > (totalPeers + 1) / 2) {
			this.clearElectionTimeout()
			this.emit('changeRole', 'leader')
		}
	}
}

Candidate.prototype.entriesAppended = function () {} // noop

Candidate.prototype.requestVote = function (vote) {
	return P(false)
}

Candidate.prototype.appendEntries = function (info) {
	return P(false)
}

Candidate.prototype.request = function (entry) {
	return P.reject({ message: 'no leader' })
}

module.exports = Candidate
