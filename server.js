var Leader = require('./leader')
var Candidate = require('./candidate')
var Follower = require('./follower')

var Log = require('./log')

function Server(id, storage, stateMachine) {
	this.id = id
	this.role = null
	this.log = new Log(storage, stateMachine)
	this.requests = {}
	this.peers = []
	this.peerMap = {}

	this.countVote = countVote.bind(this)
	this.onAppendEntries = onAppendEntries.bind(this)
	this.onChangeRole = onChangeRole.bind(this)
	this.onExecuted = onExecuted.bind(this)

	this.log.on('executed', this.onExecuted)
}

/*/
	Changes the role of this server.

	name: string - role to switch to
/*/
function onChangeRole(name) {
	if (this.role) {
		this.role.removeListener('changeRole', this.onChangeRole)
		this.role.removeListener('appendEntries', this.onAppendEntries)
		console.log('changeRole', this.id, this.role.name, name)
	}
	switch (name) {
		case 'follower':
			this.role = new Follower(this.log)
			this.role.resetElectionTimeout()
			break;
		case 'candidate':
			this.role = new Candidate(this.log)
			this.role.resetElectionTimeout()
			this.beginElection()
			break;
		case 'leader':
			this.role = new Leader(
				this.log,
				this.peers.map(function (p) { return p.id })
			)
			this.role.noop()
			break;
	}
	this.role.on('changeRole', this.onChangeRole)
	this.role.on('appendEntries', this.onAppendEntries)
}

/*/
	After an entry has executed, respond to the client if applicable.

	index: Number - log index of entry
	entry: Object - the requested entry
	result: Anything - result of executing the entry on the stateMachine
/*/
function onExecuted(index, entry, result) {
	console.log('executed', this.id, index, result)
	var request = this.requests[index]
	if (!request || !request.callback) { return }
	request.callback(null, result)
	delete this.requests[index]
}

/*/
	Sends an appendEntries RPC to the given peer

	peerId: Number - peer to send to
	info: Object - an appendEntries request
/*/
function onAppendEntries(peerId, info) {
	info.leaderId = this.id
	var peer = this.peerMap[peerId]
	if (!peer) { return }
	// TODO: peer.appendEntries must be able to timeout
	peer.appendEntries(info, entriesAppended.bind(this, peerId, info))
}

/*/
	The response handler for appendEntries. attached in onAppendEntries.

	peerId: Number,
	request: Object - the full appendEntries 'info' object. see appendEntries,
	response: {
		term: Number,
		success: Boolean
	}
/*/
function entriesAppended(peerId, request, err, response) {
	// TODO err
	this.role.assertRole(response)
	this.role.entriesAppended(peerId, request, response)
}

/*/
	Start up as a follower.

	peers: [
		{
			id: Number,
			appendEntries: function (info) { return P() },
			requestVote: function (info) { return P() }
		}
	]
/*/
Server.prototype.start = function (peers, role) {
	role = role || 'follower'
	this.peers = peers
	for (var i = 0; i < peers.length; i++) {
		var peer = peers[i]
		this.peerMap[peer.id] = peer
	}
	this.log.once('loaded', onChangeRole.bind(this, role))
	// TODO onError
	this.log.load()
}

/*/
	Broadcast requestVote RPCs to all of our peers
/*/
Server.prototype.beginElection = function () {
	this.log.currentTerm++
	var info = {
		term: this.log.currentTerm,
		candidateId: this.id,
		lastLogIndex: this.log.lastIndex(),
		lastLogTerm: this.log.lastTerm()
	}
	this.log.requestVote( // vote for self first
		info,
		function (err, voteGranted) {
			if (err || !voteGranted) {
				return console.error('wat', error)
			}
			for (var i = 0; i < this.peers.length; i++) {
				this.peers[i].requestVote(info, this.countVote)
			}
		}.bind(this)
	)
}

/*/
	The response handler for requestVote
	vote: {
		id: Number,
		term: Number,
		success: Boolean (got the vote)
	}
/*/
function countVote(err, vote) {
	if (vote.term < this.log.currentTerm) {
		return
	}
	this.role.assertRole(vote)
	this.role.countVote(vote, this.peers.length)
}

/*/
	Raft protocol RPC

	info: {
		term: Number,
		candidateId: Number,
		lastLogIndex: Number,
		lastLogTerm: Number
	}
/*/
Server.prototype.requestVote = function (info, callback) {
	this.role.assertRole(info)
	this.role.requestVote(info, afterRequestVote.bind(this, callback))
}
function afterRequestVote(cb, err, voteGranted) {
	cb(err, { id: this.id, term: this.log.currentTerm, voteGranted: voteGranted })
}

/*/
	Raft protocol RPC

	info: {
		term: Number,
		leaderId: Number,
		prevLogIndex: Number,
		prevLogTerm: Number,
		leaderCommit: Number,
		entries: {
			startIndex: Number,
			values: [{ term: Number, op: {} }, ...]
		}
	}
/*/
Server.prototype.appendEntries = function (info, callback) {
	this.role.assertRole(info, 'appendEntries')
	return this.role.appendEntries(info, afterAppendEntries.bind(this, callback))
}
function afterAppendEntries(cb, err, success) {
	cb(err, { term: this.log.currentTerm, success: success })
}

/*/
	Request an entry be applied to the state machine. This is the "public" API.

	entry: {
		requestId: string
		// anything else
	}
	callback: function (err, result) {}
/*/
Server.prototype.request = function (entry, callback) {
	// TODO: look to see if the entry.requestId is "in the system"
	var info = {
		term: this.log.currentTerm,
		leaderId: this.id,
		prevLogIndex: this.log.lastIndex(),
		prevLogTerm: this.log.lastTerm(),
		leaderCommit: this.log.commitIndex,
		entries: {
			startIndex: this.log.lastIndex() + 1,
			values: [{ term: this.log.currentTerm, op: entry }]
		}
	}
	// If I'm not the leader this will return an error
	this.role.request(
		info,
		afterRequest.bind(this, callback, info)
	)
}
function afterRequest(cb, info, err, success) {
	if (err || !success) { return cb(err || new Error('not stored')) } // TODO
	// TODO possibly a bloom filter on the entry.requestId
	// TODO wrap callback in a "timeout function"
	var index = info.entries.startIndex
	this.requests[index] = { info: info, callback: cb }
}

module.exports = Server
