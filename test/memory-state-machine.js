function MemoryStateMachine() {
	this.state = 1
}

MemoryStateMachine.prototype.execute = function (entry, callback) {
	console.log('executing', entry)
	this.state++
	process.nextTick(callback.bind(null, null, this.state))
}

module.exports = MemoryStateMachine
