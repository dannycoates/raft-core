var P = require('p-promise')

function MemoryStateMachine() {
	this.state = 1
}

MemoryStateMachine.prototype.execute = function (entry) {
	console.log('executing', entry)
	return P(this.state++)
}

module.exports = MemoryStateMachine
