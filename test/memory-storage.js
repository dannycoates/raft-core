var P = require('p-promise')

function MemoryStorage() {
	this.data = {}
	this.entries = []
}

MemoryStorage.prototype.set = function (hash) {
	var keys = Object.keys(hash)
	for (var i = 0; i < keys.length; i++) {
		var key = keys[i]
		this.data[key] = hash[key]
	}
	return P()
}

MemoryStorage.prototype.get = function (keys) {
	var result = {}
	for(var i = 0; i < keys.length; i++) {
		var key = keys[i]
		result[key] = this.data[key]
	}
	return P(result)
}

MemoryStorage.prototype.appendEntries = function (startIndex, entries, state) {
	state = state || {}
	if (this.entries.length !== startIndex) {
		this.entries.splice(startIndex)
	}
	this.entries = this.entries.concat(entries)
	return this.set(state)
}

MemoryStorage.prototype.getEntries = function (startIndex) {
	return P(this.entries.slice(startIndex))
}

MemoryStorage.prototype.load = function () {
	return this.getEntries(0)
		.then(
			function (entries) {
				return this.get(Object.keys(this.data))
					.then(
						function (data) {
							data.entries = entries
							return data
						}
					)
			}.bind(this)
		)
}

module.exports = MemoryStorage
