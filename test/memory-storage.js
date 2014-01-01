function MemoryStorage() {
	this.data = {}
	this.entries = []
}

MemoryStorage.prototype.set = function (hash, callback) {
	var keys = Object.keys(hash)
	for (var i = 0; i < keys.length; i++) {
		var key = keys[i]
		this.data[key] = hash[key]
	}
	process.nextTick(callback.bind(null, null))
}

MemoryStorage.prototype.get = function (keys, callback) {
	var result = {}
	for(var i = 0; i < keys.length; i++) {
		var key = keys[i]
		result[key] = this.data[key]
	}
	process.nextTick(callback.bind(null, null, result))
}

MemoryStorage.prototype.appendEntries = function (startIndex, entries, state, callback) {
	state = state || {}
	if (this.entries.length !== startIndex) {
		this.entries.splice(startIndex)
	}
	this.entries = this.entries.concat(entries)
	this.set(state, callback)
}

MemoryStorage.prototype.getEntries = function (startIndex, callback) {
	process.nextTick(callback.bind(null, null, this.entries.slice(startIndex)))
}

MemoryStorage.prototype.load = function (callback) {
	this.getEntries(
		0,
		function (err, entries) {
			this.get(
				Object.keys(this.data),
				function (err, data) {
					data.entries = entries
					callback(err, data)
				}
			)
		}.bind(this)
	)
}

module.exports = MemoryStorage
