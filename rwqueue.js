var _ = require('underscore')

var RWQueue = function(){this.initialize.apply(this, arguments)}

// Multiple readers may read simultaneously, readers can be added to any time.
// Writer requires exclusive access, writer can be queued only if there's no other writer executing or
// queued already.
// Writer and reader have timeouts (in milliseonds).
RWQueue.prototype = {
  initialize: function(options){
    this.options = options || {queueSizeLimit: 100}
    this.queuedReaders = []
    this.queuedWriter  = null
    this.activeReaders = {}
    this.activeWriter  = null
    // Unique id generator.
    var _lastId = 0
    this.nextId = function(){return _lastId++}

    // Doing reschedule in next tick to prevent tight coupling and long stack traces.
    this.reschedule = function(){
      var bindedReschedule = this._reschedule.bind(this)
      process.nextTick(bindedReschedule)
    }
  },

  // Add reader to queue.
  addReader: function(info, cb){
    if(!cb){
      cb   = info
      info = null
    }
    if(this.options.disabled) return cb(null, function(){})
    if(this.queuedReaders.length > this.options.readerQueueSizeLimit)
      throw new Error("can't add reader, readers queue limit reached!")
    this.queuedReaders.push({info: info, callback: cb})
    this.reschedule()
  },

  // Add writer to queue.
  addWriter: function(info, cb){
    if(!cb){
      cb   = info
      info = null
    }
    if(this.options.disabled) return cb(null, function(){})
    if(this.queuedWriter) throw new Error("can't add writer, there's already queued writer!")
    if(this.activeWriter) throw new Error("can't add writer, there's already active writer!")
    this.queuedWriter = {info: info, callback: cb}
    this.reschedule()
  },

  _reschedule: function(){
    if(this.activeWriter) return

    // If there's a queued writer it taked before queued readers.
    if(this.queuedWriter){
      if(_(this.activeReaders).isEmpty())
        var info     = this.queuedWriter.info
        var callback = this.queuedWriter.callback
        this.queuedWriter = null
        this.activeWriter = {info: info, callback: callback, startTime: Date.now()}
        var releaseWriter = function(){
          this.activeWriter = null
          this.reschedule()
        }.bind(this)
        callback(null, releaseWriter)
    }else{
      if(!this.activeWriter && (this.queuedReaders.length > 0)){
        var tuple
        while(tuple = this.queuedReaders.shift()){
          var info     = tuple.info
          var callback = tuple.callback
          var id = this.nextId()
          this.activeReaders[id] = {info: info, callback: callback, startTime: Date.now()}
          var releaseReader = function(){
            delete this.activeReaders[id]
            this.reschedule()
          }.bind(this)
          callback(null, releaseReader)
        }
      }
    }
  }
}

module.exports = RWQueue