global.expect = require('chai').expect
global.p      = console.log.bind(console)

var _       = require('underscore')
var RWQueue = require('../rwqueue')

var sync    = require('synchronize')
var async   = sync.asyncIt

describe("Read Write Queue", function(){
  it("writer should be processed first", function(next){
    var queue = new RWQueue()
    var result = []
    var called = false
    var finish = function(){
      if((result.length == 3) && !called){
        called = true
        expect(result).to.eql(['writer', 'reader', 'reader'])
        next()
      }
    }

    queue.addReader(function(err, release){
      if(err) throw err
      result.push('reader')
      process.nextTick(release)
      finish()
    })
    queue.addReader(function(err, release){
      if(err) throw err
      result.push('reader')
      process.nextTick(release)
      finish()
    })
    queue.addWriter(function(err, release){
      if(err) throw err
      result.push('writer')
      process.nextTick(release)
      finish()
    })
  })

  it("should work with fibers", function(next){
    var queue = new RWQueue()
    var result = []
    var called = false
    var finish = function(){
      if((result.length == 3) && !called){
        called = true
        expect(result).to.eql(['writer', 'reader', 'reader'])
        next()
      }
    }

    sync.fiber(function(){
      queue.addReader(sync.defer())
      var release = sync.await()
      result.push('reader')
      process.nextTick(sync.defer())
      sync.await()
      release()
      finish()
    })

    sync.fiber(function(){
      queue.addReader(sync.defer())
      var release = sync.await()
      result.push('reader')
      process.nextTick(sync.defer())
      sync.await()
      release()
      finish()
    })

    sync.fiber(function(){
      queue.addWriter(sync.defer())
      var release = sync.await()
      result.push('writer')
      process.nextTick(sync.defer())
      sync.await()
      release()
      finish()
    })
  })
})