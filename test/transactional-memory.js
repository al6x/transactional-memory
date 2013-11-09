global.expect = require('chai').expect
global.p      = console.log.bind(console)

var _  = require('underscore')
var tm = require('../transactional-memory')

describe("Transactional Memory", function(){
  describe('Multiple Operations and Transaction', function(){
    beforeEach(function(){
      this.data = {
        posts: [
          {text: 'A', tags: ['A1', 'A2']},
          {text: 'B', tags: ['B1', 'B2']}
        ]
      }
      this.originalData = tm.deepClone(this.data)
    })

    it("should apply transaction", function(){
      var llTransaction = tm.update(this.data, [
        {select: 'posts',              operation: ['add', {text: 'C'}]},
        {select: ['posts', 0, 'tags'], operation: ['add', 'A3']},
        ['set', 'version', 1]
      ])
      expect(this.data).to.eql({
        version : 1,
        posts   : [
          {text: 'A', tags: ['A1', 'A2', 'A3']},
          {text: 'B', tags: ['B1', 'B2']},
          {text: 'C'}
        ]
      })
    })

    it("should rollback all changes in transaction", function(){
      var that = this
      expect(function(){
        tm.update(that.data, [
          {select: 'posts',      operation: ['add', {text: 'C'}]},
          // Invalid operation, `add` not supported for Hashes.
          {select: ['posts', 0], operation: ['add', 'A3']}
        ])
      }).to.throw(/can't apply.*add.*A3.*operation/)
      expect(this.data).to.eql(this.originalData)
    })

    it("select should not generate low-level select if path is the same", function(){
      var data = {list: []}
      var llTransaction = tm.update(data, [
        {select: 'list', operation: ['add', 'A']},
        {select: 'list', operation: ['add', 'B']},
      ])
      expect(llTransaction).to.eql(['select', 1, 'list', 'aAdd', 1, 'A', 'aAdd', 1, 'B'])
    })
  })

  describe('Conditions', function(){
    it("should evaluate condition before applying operation", function(){
      var data = {posts: []}
      var checkArguments = function(arguments){
        var data           = arguments[0]
        var selectedObject = arguments[1]
        expect(data).to.eql({posts: []})
        expect(selectedObject).to.eql([])
      }
      tm.update(data, [
        {
          select    : 'posts',
          operation : ['add', 'A'],
          'if'      : function(){checkArguments(arguments); return false}
        },
        {
          select    : 'posts',
          operation : ['add', 'B'],
          'if'      : function(){checkArguments(arguments); return true}
        }
      ])
      expect(data).to.eql({posts: ['B']})
    })

    it("should rollback transaction in case of error in condition", function(){
      var data = {posts: []}
      expect(function(){
        tm.update(data, [
          {
            select    : 'posts',
            operation : ['add', 'A'],
            'if'      : function(){return true}
          },
          {
            select    : 'posts',
            operation : ['add', 'B'],
            'if'      : function(){throw new Error('error in condition')}
          }
        ])
      }).to.throw(/can't apply.*add.*B.*operation.*error in condition/)
      expect(data).to.eql({posts: []})
    })

    it("should use builtin conditions", function(){
      var data = {tags: {a: []}}
      tm.update(data, [
        {select: 'tags', operation: ['delete', 'a'],     'if'     : ['empty', 'a']},
        {select: 'tags', operation: ['set',    'b', []], 'unless' : ['exists', 'b']},
      ])
      expect(data).to.eql({tags: {b: []}})
    })

    it("should generate low level operations without conditions", function(){
      var data = {tags: {a: []}}
      var llTransaction = tm.update(data, [
        {select: 'tags', operation: ['delete', 'a'],  'if'     : ['empty', 'a']},
        {select: 'tags', operation: ['set', 'b', []], 'unless' : ['exists', 'b']}
      ])
      expect(data).to.eql({tags: {b: []}})
      expect(llTransaction).to.eql(['select', 1, 'tags', 'hDelete', 1, 'a', 'hSet', 2, 'b', []])
    })
  })

  describe('Validations', function(){
    it("should roll back transaction if validation failed", function(){
      var data = {posts: []}
      expect(function(){
        tm.update(data, [
          {select: 'posts', operation: ['add', 'A']},
          {validate: function(){return false}},
          {select: 'posts', operation: ['add', 'B']},
        ])
      }).to.throw(/can't apply.*validation failed/)
      expect(data).to.eql({posts: []})
    })
  })

  describe('Operations', function(){
    // Helpers.
    var hlTest = function(object, operationName, operationArgumentsWithoutObject, updatedObject){
      return function(){
        var originalObject = tm.deepClone(object)

        // Creating low level operations and rollback.
        operationArgumentsWithoutObject = _(operationArgumentsWithoutObject).clone()
        operationArgumentsWithoutObject.unshift(object)
        operationArgumentsWithoutObject.unshift(null)
        var result = tm[operationName].apply(null, operationArgumentsWithoutObject)
        var llOperations = result[0]
        var rollback   = result[1]

        // Updating object.
        tm.lowLevel.update(object, llOperations)
        expect(object).to.eql(updatedObject)

        // Rolling back.
        rollback()
        expect(object).to.eql(originalObject)
      }
    }

    it("`set` should set object in Array", hlTest(
      ['A', 'B', 'C'],
      'set', [1, 'B2'],
      ['A', 'B2', 'C']
    ))

    it("`set` should set object in Hash", hlTest(
      {a: 'A', b: 'B', c: 'C'},
      'set', ['b', 'B2'],
      {a: 'A', b: 'B2', c: 'C'}
    ))

    it("`add` should add object to Array", hlTest(
      ['A', 'B'],
      'add', ['C'],
      ['A', 'B', 'C']
    ))

    it("`delete` should delete object from Array by index", hlTest(
      ['A', 'B', 'C'],
      'delete', [1],
      ['A', 'C']
    ))

    it("`delete` should delete object from Hash by key", hlTest(
      {a: 'A', b: 'B', c: 'C'},
      'delete', ['b'],
      {a: 'A', c: 'C'}
    ))

    it("`deleteValue` should delete value from Array", hlTest(
      ['A', 'B', 'C'],
      'deleteValue', ['B'],
      ['A', 'C']
    ))

    it("`deleteValue` should delete value from Hash", hlTest(
      {a: 'A', b: 'B', c: 'C'},
      'deleteValue', ['B'],
      {a: 'A', c: 'C'}
    ))

    it("`deleteValue` should do nothing if there's no such value in Array", hlTest(
      ['A', 'B', 'C'],
      'deleteValue', ['B2'],
      ['A', 'B', 'C']
    ))

    it("`deleteValue` should do nothing if there's no such value in Hash", hlTest(
      {a: 'A', b: 'B', c: 'C'},
      'deleteValue', ['B2'],
      {a: 'A', b: 'B', c: 'C'}
    ))

    it("`deleteValue` should check for equality based on value", hlTest(
      [{a: 'A'}, {b: 'B'}, {c: 'C'}],
      'deleteValue', [{b: 'B'}],
      [{a: 'A'}, {c: 'C'}]
    ))

    it("`sort` should sort object in Array by its values", hlTest(
      ['C', 'B', 'A'],
      'sort', [],
      ['A', 'B', 'C']
    ))

    it("`sort` with attribute name should sort object in Array by its attribute", hlTest(
      [{name: 'C'}, {name: 'B'}, {name: 'A'}],
      'sort', ['name'],
      [{name: 'A'}, {name: 'B'}, {name: 'C'}]
    ))

    it("`select` without path should set root object", function(){
      var data = {posts: [{tags: ['A', 'B']}]}
      var llOperations = tm.select(data, null, [])
      expect(llOperations).to.eql(['select', 0])

      var transaction = ['set', 'k1', 'v1', 'select', 0, 'set', 'k2', 'v2']
      var selectedObject = tm.lowLevel.select(data, null, transaction, 5)
      expect(selectedObject).to.eql({posts: [{tags: ['A', 'B']}]})
    })
  })

  describe("Low level Operations", function(){
    it("should apply transaction", function(){
      var data = {posts: [{text: 'A'}]}
      tm.lowLevel.update(data, [
        'select', 2, 'posts', 0,
        'hSet',    2, 'text', 'B'
      ])
      expect(data).to.eql({posts: [{text: 'B'}]})
    })

    it("should use original, not changed objects", function(){
      var llTransaction = tm.update({}, [
        {operation: ['set', 'hash', {}]},
        {select: 'hash', operation: ['set', 'a', 'A']}
      ])
      // Should use original hash `{}`, not the updated hash `{a: 'A'}`.
      expect(llTransaction[3]).to.eql({})
      expect(llTransaction).to.eql([
        'hSet',   2, 'hash', {},
        'select', 1, 'hash',
        'hSet',   2, 'a', 'A'
      ])
    })
  })

  describe("Utilities", function(){
    it("should save operation stream as pretty JSON string", function(){
      var data = {posts: [{text: 'A', tags: []}]}
      var llTransaction = tm.update(this.data, [
        {select: 'posts',              operation: ['add', {text: 'C'}]},
        {select: ['posts', 0, 'tags'], operation: ['add', 'A1']},
        {                              operation: ['set', 'version', 1]}
      ])
      var parsed = JSON.parse(tm.toPrettyJson(llTransaction))
      expect(parsed).to.eql([
        "select", 1, "posts",
        "aAdd",   1, {"text":"C"},
        "select", 3, "posts", 0, "tags",
        "aAdd",   1, "A1",
        "select", 0,
        "hSet",   2, "version", 1
      ])
    })
  })
})