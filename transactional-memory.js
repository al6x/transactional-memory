// Transactional memory updated using persistable operations stream.
//
// Blend of Transactional Memory, Operational Transformation, Command-Query Separation and Event Stream.
//
// Possible usages:
//
// - Keep multiple databases in sync with event stream (real-time web apps, via socket).
// - Persist set of changes to object and reply it later (append only databases).
// - Isolate readers and writers (non-blocking, well, almost non-blocking, in-memory database).
// - Transactions when all or none of changs applied (in-memory database).
// - Optimistic locking for concurrent updates (using versions and validations).
//
// I use it for append-only in memory ACID complient (with some limitations) database.

var tm = {lowLevel: {}}

// Checking if it's Node.js or Browser.
if(typeof module !== "undefined"){
  module.exports = tm
  var _       = global._ || require('underscore')
  var inspect = _(JSON.stringify).bind(JSON)
}else{
  var _       = global._ || require('underscore')
  var inspect = _(JSON.stringify).bind(JSON)
}

// Helper, deep clone.
// TODO use more efficient version for node.js.
tm.deepClone = function(object){return JSON.parse(JSON.stringify(object))}

// Apply transaction of high-level operations and return equivalent transaction of
// low-level operations.
//
// In case of error all changes will be rolled back, efficiency of this operation isn't
// very important.
tm.update = function(data, transaction){
  // Rollback all operations.
  var rollbacks = []
  var rollbackTransaction = function(err, operation){
    // Rolling back already applied operations in backward order.
    for(var i = rollbacks.length - 1; i >= 0; i--) rollbacks[i]()
    if(operation) throw new Error("can't apply " + inspect(operation) + " operation - " + err.message)
    else throw err
  }

  // High level operation don't change anything, they produce low-level operations that
  // actually change the data.
  // Every high-level operation converted to low-level, executed and added to low-level
  // transaction.
  var llTransaction = []
  var selectedObject = data
  var processLowLevelOperations = function(llOperations){
    try{
      selectedObject = tm.lowLevel.update(data, llOperations, 0, selectedObject)
    }catch(err){rollbackTransaction(err, originalOperation)}

    // Saving low-level operation. If there are multiple operations, later operations may change
    // data used in previous. For example - in code below after executing second operation hash
    // became `{a: 'A'}` not `{}`:
    //
    //     {operation: ['set', 'hash', {}]},
    //     {select: 'hash', operation: ['set', 'a', 'A']}
    //
    // To prevent that llOperations should be cloned.
    llOperations = tm.deepClone(llOperations)
    Array.prototype.push.apply(llTransaction, llOperations)
  }

  // Processing operations in transaction.
  var selectedPath = []
  for(var i = 0; i < transaction.length; i++){
    var originalOperation = transaction[i]
    var operation = transaction[i]

    // Parsing.
    if(_(operation).isArray()) operation = {operation: operation}
    operation.select = operation.select || []
    var condition, conditionSatisfied
    if(condition = operation['if']) conditionSatisfied = function(result){return result}
    else if(condition = operation['unless']) conditionSatisfied = function(result){return !result}
    var validation = operation.validate
    var selectPath = operation.select
    var operation  = operation.operation
    if(selectPath && !_(selectPath).isArray()) selectPath = [selectPath]
    if(condition &&!_(condition).isArray()) condition = [condition]
    if(validation && !_(validation).isArray()) validation = [validation]
    if(operation && !_(operation).isArray()) operation = [operation]

    // Selecting object, if select is the same as the current skipping it.
    if(!_(selectPath).isEqual(selectedPath)){
      try{
        var llSelectOperations = tm.select(data, selectedObject, selectPath)
      }catch(err){rollbackTransaction(err, originalOperation)}
      processLowLevelOperations(llSelectOperations)
      selectedPath = selectPath
    }

    // Checking condition and skipping the operation if it's not satisfied.
    if(condition){
      try{
        var func = tm._parseCondition(condition)
        if(!conditionSatisfied(func(data, selectedObject))) continue
      }catch(err){rollbackTransaction(err, originalOperation)}
    }

    // Checking validation and rolling back if it failed, if there's both condition
    // and validation - validation will be skipped if condition not satisfied.
    if(validation){
      try{
        var func = tm._parseCondition(validation)
        if(!func(data, selectedObject)) throw new Error("validation failed!")
      }catch(err){rollbackTransaction(err, originalOperation)}
    }

    // Processing operation.
    var operationName = operation[0]
    var func = tm[operationName]
    if(!func) rollbackTransaction(new Error("no '" + operationName + "' operation!"), originalOperation)

    // Preparing arguments.
    var arguments = operation.slice(1, operation.length)
    arguments.unshift(selectedObject)
    arguments.unshift(data)

    // Executing high-level operation to produce low-level operations.
    try{
      var result = func.apply(null, arguments)
    }catch(err){rollbackTransaction(err, originalOperation)}
    var llOperations = result[0]
    var rollback   = result[1]

    // Storing rollback.
    rollbacks.push(rollback)

    // Executing low-level operations.
    processLowLevelOperations(llOperations)
  }
  return llTransaction
}

// Apply transaction of low level operations.
//
// Note: this operation should be as efficient as possible, because when database loaded
// its huge event stream will be applied. So, the format of event stream and processing code
// isn't very simple and readable. If You know how to improove efficiency of it - let me
// know please.
//
// Transaction is just an Array of operation names and its attributes, counter is used
// to keep the state of current operation (for efficiency).
//
// There is special `select` operation that changes currently selected object, all other
// operations change state of that object.
//
// Unlike high-level apply, the Low-level apply doesn't support roll-back.
//
// `_tokenIndex` and `_selectedObject` needed for internal usage iniside of high-level apply.
tm.lowLevel.update = function(data, transaction, _tokenIndex, _selectedObject){
  var selectedObject = _selectedObject || data
  // Index of currently processed token in transaction.
  var tokenIndex = _tokenIndex || 0
  var operations = tm.lowLevel
  while(tokenIndex < transaction.length){
    // First token is the name of operation.
    var operationName = transaction[tokenIndex++]
    // Operations may have different size of arguments, in order to recognise such variable
    // length chunk in the stream of tokens the first token after the operation name is a number
    // of its arguments.
    var argumentsSize = transaction[tokenIndex++]
    if(operationName == 'select'){
      selectedObject = operations.select(data, selectedObject, transaction, tokenIndex)
    }else{
      operations[operationName](selectedObject, transaction, tokenIndex)
    }
    tokenIndex = tokenIndex + argumentsSize
  }
  // Needed for internal usage inside of high-level update.
  return selectedObject
}


// Set key of Array or Hash
tm.set = function(data, object, key, value){
  tm._validateObjectAndKey(object, key)
  if(_(object).isArray()){
    var oldLength = object.length
    var oldValue  = object[key]
    return [
      ['aSet', 2, key, value],
      function(){
        if(key < oldLength) object[key] = oldValue
        else object.splice(oldLength)
      }
    ]
  }else{
    var hadKey   = key in object
    var oldValue = object[key]
    return [
      ['hSet', 2, key, value],
      function(){
        if(hadKey) object[key] = oldValue
        else delete object[key]
      }
    ]
  }
}
tm.lowLevel.aSet = function(object, transaction, argumentsIndex){
  object[transaction[argumentsIndex]] = transaction[argumentsIndex + 1]
}
tm.lowLevel.hSet = function(object, transaction, argumentsIndex){
  object[transaction[argumentsIndex]] = transaction[argumentsIndex + 1]
}

// Add object to Array.
tm.add = function(data, object, value){
  // Validating and building rollback.
  if(!_(object).isArray()) throw new Error("can't add value " + inspect(value) + " to non Array!")
  return [
    ['aAdd', 1, value],
    function(){object.pop()}
  ]
}
tm.lowLevel.aAdd = function(object, transaction, argumentsIndex){
  object.push(transaction[argumentsIndex])
}

// Delete object from Array or Hash by index or key.
tm['delete'] = function(data, object, key){
  tm._validateObjectAndKey(object, key)
  if(_(object).isArray()){
    var oldLength = object.length
    var oldValue  = object[key]
    return [
      ['aDelete', 1, key],
      function(){
        if(key < oldLength) object.splice(key, 0, oldValue)
      }
    ]
  }else{
    var hadKey   = key in object
    var oldValue = object[key]
    return [
      ['hDelete', 1, key],
      function(){
        if(hadKey) object[key] = oldValue
      }
    ]
  }
}
tm.lowLevel.aDelete = function(object, transaction, argumentsIndex){
  var key = transaction[argumentsIndex]
  if(key < object.length) object.splice(key, 1)
}
tm.lowLevel.hDelete = function(object, transaction, argumentsIndex){
  var key = transaction[argumentsIndex]
  delete object[key]
}

// Delete value from Array or Hash.
tm.deleteValue = function(data, object, value){
  // Deleting by value is very inefficient, calculating index or key instead, and
  // using more efficient delete by index or key operation.
  tm._validateObjectOrArray(object)
  if(_(object).isArray()){
    var index = -1
    for(var i = 0; i < object.length; i++){
      if(_(object[i]).isEqual(value)){
        index = i
        break
      }
    }
    if(index >= 0){
      var oldValue = object[index]
      return [
        ['aDelete', 1, index],
        function(){
          if(index >= 0) object.splice(index, 0, oldValue)
        }
      ]
    }
  }else{
    var hasKey = false
    var key
    var oldValue
    for(var k in object){
      if(object[k] == value){
        hasKey = true
        key = k
        oldValue = object[k]
        break
      }
    }
    if(hasKey){
      return [
        ['hDelete', 1, key],
        function(){
          if(hasKey) object[key] = oldValue
        }
      ]
    }
  }

  return [[], function(){}]
}

// Sort Array by object values or attribute of object values.
//   ['sort']
//   ['sort', 'createdAt']
tm.sort = function(data, object, attrName){
  if(!_(object).isArray())
    throw new Error("can't sort non Array!")
  if(!(!attrName || _(attrName).isNumber() || _(attrName).isString()))
    throw new Error("invalid attribute for sorting " + inspect(attr) + " !")

  // Rollback.
  var oldArray = []
  for(var i = 0; i < object.length; i++) oldArray[i] = object[i]
  var rollback = function(){
    for(var i = 0; i < oldArray.length; i++) object[i] = oldArray[i]
  }

  if(attrName){
    return [
      ['aSortBy', 1, attrName],
      rollback
    ]
  }else{
    return [
      ['aSort', 0],
      rollback
    ]
  }
}
tm.lowLevel.aSort = function(object, transaction, argumentsIndex){
  object.sort()
}
tm.lowLevel.aSortBy = function(object, transaction, argumentsIndex){
  var attrName = transaction[argumentsIndex]
  object.sort(function(a, b){
    var vA = a[attrName]
    var vB = b[attrName]
    if(vA < vB) return -1
    if(vA > vB) return 1
    return 0
  })
}

// Helper, prints current object.
tm.p = function(data, object){
  console.log("\n")
  console.log(inspect(object))
  return [[], function(){}]
}

// Special operation, sets current object for other operations.
tm.select = function(data, currentObject, path){
  // Selecting object.
  var object = data
  for(var i = 0; i < path.length; i++){
    var key = path[i]
    tm._validateObjectAndKey(object, key)
    object = object[key]
  }
  tm._validateObjectOrArray(object, key)

  var llOperations = ['select', path.length]
  Array.prototype.push.apply(llOperations, path)
  return llOperations
}
// Special low level select operation, set current object for other operations.
tm.lowLevel.select = function(data, currentObject, transaction, argumentsIndex){
  var object = data
  var argumentsEndIndex = argumentsIndex + transaction[argumentsIndex - 1]
  for(; argumentsIndex < argumentsEndIndex; argumentsIndex++)
    object = object[transaction[argumentsIndex]]
  return object
}

// # Build-in conditions.
tm.conditions = {}

// Check if object or its attribute is empty.
tm.conditions.empty = function(data, currentObject, attribute){
  if(attribute) return _(currentObject[attribute]).isEmpty()
  else return _(currentObject).isEmpty()
}

// Check if attribute of object exists.
tm.conditions.exists = function(data, currentObject, attribute){
  return attribute in currentObject
}

// # Helpers.
tm._validateObjectAndKey = function(object, key){
  if(_(object).isArray()){
    if(!_(key).isNumber()) throw new Error("key " + inspect(key) + " for Array should be a Number!")
  }else if(_(object).isObject()){
    if(!(_(key).isNumber() || _(key).isString()))
      throw new Error("key " + inspect(key) + " for Hash should be Number or String!")
  }else
    throw new Error("object " + inspect(object) + " should be Hash or Array!")
}
tm._validateObjectOrArray = function(object, path){
  if(!(_(object).isArray() || _(object).isObject()))
    if(path) throw new Error("object for path " + inspect(path) + " should be Hash or Array!")
    else throw new Error("object should be Hash or Array!")
}

// Parses condition, in form of `[function]` or `[built-in-function, arg1, arg2, ...]`.
tm._parseCondition = function(condition){
  if(!_(condition).isArray())
    throw new Error("condition " + inspect(condition) + " should be passed as Array!")

  // Explicit function.
  if(_(condition[0]).isFunction()) return condition[0]

  // Built-in function.
  var funcName = condition[0]
  var func = tm.conditions[funcName]
  if(!func)
    throw new Error("no built-in function with " + inspect(funcName) + " name!")
  var attributes = condition.slice(1, condition.length)
  return function(data, selectedObject){
    attributes.unshift(selectedObject)
    attributes.unshift(data)
    return func.apply(null, attributes)
  }
}

// # Utilities.
//
// Takes current hash and set of changes and provides some hash functions as if
// it's a new hash.
// Set of changes should be in form of `{create: {}, update: {}, delete: {}}`.
tm.ComboHash = function(hash, changes){
  this.hash = hash
  this.changes = changes
}
tm.comboHash = function(hash, changes){return new tm.ComboHash(hash, changes)}

tm.ComboHash.prototype = {
  each: function(cb){
    for(var k in this.changes.create) cb(k, this.changes.create[k])
    for(var k in this.changes.update) cb(k, this.changes.update[k])
    for(var k in this.hash) if(!this._changed(k)) cb(k, this.hash[k])
  },

  get: function(key){
    if(key in this.changes.create) return this.changes.create[key]
    if(key in this.changes.update) return this.changes.update[key]
    if(!this._changed(key)) return this.hash[key]
  },

  has: function(key){
    if(key in this.changes.create) return true
    if(key in this.changes.update) return true
    if(!this._changed(key)) return key in this.hash
    return false
  },

  _changed: function(key){
    return (key in this.changes.create) || (key in this.changes.update) ||
      (key in this.changes.delete)
  }
}

// Save operation stream as pretty JSON string.
var _spaces = []
for(var k = 9; k >= 0; k--)
  for(var l = 0; l < k; l++)
    _spaces[k] = (_spaces[k] || '') + ' '
tm.toPrettyJson = function(lowLevelTransaction){
  var buffer = ["[\n"]
  var i = 0
  while(i < lowLevelTransaction.length){
    var operationName = JSON.stringify(lowLevelTransaction[i++])
    buffer.push(operationName, ', ')
    if(operationName.length < 10) buffer.push(_spaces[10 - operationName.length])
    var argumentsCount = lowLevelTransaction[i++]
    buffer.push(JSON.stringify(argumentsCount), ',  ')
    for(var j = 0; j < argumentsCount; j++){
      buffer.push(JSON.stringify(lowLevelTransaction[i++]))
      if(j < argumentsCount - 1) buffer.push(', ')
    }
    if(i < lowLevelTransaction.length){
      if(argumentsCount > 0) buffer.push(',')
      buffer.push("\n")
    }
  }
  buffer.push("\n]")
  return buffer.join('')
}