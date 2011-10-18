var redis = require('redis')
  , EventEmitter = require('events').EventEmitter;

module.exports = function(config) {
  var connections = []
    , active = 0
    , ending = false
    , that;
  
  config = config || {};
  
  config.popTimeout || (config.popTimeout = 5);
  
  function createClient() {
    var client = redis.createClient(config.port || 6379, config.host || '127.0.0.1');
    if (config.password) { client.auth(config.password); }
    return client;
  }

  function inactiveQueueName(type) {
    return 'q:jobs:' + type + ':inactive';
  }

  function activeQueueName(type) {
    return 'q:jobs:' + type + ':active';
  }
  
  function push(type, job, done) {
    var client = createClient();

    if (! done) {
      done = function(err) {
        if (err) {
          console.error(err); return that.emit('error', err);
        }
      }
    }

    function callback() {
      if (client && client.connected) {
        client.end();
      }
      done.apply(module, arguments);
    }
    
    
    function error(err) {
      if (err) {
        callback(err);
      }
    }
    
    
    client.incr('jobs:next_id', function(err, nextId) {
      if (err) { return error(err); }
      
      var jobId = 'jobs:' + nextId;

      client.set(jobId, JSON.stringify(job), function(err) {
        if (err) { return error(err); }
        client.lpush(inactiveQueueName(type), nextId, function(err) {
          if (err) { return error(err); }
          client.end();
          callback();
        });
      });
    });
    
  };
  
  function connect(type, callback) {
    var connection = connections[type];
    try {
      if (! connection || ! connection.connected) {
        connections[type] = connection = createClient();
      }
    } catch(err) {
      return callback(err);
    }
    callback(null, connection);
  }
  
  function disconnect(type) {
    var connection = connections[type];
    if (connection && connection.connected) {
      connection.end();
    }
  }
  
  function listen(type, callback) {
    connect(type, function(err, client) {
      if (err) { return that.emit('error', err); }
      var activeName = activeQueueName(type) ;
      client.brpoplpush(inactiveQueueName(type), activeName, config.popTimeout, function(err, jobId) {

        if (err) { return that.emit('error', err); }
        
        if (! jobId) { return callback(null, null); }

        client.get('jobs:' + jobId, function(err, jobStr) {
          if (err) { return that.emit(err); }

          callback(JSON.parse(jobStr), function(cb) {
            client.lrem(activeName, 1, jobId, function(err) {
              if (err) { return that.emit(err); }
              cb();
            });
          });
        });
      });
    });
  }
  
  
  
  function end(cb) {
    ending = true;
    if (cb) {
      that.on('done', function doneCB() {
        if (active === 0) {
          that.removeListener('done', doneCB);
          cb();
        }
      });
    }
  }
  
  function pop(type, callback) {
    if (! ending) {
      active += 1;
      listen(type, function(job, done) {
        if (job) {
          callback(job, function() {
            done(function() {
              active -= 1;
              that.emit('done');
              pop(type, callback);
            });
          });
        } else {
          active -= 1;
          pop(type, callback);
        }
      });
    } else {
      that.emit('done');
      disconnect(type);
    }
  }
  
  that = {
      end: end
    , push: push
    , pop: pop
  };
  
  that.__proto__ = EventEmitter.prototype;
  
  return that;
};
