var redis = require('redis')
  , EventEmitter = require('events').EventEmitter
  , Monitor = require('./monitor');

module.exports = function(config) {
  var connections = []
    , active = 0
    , ending = false
    , that;
  
  config = config || {};
  
  config.popTimeout || (config.popTimeout = 5); // 5 seconds
  config.workerTimeout || (config.workerTimeout = 60 * 1000);
  
  function createClient() {
    var client = redis.createClient(config.port || 6379, config.host || '127.0.0.1');
    if (config.password) { client.auth(config.password); }
    return client;
  }

  function inactiveQueueName(type) {
    return 'q:jobs:' + encodeURIComponent(type) + ':inactive';
  }

  function activeQueueName(type) {
    return 'q:jobs:' + encodeURIComponent(type) + ':active';
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
      
      client
        .multi()
        .hset(jobId, 'job', JSON.stringify(job))
        .hset(jobId, 'created', Date.now())
        .lpush(inactiveQueueName(type), nextId)
        .exec(function(err) {
          if (err) { return error(err); }
          client.end();
          callback();
        });
    });
    
  };
  
  function connect(type, callback) {
    var connection = connections[type]
      , client
      , monitor;
    try {
      if (! connection) {
        client = createClient();
        monitor = Monitor(createClient(), inactiveQueueName(type), activeQueueName(type), config.workerTimeout, config.monitorInterval);
        monitor.on('error', function(err) {
                         that.emit('err', err);
                       });
                       monitor.on('timeout', function(job) {
                         that.emit('timeout', job);
                       });
        connections[type] = connection = {
            client: client
          , monitor: monitor
        };
      }
    } catch(err) {
      console.error(err);
      return callback(err);
    }
    callback(null, connection.client);
  }
  
  function disconnect(type) {
    var connection = connections[type];
    if (connection) {
      if (connection.client && connection.client.connected) {
        connection.client.end();
      }
      if (connection.monitor) {
        connection.monitor.end();
      }
      delete connections[type];
    }
  }
  
  function listen(type, callback) {
    connect(type, function(err, client) {
      if (err) { return that.emit('error', err); }
      var activeName = activeQueueName(type) ;
      client.brpoplpush(inactiveQueueName(type), activeName, config.popTimeout, function(err, jobId) {
        var jobDbId;
        if (err) { console.error('error performing brpoplpush', err); return that.emit('error', err); }
        
        if (! jobId) { return callback(null, null); }
        
        jobDbId = 'jobs:' + jobId;

        client.hget(jobDbId, 'job', function(err, jobStr) {
          if (err) { console.error('error on hget', err); return that.emit(err); }

          callback(JSON.parse(jobStr), function(cb) {
            client.lrem(activeName, 1, jobId, function(err) {
              if (err) { console.error('error on lrem', err); return that.emit(err); }
              client
                .multi()
                .hdel(jobDbId, 'job')
                .hdel(jobDbId, 'created')
                .exec(function(err) {
                  if (err) { console.error('error on multi hdel', err); return that.emit(err); }
                  cb();
                });
            });
          });
        });
      });
    });
  }
  
  
  
  function end(cb) {
    ending = true;
    if (cb) {
      if (active === 0) {
        return cb();
      }
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
