var DEFAULT_MONITOR_INTERVAL = 30000;
var EventEmitter = require('events').EventEmitter;

module.exports = exports = function(client, inactiveQueueName, activeQueueName, timeout, monitorInterval) {
  var that, interval;
  
  if (! monitorInterval) { monitorInterval = DEFAULT_MONITOR_INTERVAL; }

  function monitor() {
    interval = setInterval(function() {
      client.llen(activeQueueName, function(err, len) {
        var i;
        if (err) { return that.emit(err); }
        if (len === 0) { return; }
        for(i = 0; i < len; i++) {
          client.lindex(activeQueueName, i, function(err, jobId) {
            var jobIdStr;
            if (err) { return that.emit(err); }
            if (jobId) {
              jobIdStr = 'jobs:' + jobId;
              client.hget(jobIdStr, 'created', function(err, when) {
                if (err) { return that.emit(err); }
                if (when) {
                  if (when + timeout < Date.now()) {
                    client.hget(jobIdStr, 'job', function(err, jobStr) {
                      if (err) { return that.emit(err); }
                      if (jobStr) {
                        that.emit('timeout', JSON.parse(jobStr));
                      }
                    });
                  }
                }
              });
            }
          });
        }
      });
    }, monitorInterval);
  }
  
  function end() {
    if (interval) {
      clearInterval(interval);
    }
    if (client && client.connected) {
      client.end();
    }
  }
  
  monitor();
  
  that = {
    end: end
  };
  
  that.__proto__ = EventEmitter.prototype;
  
  return that;
};