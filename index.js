var kue = require('kue')
  , redis = require('redis');
  
module.exports = function(config) {
  var jobs;
  
  config = config || {};
    
  kue.redis.createClient = function() {
    var client = redis.createClient(config.port || 6379, config.host || '127.0.0.1');
    if (config.password) { client.auth(config.password); }
    return client;
  };
  
  jobs = kue.createQueue();
  
  function push(type, job, done) {
    jobs.create(type, job).save(done);
  };
  
  function pop(type, callback) {
    jobs.process(type, function(job, done) {
      callback(job.data, done);
    });
  }
  
  return {
      push: push
    , pop: pop
  }
};
