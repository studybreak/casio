var _ = require('underscore');
var Cassandra = require('cassandra-client');
var EventEmitter = require('events').EventEmitter;
var util = require('util');


var BucketedConnectionPool = module.exports = function (options) {
  this._options = options;

  this.waiting = [];
  this.serverPools = [];
  this.nextServer = 0;
  this.executeRetry = 3;
  
  if (options.timeout) {
    Cassandra.DEFAULT_STEP_TIMEOUTS['login'] = options.timeout;
    Cassandra.DEFAULT_STEP_TIMEOUTS['learn'] = options.timeout;
    Cassandra.DEFAULT_STEP_TIMEOUTS['use'] = options.timeout;
  }

  this.createPools();

  EventEmitter.call(this);
};
util.inherits(BucketedConnectionPool, EventEmitter);

BucketedConnectionPool.prototype.appExceptions = [
  'InvalidRequestException',
  'TimedOutException',
  'UnavailableException',
  'SchemaDisagreementException'
];

BucketedConnectionPool.prototype.createPools = function () {
  var self = this;

  // Construct a set of connection pools one per host in options.hosts
  var hosts = self._options.hosts;
  for (var i = 0; i < hosts.length; i++) {
    if (!hosts[i]) continue;

    var parts = hosts[i].split(':');
    if (parts.length > 2) {
      throw new Error('Malformed host entry "' + hostSpec + '"');
    }

    var pool = new ConnectionPool(_.extend({}, self._options, {
      host: parts[0],
      port: (isNaN(parts[1])) ? 9160 : parts[1],
    }));

    bubbleEvent(self, pool, 'log');

    // When any pool has a connection become available, send it to a waiter.
    pool.on('available', function (avail) {
      if (!self.waiting.length) return;
      var waiter = self.waiting.shift();
      var connection = avail.acquire();
      return waiter(null, connection);
    });

    self.serverPools.push(pool);
  }
};

BucketedConnectionPool.prototype.acquire = function (callback) {
  var startServer = this.nextServer;
  var healthy, healthyCount = 0;
  var nextPool, connection;
  
  do {
    nextPool = this.serverPools[this.nextServer];

    this.nextServer += 1;
    if (this.nextServer == this.serverPools.length) this.nextServer = 0;

    healthy = nextPool.isHealthy();
    healthyCount += Number(healthy);

    if (healthy) {
      connection = nextPool.acquire();
    }
  } while (!connection && startServer != this.nextServer);

  if (!healthyCount) {
    return callback(new Error('All connections are unhealthy.'));
  }

  if (connection) {
    return callback(null, connection);
  }
  // There are no available connections. Add this acquisition to a waiter queue.
  // Will be given a connection when one is returned to the pool.
  else {
    return this.waiting.push(callback);
  }
};

BucketedConnectionPool.prototype.release = function (connection) {
  return connection._connectionpool.release(connection);
};

BucketedConnectionPool.prototype.execute = function (query, args, callback) {
  this.retry(0, query, args, callback);
};

BucketedConnectionPool.prototype.retry = function (attempt, query, args, callback) {
  var self = this;
  
  var tryThis = function(connection) {
    try {
      connection.execute(query, args, function (err, results) {
        attempt+=1;
        var hasUnhealthyErr = err && (!err.hasOwnProperty('name') || 
                                      self.appExceptions.indexOf(err.name) === -1);
        if (hasUnhealthyErr) {
          connection._connectionpool.setUnhealthy();
          self.emit('log', 'error', 'Set connection to unhealthy after execute', err);
        }
        
        // release this connection
        self.release(connection);
        
        if (!hasUnhealthyErr || attempt >= self.executeRetry) {
          // we don't have an unhealthy error, 
          // or we hit the retry limit
          return callback(err, results);
        } else {
          // ok, let's try again, this time with a different connection/host pool
          return self.retry(attempt, query, args, callback);
        }
      });
    }
    catch (err) {
      connection._connectionpool.setUnhealthy();
      self.emit('log', 'error', 'Set connection to unhealthy on exception', err);
      self.release(connection);

      if (attempt >= self.executeRetry) {
        return callback(err);
      } else {
        return self.retry(attempt, query, args, callback);        
      }
    }
  };

  self.acquire(function (err, connection) {
    if (err) return callback(err);
    tryThis(connection);
  });  
};

var ConnectionPool = function (options) {
  this._options = _.extend({
    maxPoolSize: 1
  }, options);

  this.minStaleThreshold = 1000;
  this.resetThreshold();        // initialize this.staleThreshold  
  this.unhealthyAt = 0;         // timestamp of the most recent server failure
  this.connections = [];
  this.available = [];
  this.connectionExists = false;
  
  // let's try and create one connection
  this.create();
  
  EventEmitter.call(this);

};
util.inherits(ConnectionPool, EventEmitter);

ConnectionPool.prototype.setUnhealthy = function () {
  this.unhealthyAt = new Date().getTime();
  this.backoff();
};

ConnectionPool.prototype.isHealthy = function() {
  // The connection is be considered healthy again after the staleThreshold.
  if (new Date().getTime() - this.unhealthyAt > this._options.staleThreshold) {
    this.unhealthyAt = 0;
    this.resetThreshold();
  }
  return this.unhealthyAt === 0;
};

ConnectionPool.prototype.hasAvailable = function () {
  return this.available.length;
};

ConnectionPool.prototype.backoff = function () {
  this._options.staleThreshold *= 2;
};

ConnectionPool.prototype.resetThreshold = function () {
  this._options.staleThreshold = this.minStaleThreshold;
};

/**
 * Acquire a connection from the connection pool. The connection is returned
 * to the callback when a connection is ready. If no connections are available,
 * the callback is placed on a queue and is sent a connection when the next
 * connection is released.
 *
 * @param callback
 * @returns
 */
ConnectionPool.prototype.acquire = function () {
  var self = this;
  
  // Don't hand out connections during closing or unhealthy times.
  if (!this.isHealthy() || !this.connectionExists || this.closing) {
    return null;
  }
  
  // There's already a connection free, acquire it and return it.
  if (this.available.length) {
    return this.available.pop();
  }
  
  // Create a new connection if there are less than maxPoolSize already.
  // Don't return it. It will be available after the connection is established.
  // Just use this acquire as a signal that the pool should be grown.
  if (this.connections.length < this._options.maxPoolSize) {
    var connection = this.create();
    if (connection) {
      self.connections.push(connection);
    }
  }

  return null;
};

ConnectionPool.prototype.create = function () {

  var self = this;
  var connection = new Cassandra.Connection(this._options);
  connection._connectionpool = this;

  bubbleEvent(self, connection, 'log');

  // Catch errors, close the client and remove it from the pool.
  connection.on('error', function (err) {
    self.emit('log', 'error', 'Cassandra error', err);
    self.remove(connection);
  });
  
  connection.connect(function (err) {
    connection._connected = !err;
    
    if (err) {
      self.emit('log', 'error', 'Error connecting to Cassandra', err);
      self.emit('log', 'error', 'Set connection to unhealthy on connect', err);
      
      // we're going to set this as unhealthy
      // so we can try creating again
      // and hit the backoff...
      self.setUnhealthy();
      self.remove(connection);

      var host = [self._options.host,self._options.port].join(':'); 
      self.emit('log', 'info', 'Retrying first connection to Cassandra for host ' + 
                                host + ' in ' + self._options.staleThreshold + 'ms');

      // now try and connect again...
      // and back using our backoff threshold
      setTimeout(function(){
        if (!self.connectionExists) {
          self.create();
        }
      }, self._options.staleThreshold)
    }
    else {
      self.connectionExists = true;
      self.release(connection);
    }
  });
  
  return connection;
};

ConnectionPool.prototype.remove = function (connection) {
  
  var self = this;

  // Remove all references to the connection
  self.connections.splice(self.connections.indexOf(connection), 1);
  self.available.splice(self.available.indexOf(connection), 1);

  connection._connected = false;
  // Don't reclose a connection
  if (!connection.con) {
    self.emit('remove', connection);
  }
  else {
    connection.close(function () {
      self.emit('remove', connection);
    });
  }
};

/**
 * Releases a connection to be used again by another query. Must be called to
 * avoid starving other consumers.
 *
 * @param connection
 * @returns
 */
ConnectionPool.prototype.release = function (connection) {
  var self = this;

  // Don't accept connections to other servers.
  if (connection._connectionpool !== self) return;

  // Don't return bad connections to the free list.
  if (!connection._connected) return self.remove(connection);

  // Drain while the pool is closing
  if (self.closing) return self.remove(connection);

  // Don't allow double free
  if (self.available.indexOf(connection) >= 0) return;

  // Released connections go back on the free list.
  // Add it back and announce on nextTick to avoid letting another thread
  // run. This prevents this function from having unpredictable runtime from
  // the releasing thread's point of view.
  // Waiting to add it back to the availbable list prevents someone from
  // stealing the connection from waiters already in the queue.
  process.nextTick(function () {
    self.available.push(connection);
    self.emit('available', self);
  });
};

ConnectionPool.prototype.close = function (callback) {
  var self = this;

  // When all of the connections have been removed, call the callback.
  var remove = function () {
    if (!self.connections.length) {
      self.removeListener('remove', remove);
      self.closing = false;
      return callback();
    }
  };
  self.on('remove', remove);

  // Start by closing all the free connections.
  self.closing = true;
  self.available.slice().forEach(function (connection) {
    self.remove(connection);
  });
};


var bubbleEvent = function (parent, child, name) {
  child.on(name, function () {
    var args = [name].concat(Array.prototype.slice.call(arguments));
    parent.emit.apply(parent, args);
  });
};
