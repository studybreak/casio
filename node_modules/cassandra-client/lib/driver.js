/*
 *  Copyright 2011 Rackspace
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/** node.js driver for Cassandra-CQL. */

var util = require('util');
var constants = require('constants');
var EventEmitter = require('events').EventEmitter;

var thrift = require('thrift');
var async = require('async');
var Cassandra = require('./gen-nodejs/Cassandra');
var ttypes = require('./gen-nodejs/cassandra_types');

var Decoder = require('./decoder').Decoder;

var bufferToString = module.exports.bufferToString = require('./decoder').bufferToString;


// used to parse the CF name out of a select statement.
var selectRe = /\s*SELECT\s+.+\s+FROM\s+[\']?(\w+)/im;
var selectCountRe = /\s*SELECT\s+.*COUNT\(.+\)\s+FROM\s+[\']?(\w+)/im;

var appExceptions = ['InvalidRequestException', 'TimedOutException', 'UnavailableException',
  'SchemaDisagreementException'];

var nullBindError = {
  message: 'null/undefined query parameter'
};

//  how long we wait for a connection to time out.
var DEFAULT_CONNECTION_TIMEOUT = 4000;

// how long we wait for a query to finish.
var DEFAULT_MAX_QUERY = 100000;

/** Default timeout for each of the steps (login, learn, use) which are performed
* when the Connection to the Cassandra server has been established. */
var DEFAULT_STEP_TIMEOUTS = {
  'login': 1000,
  'learn': 2000,
  'use': 1000
};

/** converts object to a string using toString() method if it exists. */
function stringify(x) {
  // node buffers should be hex encoded
  if (x instanceof Buffer) {
    return x.toString('hex');
  }
  if (x.toString) {
    return x.toString();
  }
  return x;
}

/** wraps in quotes */
function quote(x) {
  return '\'' + x + '\'';
}

/** replaces single quotes with double quotes */
function fixQuotes(x) {
  return x.replace(/\'/img, '\'\'');
}

/**
 * binds arguments to a query. e.g: bind('select ?, ? from MyCf where key=?', ['arg0', 'arg1', 'arg2']);
 * quoting is handled for you.  so is converting the parameters to a string, preparatory to being sent up to cassandra.
 * @param query
 * @param args array of arguments. falsy values are never acceptable.
 * @return a buffer suitable for cassandra.execute_cql_query().
 */
function bind(query, args) {
  if (args.length === 0) {
    return query;
  }
  var q = 0;
  var a = 0;
  var str = '';
  while (q >= 0) {
    var oldq = q;
    q = query.indexOf('?', q);
    if (q >= 0) {
      str += query.substr(oldq, q-oldq);
      if (args[a] === null) {
        return nullBindError;
      }
      str += quote(fixQuotes(stringify(args[a++])));
      q += 1;
    } else {
      str += query.substr(oldq);
    }
  }
  return new Buffer(str);
}

/** returns true if obj is in the array */
function contains(a, obj) {
  var i = a.length;
  while (i > 0) {
    if (a[i-1] === obj) {
      return true;
    }
    i--;
  }
  return false;
}


var System = module.exports.System = require('./system').System;
var KsDef = module.exports.KsDef = require('./system').KsDef;
var CfDef = module.exports.CfDef = require('./system').CfDef;
var ColumnDef = module.exports.ColumnDef = require('./system').ColumnDef;
var BigInteger = module.exports.BigInteger = require('./bigint').BigInteger;
var UUID = module.exports.UUID = require('./uuid');


/**
 * Make sure that err.message is set to something that makes sense.
 *
 * @param {Object} err Error object.
 * @param {Object} connectionInfo Optional connection info object which is
 * attached to the error.
 */
function amendError(err, connectionInfo) {
  if (!err.message || err.message.length === 0) {
    if (err.name === "NotFoundException") {
      err.message = "ColumnFamily or Keyspace does not exist";
    } else if (err.why) {
      err.message = err.why;
    }
  }

  err.connectionInfo = connectionInfo;
  return err;
}

/** abstraction of a single row. */
var Row = module.exports.Row = function(row, decoder) {
  // decoded key.
  this.key = decoder.decode(row.key, 'key');

  // cols, all names and values are decoded.
  this.cols = []; // list of hashes of {name, value};
  this.colHash = {}; // hash of  name->value

  var count = 0;
  for (var i = 0; i < row.columns.length; i++) {
    if (row.columns[i].value && row.columns[i].name != 'KEY') {
      // avoid 'KEY' in column[name:value] as it is neat. Also it breaks specificValidators.
      var decodedName = decoder.decode(row.columns[i].name, 'comparator');
      var decodedValue = decoder.decode(row.columns[i].value, 'validator', row.columns[i].name);
      this.cols[count] = {
        name: decodedName,
        value: decodedValue
      };
      this.colHash[decodedName] = decodedValue;
      count += 1;
    }
  }

  this._colCount = count;
};

/** @returns the number of columns in this row. */
Row.prototype.colCount = function() {
  return this._colCount;
};

var CON_COUNTER = 0;

/**
 * @param options: valid parts are:
 *  user, pass, host, port, keyspace, use_bigints, timeout, log_time
 */
var Connection = module.exports.Connection = function(options) {
  options = options || {};
  this.validators = {};
  this.client = null;
  this.connectionInfo = options;
  this.timeout = options.timeout || DEFAULT_CONNECTION_TIMEOUT;
  this.max_query = options.max_query || DEFAULT_MAX_QUERY;
  this._id = ++CON_COUNTER;

  EventEmitter.call(this);
};
util.inherits(Connection, EventEmitter);


/**
 * makes the connection.
 * @param callback called when connection is successful or ultimately fails (err will be present).
 */
Connection.prototype.connect = function(callback) {
  var self = this,
      timeoutId;

  self.emit('log', 'info', 'connecting ' + self.connectionInfo.host + ':' + self.connectionInfo.port + '(' + this._id + ')');

  // build connection here, so that timeouts on bad hosts happen now and not in the constructor.
  this.con = thrift.createConnection(self.connectionInfo.host, self.connectionInfo.port);
  this.con.on('error', function(err) {
    clearTimeout(timeoutId);
    self.emit('log', 'error', self.connectionInfo.host + ':' + self.connectionInfo.port + ' error during connect.');
    amendError(err, self.connectionInfo);
    callback(err);
  });

  this.con.on('close', function() {
    clearTimeout(timeoutId);
    self.emit('log', 'info', self.connectionInfo.host + ':' + self.connectionInfo.port + ' is closed (' + self._id + ')');
  });

  this.con.on('connect', function() {
    clearTimeout(timeoutId);

    function decorateErrWithErrno(err, errno) {
      err.errno = errno;
      return err;
    }

    // preparing the conneciton is a 3-step process.

    // 1) login
    var login = function(cb) {
      if (self.connectionInfo.user || self.connectionInfo.pass) {
        var creds = new ttypes.AuthenticationRequest({user: self.connectionInfo.user, password: self.connectionInfo.pass});
        var timeoutId = setTimeout(function() {
          if (timeoutId) {
            timeoutId = null;
            cb(decorateErrWithErrno(new Error('login timed out'), constants.ETIMEDOUT));
          }
        }, DEFAULT_STEP_TIMEOUTS.login);
        self.client.login(creds, function(err) {
          if (timeoutId) {
            timeoutId = clearTimeout(timeoutId);
            if (err) { amendError(err, self.connectionInfo); }
            cb(err);
          }
        });
      } else {
        cb(null);
      }
    };

    // 2) learn.
    var learn = function(cb) {
      var timeoutId = setTimeout(function() {
        if (timeoutId) {
          timeoutId = null;
          cb(decorateErrWithErrno(new Error('learn timed out'), constants.ETIMEDOUT));
        }
      }, DEFAULT_STEP_TIMEOUTS.learn);
      self.client.describe_keyspace(self.connectionInfo.keyspace, function(err, def) {
        if (timeoutId) {
          timeoutId = clearTimeout(timeoutId);
          if (err) {
            amendError(err, self.connectionInfo);
            cb(err);
          } else {
            for (var i = 0; i < def.cf_defs.length; i++) {
              var validators = {
                key: def.cf_defs[i].key_validation_class,
                comparator: def.cf_defs[i].comparator_type,
                defaultValidator: def.cf_defs[i].default_validation_class,
                specificValidators: {}
              };
              for (var j = 0; j < def.cf_defs[i].column_metadata.length; j++) {
                // todo: verify that the name we use as the key represents the raw-bytes version of the column name, not
                // the stringified version.
                validators.specificValidators[def.cf_defs[i].column_metadata[j].name] = def.cf_defs[i].column_metadata[j].validation_class;
              }
              self.validators[def.cf_defs[i].name] = validators;
            }
            cb(null); // no errors.
          }
        }
      });
    };

    // 3) set the keyspace on the server.
    var use = function(cb) {
      var timeoutId = setTimeout(function() {
        timeoutId = null;
        cb(decorateErrWithErrno(new Error('use timed out'), constants.ETIMEDOUT));
      }, DEFAULT_STEP_TIMEOUTS.use);

      self.client.set_keyspace(self.connectionInfo.keyspace, function(err) {
        if (timeoutId) {
          timeoutId = clearTimeout(timeoutId);
          if (err) { amendError(err, self.connectionInfo); }
          cb(err);
        }
      });
    };

    async.series(
      [login, learn, use],
      function(err) {
        if (err) {
          self.close();
        }
        callback(err);
      }
    );
  });

  function connectTimeout() {
    var err = new Error('ETIMEDOUT, Operation timed out');
    err.errno = constants.ETIMEDOUT;

    try {
      self.con.connection.destroy(err);
    }
    catch (e) {}

    self.con = null;
  }

  // kicks off the connection process.
  this.client = thrift.createClient(Cassandra, this.con);

  // set a connection timeout handler
  timeoutId = setTimeout(connectTimeout, this.timeout);
};

/**
 * Closes the current connection
 *
 * `this.con` is a socket connection. For failed socket connections
 * `this.con.end()` may not trigger a `close` event. So in the cases where we
 * are experiencing problems with the connection, we can just `end()` it
 * without waiting for the `close` event.
 *
 * Note that the callback is only called, if the `close` event is fired. Also
 * we only wait for the `close` event if a callback is given.
 *
 * @param {function} callback
 */
Connection.prototype.close = function(callback) {
  var self = this;
  if (!callback) {
    self.con.end();
    self.con = null;
    self.client = null;
    return;
  }
  self.con.on('close', function(err) {
    self.con = null;
    self.client = null;
    callback();
  });
  self.con.end();
};

/**
 * executes any query
 * @param query any cql statement with '?' placeholders.
 * @param args array of arguments that will be bound to the query.
 * @param callback executed when the query returns. the callback takes a different number of arguments depending on the
 * type of query:
 *    SELECT (single row): callback(err, row)
 *    SELECT (mult rows) : callback(err, rows)
 *    SELECT (count)     : callback(err, count)
 *    UPDATE             : callback(err)
 *    DELETE             : callback(err)
 */
Connection.prototype.execute = function(query, args, callback) {
  var cql = bind(query, args);
  if (cql === nullBindError) {
    callback(new Error(nullBindError.message));
  } else {
    var self = this,
        cqlString = cql.toString(),
        start, end, diff;

    start = new Date().getTime();
    self.emit('log', 'cql', {'query': query, 'parameterized_query': cqlString, 'args': args});

    // if a connection dies at the right place, execute_cql_query never returns. make sure the callback gets called.
    var longQueryWatch = setTimeout(function() {
      self.emit('log', 'trace', "query is taking long: " + cql);
    },10000);
    var timeoutId = setTimeout(function() {
      callback(new Error('Connection timed out'));
      timeoutId = null;
    }, self.max_query);
    self.client.execute_cql_query(cql, ttypes.Compression.NONE, function(err, res) {
      clearTimeout(longQueryWatch);
      if (!timeoutId) {
        self.emit('log', 'warn', 'query returned after timeout: ' + cql);
        return;
      } else {
        clearTimeout(timeoutId);
      }
      
      end = new Date().getTime();
      diff = (end - start);
      if (self.connectionInfo.log_time) {
        self.emit('log', 'timing', {
          'query': query,
          'parameterized_query': cqlString,
          'args': args,
          'time': diff
        });
      }

      if (err) {
        amendError(err, self.connectionInfo);
        callback(err, null);
      } else if (!res) {
        callback(new Error('No results'), null);
      } else {
        if (res.type === ttypes.CqlResultType.ROWS) {
          var cfName = selectRe.exec(cql)[1];
          var decoder = new Decoder(self.validators[cfName], {use_bigints: self.connectionInfo.use_bigints, 
                                                              select_count: selectCountRe.test(cql)});
          // for now, return results.
          var rows = [];
          for (var i = 0; i < res.rows.length; i++) {
            var row = new Row(res.rows[i], decoder);
            rows.push(row);
          }
          rows.rowCount = function() {
            return res.rows.length;
          };
          callback(null, rows);
        } else if (res.type === ttypes.CqlResultType.INT) {
          callback(null, res.num);
        } else if (res.type === ttypes.CqlResultType.VOID) {
          callback(null);
        } else {
          callback(new Error('Execution unexpectedly got here. Result type is ' + res.type));
        }
      }
    });
  }
};


/**
 * pooled connection behave a bit different but offer the same service interface as regular connections.
 * This constructor behaves differently from the normal Connection since Connection() does some socket work.
 * that work is delayed to connect() here.
 */
var ConnectionInPool = module.exports.ConnectionInPool = function(options) {
  options.staleThreshold = options.staleThreshold || 10000;
  // cache options so that thrift setup can happen later.
  this._options = options;
  this.taken = false; // true when being used in a query.
  this.connected = false; // true when connected.
  this.unhealthyAt = 0; // timestamp this connection went bad.
}
util.inherits(ConnectionInPool, Connection);

/**
 * connects to the remote endpoint. 
 * @param callback
 */
ConnectionInPool.prototype.connect = function(callback) {
  var self = this;
  
  // pooled connections can be reestablished. track this and make sure the previous transport is torn down.
  // it occupies a fair amount of resources. the callback doesn't need to depend on this since the new transport
  // can be repointed before the old transport goes away.
  if (self._id !== undefined && self.con && self.client) {
    self.emit('log', 'debug', 'initiating proactive transport close');
    // ghetto close.
    var _con = self.con;
    _con.on('close', function(err) {
      self.emit('log', 'debug', 'proactive transport close finished');
    });
    _con.end();
  }
  Connection.call(this, this._options);
  Connection.prototype.connect.call(this, function(err) {
    self.connected = !err;
    self.unhealthyAt = err ? new Date().getTime() : 0;
    callback(err);
  });
};

ConnectionInPool.prototype.isHealthy = function() {
    return this.unhealthyAt === 0;
}

/**
 * a 'stale unhealthy' node is a node that has been bad for some period of time. After that
 * period, it is safe to retry the connection.
 */
ConnectionInPool.prototype.isStaleUnhealthy = function() {
  return !this.isHealthy() && new Date().getTime() - this.unhealthyAt > this._options.staleThreshold;
}

/**
 * Perform queries against a pool of open connections.
 *
 * Accepts a single argument of an object used to configure the new PooledConnection
 * instance.  The config object supports the following attributes:
 *
 *         hosts : List of strings in host:port format.
 *      keyspace : Keyspace name.
 *          user : User for authentication (optional).
 *          pass : Password for authentication (optional).
 *       maxSize : Maximum number of connection to pool (optional).
 *    idleMillis : Idle connection timeout in milliseconds (optional).
 *
 * Example:
 *
 *   var pool = new PooledConnection({
 *     hosts      : ['host1:9160', 'host2:9170', 'host3', 'host4'],
 *     keyspace   : 'database',
 *     user       : 'mary',
 *     pass       : 'qwerty',
 *     maxSize    : 25,
 *     idleMillis : 30000
 *   });
 *
 * @param config an object used to control the creation of new instances.
 */
var PooledConnection = module.exports.PooledConnection = function(config) {
  var self = this;
  config = config || {};
  this.connections = [];
  this.current_node = 0;
  this.use_bigints = config.use_bigints ? true : false;
  this.timeout = config.timeout || DEFAULT_CONNECTION_TIMEOUT;
  this.log_time = config.log_time || false;

  // Number of currently running queries
  this.running = 0;

  // Shutdown mode
  this.shuttingDown = false;

  // Construct a list of nodes from hosts in <host>:<port> form
  for (var i = 0; i < config.hosts.length; i++) {
    var hostSpec = config.hosts[i];
    if (!hostSpec) { continue; }
    var host = hostSpec.split(':');
    if (host.length > 2) {
      throw new Error('malformed host entry "' + hostSpec + '" (skipping)');
    }
    var connection = new ConnectionInPool({
      host: host[0],
      port: (isNaN(host[1])) ? 9160 : host[1],
      keyspace: config.keyspace,
      user: config.user,
      pass: config.pass,
      use_bigints: self.use_bigints,
      timeout: self.timeout,
      log_time: self.log_time
    });
    connection.on('log', function(level, message) {
      self.emit('log', level, message);
    });
    this.connections.push(connection);
  }

  EventEmitter.call(this);
};
util.inherits(PooledConnection, EventEmitter);

/**
 * increment the current node pointer, skipping over any bad nodes.  has a side-effect of resetting
 * unhealthy nodes that are stale (but not reconnecting them).
 * @return boolean indicating if all nodes are unhealthy.
 */
PooledConnection.prototype._incr = function() {
  var incrCount = 0;
  while (incrCount < this.connections.length) {
    incrCount += 1;
    this.current_node = (this.current_node + 1) % this.connections.length;
    if (this.connections[this.current_node]) {
      if (this.connections[this.current_node].isHealthy()) {
        break;
      } else if (this.connections[this.current_node].isStaleUnhealthy()) {
        // unhealthy and stale, so let reset the node (appears as if unconnected).
        this.connections[this.current_node].taken = false;
        this.connections[this.current_node].connected = false;
        this.connections[this.current_node].unhealthyAt = 0;
        break;
      } else {
        //`console.log('not healthy ' + this.current_node + ',' + incrCount);
      }
    }
  }
  // all nodes are unhealthy if we looped around and no healthy nodes were found.
  return incrCount >= this.connections.length && !this.connections[this.current_node].isHealthy();
};


/**
 * Establishes connections to all hosts in the pool.
 * @return callback expects err (if all the connect()s failed.
 */
PooledConnection.prototype.connect = function(callback) {
  var self = this;
  var errors = [];
  async.forEach(self.connections, function doConnect(con, callback) {
    con.connect(function(err) {
      if (err) {
        errors.push(err);
      }
      callback();
    });
  }, function() {
    if (errors.length === self.connections.length) {
      var error = new Error('There were errors connecting to every connection');
      error._individualErrors = errors;
      callback(error);
    } else {
      callback();
    }
  });
};


/**
 * executes any query
 * @param query any CQL statement with '?' placeholders.
 * @param args array of arguments that will be bound to the query.
 * @param callback executed when the query returns. the callback takes a different number of arguments depending on the
 * type of query:
 *    SELECT (single row): callback(err, row)
 *    SELECT (mult rows) : callback(err, rows)
 *    SELECT (count)     : callback(err, count)
 *    UPDATE             : callback(err)
 *    DELETE             : callback(err)
 */
PooledConnection.prototype.execute = function(query, args, executeCallback) {
  var self = this;

  if (self.shuttingDown) {
    executeCallback(new Error('Unable to execute query, connection pool is shutting down.'));
    return;
  }

  self.running++;
  var callback = function(err, results) {
    self.running--;
    executeCallback(err, results);
    if (self.running === 0) {
      self.emit('drain');
    }
  };

  self._getNextCon(function(err, con) {
    if (err) {
      callback(err, null);
    } else {
      try {
        con.execute(query, args, function(err, result) {
          con.taken = false;
          if (err) {
            if (err.hasOwnProperty('name') && contains(appExceptions, err.name)) {
              callback(err, null);
            } else {
              con.unhealthyAt = new Date().getTime();
              con.taken = false;
              self.emit('log', 'warn', 'setting unhealthy from execute ' + con.connectionInfo.host + ':' + con.connectionInfo.port + ',' + err.message);
              // try again.
              PooledConnection.prototype.execute.call(self, query, args, callback);
            }
          } else {
            callback(null, result);
          }
        });
      } catch (err) {
        // individual connection has failed.
        con.unhealthyAt = new Date().getTime();
        con.taken = false;
        self.emit('log', 'warn', 'setting unhealthy from catch outside execute ' + con.connectionInfo.host + ':' + con.connectionInfo.port);
        // try again.
        PooledConnection.prototype.execute.call(self, query, args, callback);
      }
    }
  });
};

/** gets the next untaken connection. errors when all connections are bad, or loop times out. */
PooledConnection.prototype._getNextCon = function(callback) {
  var self = this;
  var tryStart = new Date().getTime();
  var con = null;
  var allBad = false;
  var timedOut = false;
  var takens = [];
  var connectionInfo;
  async.whilst(function truthTest() {
    // should the timeout of getting a single connection be the sum of all connections?  Think of a scenario where the
    // timeout is N, but the first X nodes are unresponsive.  You still want to allow access to the subsequent good
    // nodes.
    timedOut = (new Date().getTime() - tryStart) >= (self.timeout * self.connections.length)
    return !allBad && con === null && !timedOut;
  }, function tryConnect(callback) {
    var c = self.connections[self.current_node];
    connectionInfo = c.connectionInfo;
    allBad = self._incr();
    if (c.taken) {
      takens[self.current_node] = takens[self.current_node] === undefined ? 1 : takens[self.current_node] + 1;
      if (takens[self.current_node] > 0) {
        // we've tried this node > 1 times and it still isn't available, this means that all other nodes are occupied
        // or down (we've looped around all nodes).  Continually checking will blow the stack, so lets wait 
        // 10 ms. before checking again.
        setTimeout(callback, 10);
      } else {
        callback();
      }
    } else if (c.unhealthyAt > 0) {
      callback();
    } else if (!c.connected) {
      c.taken = true;
      c.connect(function(err) {
        if (c.connected) {
          con = c;
        }
        // some errors we pass back. some we swallow and iterate over.
        if (err instanceof ttypes.NotFoundException) {
          callback(err, null);
        } else if (err && err.errno && err.errno === constants.ETIMEDOUT) {
          callback();
        } else {
          callback();
        }
      });
    } else {
      con = c;
      callback();
    }
  }, function whenDone(err) {
    if (timedOut && !err) {
      err = new Error('Timed out waiting for connection ' + (self.timeout * self.connections.length));
    } else if (allBad && !err) {
      err = new Error('All connections are unhealthy.');
    } else if (!con && !err) {
      err = new Error('connection was not set');
    }
    err = err ? amendError(err, connectionInfo) : err;
    callback(err, con);
  });
};

/**
 * Signal the pool to shutdown.  Once called, no new requests (read: execute())
 * can be made. When all pending requests have terminated, the callback is run.
 *
 * @param callback called when the pool is fully shutdown
 */
PooledConnection.prototype.shutdown = function(callback) {
  var self = this;

  callback = callback || function() {};

  // Start shutdown mode, causes no new execute()'s to be accepted
  if (self.shuttingDown) {
    var err = new Error('Already shutting down.');
    return callback(err);
  }
  self.shuttingDown = true;

  // Close all open connections as soon as the pool has drained
  self.once('drain', function() {
    self._closeConnections(function() {
      self.shuttingDown = false;
      callback();
    });
  });

  // If no queries were running, emit the drain event immediately
  if (self.running === 0) {
    self.emit('drain');
  }
};

/**
 * Close all connected connections.
 *
 * @param {function} closeCallback that is fired once all connections are closed
 */
PooledConnection.prototype._closeConnections = function(closeCallback) {
  async.forEach(this.connections, function(con, cb) {
    if (con.connected) {
      con.close(cb);
    } else {
      cb(null);
    }
  }, function(err) {
    closeCallback(err);
  });
};
