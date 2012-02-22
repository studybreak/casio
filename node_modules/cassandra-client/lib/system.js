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

var util = require('util');
var EventEmitter = require('events').EventEmitter;

var thrift = require('thrift');
var Cassandra = require('./gen-nodejs/Cassandra');
var ttypes = require('./gen-nodejs/cassandra_types');

// re-export the standard thrift structures.
var CfDef = module.exports.CfDef = ttypes.CfDef;
var KsDef = module.exports.KsDef = ttypes.KsDef;
var ColumnDef = module.exports.ColumnDef = ttypes.ColumnDef;

/** encapsulates a connection (thrift proto) and a client bound to it. */
var ThriftConnection = function(thriftCon, thriftCli) {
  this.thriftCon = thriftCon;
  this.thriftCli = thriftCli;
};

ThriftConnection.prototype.tearDown = function() {
  this.thriftCon.end();
};

/** A pool of thrift connections. keeping this class around because driver.js will eventually use something like this */
var Pool = function(urns) {
  this.connecting = [];
  this.connections = [];
  this.connectors = [];
  this.index = 0;
  var self = this;
  for (var i = 0; i < urns.length; i++) {
    this.connections[i] = null;
    this.connecting[i] = false;
    var parts = urns[i].split(':');
    // lazy connection strategy.
    // btw, JS scoping rules really bum me out.
    this.connectors[i] = (function(addr, port, index) {
      return function(callback) {
        self._make_client(addr, port, index, callback);
      };
    })(parts[0], parts[1], i);
  }
  this.length = i;

  EventEmitter.call(this);
};
util.inherits(Pool, EventEmitter);

// makes the thrift connection+client.
Pool.prototype._make_client = function(addr, port, i, callback) {
  if (!this.connecting[i] && this.connections[i]) {
    if (callback) {
      callback(this.connections[i]);
    }
    return; // already connected.
  }
  this.connecting[i] = true;
  var con = thrift.createConnection(addr, port);
  var self = this;
  con.on('error', function(err) {
    self.emit('log', 'error', err);
    self.connections[i] = null;
    self.connecting[i] = false;
  });
  var client = thrift.createClient(Cassandra, con);
  self.emit('log', 'info', 'connected to ' + addr + ':' + port + '@' + i);
  self.connections[i] = new ThriftConnection(con, client);
  if (callback) {
    callback(this.connections[i]);
  }
  self.connecting[i] =  false;
};

/** borrows a connection from the pool. not sophisticated. */
Pool.prototype.getNext = function() {
  var stop = (this.length + this.index - 1) % this.length;
  var ptr = this.index;
  do {
    // some connections may need to be started up.
    if (!this.connections[ptr] && !this.connecting[ptr]) {
      this.connectors[ptr]();
    }
    if (this.connections[ptr]) {
      break;
    }
    else {
      ptr = (ptr + 1) % this.length;
    }
  } while (ptr != stop);

  // only increment index once.
  this.index = (this.index + 1) % this.length;

  return this.connections[ptr]; // may be null!
};

Pool.prototype.forEach = function(fn) {
  for (var i = 0; i < this.length; i++) {
    this.connectors[i](fn);
  }
};

/** returns the connection and closes the physical link to the cassandra server */
Pool.prototype.tearDown = function() {
  for (var i = 0; i < this.length; i++) {
    if (this.connections[i]) {
      this.connections[i].tearDown();
    }
    this.connecting[i] = false;
    this.connections[i] = null;
  }
};

/** Naïve FIFO queue */
function Queue(maxSize) {
  var items = [];
  var putPtr = 0;
  var takePtr = 0;
  var max = maxSize;
  var curSize = 0;

  this.put = function(obj) {
    if (curSize == max) {
      return false;
    }
    if (items.length < max) {
      items.push(obj);
    }
    else {
      items[putPtr] = obj;
    }
    putPtr = (putPtr + 1) % max;
    curSize += 1;
    return true;
  };

  this.take = function() {
    if (curSize === 0) {
      return false;
    }
    var item = items[takePtr];
    items[takePtr] = null;
    takePtr = (takePtr + 1) % max;
    curSize -= 1;
    return item;
  };

  this.size = function() {
    return curSize;
  };
}

/** system database */
var System = module.exports.System = function(urn) {
  EventEmitter.call(this);
  this.q = new Queue(500);
  this.pool = new Pool([urn]);
  
  var self = this;
  this.on('checkq', function() {
    var con = self.pool.getNext();
    if (!con) {
      // no connection is available. create a timer event to check back in a bit to see if there is a con available to
      // do work.
      setTimeout(function() {
        self.emit('checkq');
      }, 25);
    } else {
      // drain the work queue.
      while (self.q.size() > 0) {
        self.q.take()(con);
      }
    }
  });
  
  this.q.put(function(con) {
    con.thriftCli.set_keyspace('system', function(err) {
      if (err) {
        throw Error(err);
      }
    });
  });
};
util.inherits(System, EventEmitter);

/** adds a keyspace */
System.prototype.addKeyspace = function(ksDef, callback) {
  this.q.put(function(con) {
    con.thriftCli.system_add_keyspace(ksDef, callback);
  });
  this.emit('checkq');
};

/** gets keyspace information */
System.prototype.describeKeyspace = function(ksName, callback) {
  this.q.put(function(con) {
    con.thriftCli.describe_keyspace(ksName, callback);
  });
  this.emit('checkq');
};

/** shuts down thrift connection */
System.prototype.close = function(callback) {
  var self = this;
  this.q.put(function() {
    self.pool.tearDown();
    if (callback) {
      callback();
    }
  });
  this.emit('checkq');
};
