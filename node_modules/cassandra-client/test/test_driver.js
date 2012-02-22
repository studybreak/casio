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


var assert = require('assert');
var console = require('console');
var EventEmitter = require('events').EventEmitter;
var http = require('http');

var async = require('async');

var BigInteger = require('../lib/bigint').BigInteger;

var Connection = require('../lib/driver').Connection;
var PooledConnection = require('../lib/driver').PooledConnection;
var ConnectionInPool = require('../lib/driver').ConnectionInPool;
var ttypes = require('../lib/gen-nodejs/cassandra_types');
var Keyspace = require('../node-cassandra-client').Keyspace;
var System = require('../lib/system').System;
var KsDef = require('../lib/system').KsDef;
var CfDef = require('../lib/system').CfDef;
var UUID = require('../lib/driver').UUID;
var util = require('./util');
var decoder = require('../lib/decoder');

var CASSANDRA_PORT = 19170;

function merge(a, b) {
  var c = {}, attrname;

  for (attrname in a) {
    if (a.hasOwnProperty(attrname)) {
      c[attrname] = a[attrname];
    }
  }
  for (attrname in b) {
    if (b.hasOwnProperty(attrname)) {
      c[attrname] = b[attrname];
    }
  }
  return c;
};

function stringToHex(s) {
  var buf = '';
  for (var i = 0; i < s.length; i++) {
    buf += s.charCodeAt(i).toString(16);
  }
  return buf;
}


function connect(options, callback) {
  if (typeof options === 'function') {
      callback = options;
      options = {};
  }

  var defaultOptions = {
      host: '127.0.0.1',
      port: CASSANDRA_PORT,
      keyspace: 'Keyspace1',
      use_bigints: true
  };
  var connOptions = merge(defaultOptions, options);
  var handler = new EventEmitter();
  handler.on('error', function(err) {
    callback(err, null);
  });
  handler.on('ready', function(con) {
    callback(null, con);
  });
  var con = new Connection(connOptions);
  con.on('log', function(level, message) {
    if (['cql'].indexOf(level) !== -1) {
      return;
    }
    console.log('log event: %s -- %j', level, message);
  });
  con.connect(function(err) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, con);
    }
  });
}

exports.setUp = function(test, assert) {
  var sys = new System('127.0.0.1:'+CASSANDRA_PORT);
  var ksName = 'Keyspace1';
  var close = function() {
    sys.close(function() {
      sys.close();
      console.log('System keyspace closed');
    });
  };
  sys.describeKeyspace(ksName, function(descErr, ksDef) {
    if (descErr) {
      console.log('adding test keyspace');
      var standard1 = new CfDef({keyspace: ksName, name: 'Standard1', column_type: 'Standard', comparator_type: 'UTF8Type', default_validation_class: 'UTF8Type'});
      var cfLong = new CfDef({keyspace: ksName, name: 'CfLong', column_type: 'Standard', comparator_type: 'LongType', default_validation_class: 'LongType', key_validation_class: 'LongType'});
      var cfInt = new CfDef({keyspace: ksName, name: 'CfInt', column_type: 'Standard', comparator_type: 'IntegerType', default_validation_class: 'IntegerType', key_validation_class: 'IntegerType'});
      var cfUtf8 = new CfDef({keyspace: ksName, name: 'CfUtf8', column_type: 'Standard', comparator_type: 'UTF8Type', default_validation_class: 'UTF8Type', key_validation_class: 'UTF8Type'});
      var cfBytes = new CfDef({keyspace: ksName, name: 'CfBytes', column_type: 'Standard', comparator_type: 'BytesType', default_validation_class: 'BytesType', key_validation_class: 'BytesType'});
      var cfUuid = new CfDef({keyspace: ksName, name: 'CfUuid', column_type: 'Standard', comparator_type: 'TimeUUIDType', default_validation_class: 'TimeUUIDType', key_validation_class: 'TimeUUIDType'});
      var cfUgly = new CfDef({keyspace: ksName, name: 'CfUgly', column_type: 'Standard', comparator_type: 'UTF8Type',
                              default_validation_class: 'LongType', key_validation_class: 'IntegerType',
                              column_metadata: [
                                new ttypes.ColumnDef({name: 'int_col', validation_class: 'IntegerType'}),
                                new ttypes.ColumnDef({name: 'string_col', validation_class: 'UTF8Type'}),
                                new ttypes.ColumnDef({name: 'uuid_col', validation_class: 'TimeUUIDType'})
                              ]});
      var cfCounter = new CfDef({keyspace: ksName, name: 'CfCounter', column_type: 'Standard', comparator_type: 'AsciiType', default_validation_class: 'CounterColumnType', key_validation_class: 'AsciiType'});
      var super1 = new CfDef({keyspace: ksName, name: 'Super1', column_type: 'Super', comparator_type: 'UTF8Type', subcomparator_type: 'UTF8Type'});
      var keyspace1 = new KsDef({name: ksName, strategy_class: 'org.apache.cassandra.locator.SimpleStrategy', strategy_options: {'replication_factor': '1'}, cf_defs: [standard1, super1, cfInt, cfUtf8, cfLong, cfBytes, cfUuid, cfUgly, cfCounter]});
      sys.addKeyspace(keyspace1, function(addErr) {
        console.log(addErr);
        close();
        if (addErr) {
          assert.ifError(addErr);
        } else {
          console.log('keyspace created');
          test.finish();
        }
      });
    } else {
      close();
      console.log(ksDef.name + ' keyspace already exists');
      test.finish();
    }
  });
};

exports.tearDown = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ifError(err);
      con.close();
      test.finish();
      return;
    }

    con.execute('DROP KEYSPACE ?', ['Keyspace1'], function(dropErr, res) {
      assert.ifError(dropErr);
      con.close();
      test.finish();
    });
  });
};

exports.testWhiskyIsWorking = function(test, assert) {
  assert.throws(function() {
      assert.ok(false);
  }, require('assert').AssertionError);
  test.finish();
};

exports.testNoResults = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      con.close();
      assert.ifError(err);
      test.finish();
    } else {
      con.execute('select * from CfLong where key=999999999', [], function(err, rows) {
        con.close();
        assert.ok(rows);
        // NOTE: There is a bug in some newer minor cassandra versions so this
        // check is temporary commented out
        // https://issues.apache.org/jira/browse/CASSANDRA-3424
        //assert.strictEqual(rows.rowCount(), 0);
        assert.strictEqual(err, null);
        test.finish();
      });
    }
  });
};

exports.testSelectCount = function(test, assert) {
  var con = null;

  async.waterfall([
    connect,

    function executeCountQuery(_con, callback) {
      con = _con;
      con.execute('SELECT COUNT(*) FROM CfLong', [], function(err, rows) {
        assert.ifError(err);
        assert.equal(rows[0].cols[0].value, 0);
        callback();
      });
    },

    function insertFiveRows(callback) {
      async.forEach([1, 2, 3, 4, 5], function(i, callback) {
        con.execute('UPDATE CfLong SET 1=1 WHERE key=?', [i], callback);
      }, callback);
    },

    function executeCountQuery(callback) {
      con.execute('SELECT COUNT(*) FROM CfLong', [], function(err, rows) {
        assert.ifError(err);
        assert.strictEqual(rows[0].cols[0].value, 5);
        callback();
      });
    },
  ],

  function(err) {
    if (con) {
      con.close();
    }

    assert.ifError(err);
    test.finish();
  });
};

exports.testSimpleUpdate = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ifError(err);
      test.finish();
    } else {
      var key = stringToHex('key0');
      con.execute('update Standard1 set ?=?, ?=? where key=?', ['cola', 'valuea', 'colb', 'valueb', key], function(updateErr) {
        if (updateErr) {
          con.close();
          assert.ifError(updateErr);
          test.finish();
        } else {
          con.execute('select ?, ? from Standard1 where key=?', ['cola', 'colb', key], function(selectErr, rows) {
            con.close();
            assert.ifError(selectErr);
            assert.strictEqual(rows.rowCount(), 1);
            var row = rows[0];
            assert.strictEqual('cola', row.cols[0].name.toString());
            assert.strictEqual('valuea', row.cols[0].value.toString());
            test.finish();
          });
        }
      });
    }
  });
};

exports.testConnectToBadUrl = function(test, assert) {
  connect({port:19171}, function(err, con) {
    assert.ok(err);
    assert.strictEqual(err.code, 'ECONNREFUSED');
    test.finish();
  });
};

exports.testConnectionKeyspaceDoesNotExistConnect = function(test, assert) {
  connect({keyspace: 'doesnotexist.'}, function(err, conn) {
    assert.ok(err);
    assert.equal(err.name, 'NotFoundException')
    assert.equal(err.message, 'ColumnFamily or Keyspace does not exist');
    assert.ok(!conn);
    test.finish();
  });
};

exports.testPooledConnectionKeyspaceDoesNotExistConnect = function(test, assert) {
  var con = new PooledConnection({hosts: ['127.0.0.1:19170'],
                                  keyspace: 'doesNotExist.',
                                  use_bigints: false});
  con.execute('SELECT * FROM foo', [], function(err) {
    assert.ok(err);
    assert.equal(err.message, 'ColumnFamily or Keyspace does not exist');
    assert.equal(err.name, 'NotFoundException')
    test.finish();
  });
};

exports.testCounterUpdate = function(test, assert) {
  connect(function(err0, con) {
    assert.ifError(err0);
    con.execute('UPDATE CfCounter SET ? = ? + 3 WHERE KEY = ?',
                ['a', 'a', 'test'],
                function(err1, res0) {
                  if (err1) {
                    con.close();
                    assert.ifError(err1);
                    test.finish();
                  } else {
                    con.execute('SELECT ? FROM CfCounter WHERE KEY = ?',
                                ['a', 'test'],
                                function(err2, res1) {
                                  if (err2) {
                                    con.close();
                                    assert.ifError(err2);
                                    test.finish();
                                  } else {
                                    con.close();
                                    assert.strictEqual(res1[0].colHash['a'].toString(), '3');
                                    test.finish();
                                  }
                                });
                  }
                });
  });
};

exports.testUpdateWithNull = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ifError(err);
      test.finish();
    } else {
      var key = stringToHex('key0');
      con.execute('update Standard1 set ?=?, ?=? where key=?', ['cola', null, 'colb', 'valueb', key], function(updateErr) {
        con.close();
        assert.ok(updateErr);
        test.finish();
      });
    }
  });
};

exports.testSimpleDelete = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      var key = stringToHex('key2');
      con.execute('update Standard1 set ?=?, ?=? where key=?', ['colx', 'xxx', 'colz', 'bbb', key], function(updateErr) {
        if (updateErr) {
          con.close();

        } else {
          con.execute('delete ?,? from Standard1 where key in (?)', ['colx', 'colz', key], function(delErr) {
            if (delErr) {
              con.close();
              assert.ok(false);
              test.finish();
            } else {
              con.execute('select ?,? from Standard1 where key=?', ['colx', 'colz', key], function(selErr, rows) {
                con.close();
                if (selErr) {
                  assert.ok(false);
                } else {
                  assert.strictEqual(rows.rowCount(), 1);
                  var row = rows[0];
                  assert.strictEqual(0, row.colCount());
                }
                test.finish();
              });
            }
          });
        }
      });
    }
  });
};

exports.testLongNoBigint = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      assert.ok(true);
      assert.strictEqual(con.connectionInfo.use_bigints, true);
      con.connectionInfo.use_bigints = false;
      assert.strictEqual(con.connectionInfo.use_bigints, false);

      var updParms = [1,2,99];
      con.execute('update CfLong set ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ? from CfLong where key=?', [1, 99], function(selErr, rows) {
            con.close();
            assert.strictEqual(rows.rowCount(), 1);
            var row = rows[0];
            assert.strictEqual(1, row.colCount());
            assert.strictEqual(1, row.cols[0].name);
            assert.strictEqual(2, row.cols[0].value);
            test.finish();
          });
        }
      });
    }
  });
};

exports.testIntNoBigint = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      assert.ok(true);
      assert.strictEqual(con.connectionInfo.use_bigints, true);
      con.connectionInfo.use_bigints = false;
      assert.strictEqual(con.connectionInfo.use_bigints, false);

      var updParms = [1,2,99];
      con.execute('update CfInt set ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ? from CfInt where key=?', [1, 99], function(selErr, rows) {
            con.close();
            assert.strictEqual(rows.rowCount(), 1);
            var row = rows[0];
            assert.strictEqual(1, row.colCount());
            assert.strictEqual(1, row.cols[0].name);
            assert.strictEqual(2, row.cols[0].value);
            test.finish();
          });
        }
      });
    }
  });
};

exports.testBinary = function(test, assert) {
  connect(function(err, con) {
    assert.ifError(err);
    var key = 'binarytest';
    var binaryParams = [util.randomBuffer(), util.randomBuffer(), util.randomBuffer()];
    con.execute('update CfBytes set ?=? where key=?', binaryParams, function(updErr) {
      if (updErr) {
        con.close();
        assert.ok(false);
        test.finish();
      } else {
        con.execute('select ? from CfBytes where key=?', [binaryParams[0], binaryParams[2]], function(selErr, rows) {
          con.close();
          assert.strictEqual(rows.rowCount(), 1);
          var row = rows[0];
          assert.strictEqual(row.key.toString('base64'), binaryParams[2].toString('base64'));
          assert.strictEqual(row.cols[0].name.toString('base64'), binaryParams[0].toString('base64'));
          assert.strictEqual(row.cols[0].value.toString('base64'), binaryParams[1].toString('base64'));
          test.finish();
        });
      }
    });
  });
};

exports.testLong = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      // the third pair is ±2^62, which overflows the 53 bits in the fp mantissa js uses for numbers (should lose precision
      // coming back), but still fits nicely in an 8-byte long (it should work).
      // notice how updParams will take either a string or BigInteger
      var key = 123456;
      var updParms = [1, 2, 3, 4, '4611686018427387904', new BigInteger('-4611686018427387904'), key];
      var selParms = [1, 3, new BigInteger('4611686018427387904'), key];
      con.execute('update CfLong set ?=?,?=?,?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?,?,? from CfLong where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(3, row.colCount());

              assert.ok(new BigInteger('1').equals(row.cols[0].name));
              assert.ok(new BigInteger('2').equals(row.cols[0].value));
              assert.ok(new BigInteger('3').equals(row.cols[1].name));
              assert.ok(new BigInteger('4').equals(row.cols[1].value));
              assert.ok(new BigInteger('4611686018427387904').equals(row.cols[2].name));
              assert.ok(new BigInteger('-4611686018427387904').equals(row.cols[2].value));

              assert.ok(new BigInteger('2').equals(row.colHash['1']));
              assert.ok(new BigInteger('4').equals(row.colHash['3']));
              assert.ok(new BigInteger('-4611686018427387904').equals(row.colHash['4611686018427387904']));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testSlice = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?..? from CfLong where key=12345', [-2, 2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(5, row.colCount());
              assert.ok(row.cols[1].name.equals(new BigInteger('-1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('-11')));
              assert.ok(row.cols[3].name.equals(new BigInteger('1')));
              assert.ok(row.cols[3].value.equals(new BigInteger('11')));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testReverseSlice = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(5, row.colCount());
              assert.ok(row.cols[3].name.equals(new BigInteger('-1')));
              assert.ok(row.cols[3].value.equals(new BigInteger('-11')));
              assert.ok(row.cols[1].name.equals(new BigInteger('1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('11')));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testReversedSliceLimit = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select first 3 REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(3, row.colCount());
              assert.ok(row.cols[1].name.equals(new BigInteger('1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('11')));
              assert.ok(row.cols[2].name.equals(new BigInteger('0')));
              assert.ok(row.cols[2].value.equals(new BigInteger('0')));
              assert.equal(row.cols[2].name, 0);
              assert.equal(row.cols[2].value, 0);
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testReversedSlice = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      con.execute('update CfLong set -5=-55, -4=-44, -3=-33, -2=-22, -1=-11, 0=0, 1=11, 2=22, 3=33, 4=44, 5=55 where key=12345', [], function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select REVERSED ?..? from CfLong where key=12345', [2, -2], function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(5, row.colCount());
              assert.ok(row.cols[3].name.equals(new BigInteger('-1')));
              assert.ok(row.cols[3].value.equals(new BigInteger('-11')));
              assert.ok(row.cols[1].name.equals(new BigInteger('1')));
              assert.ok(row.cols[1].value.equals(new BigInteger('11')));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testInt = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      // make sure to use some numbers that will overflow a 64 bit signed value.
      var updParms = [1, 11, -1, -11, '8776496549718567867543025521', '-8776496549718567867543025521', '3456543434345654345332453455633'];
      var selParms = [-1, 1, '8776496549718567867543025521', '3456543434345654345332453455633'];
      con.execute('update CfInt set ?=?, ?=?, ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?, ?, ? from CfInt where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              var row = rows[0];
              assert.strictEqual(rows.rowCount(), 1);
              assert.strictEqual(3, row.colCount());

              assert.ok(new BigInteger('-1').equals(row.cols[0].name));
              assert.ok(new BigInteger('-11').equals(row.cols[0].value));
              assert.ok(new BigInteger('1').equals(row.cols[1].name));
              assert.ok(new BigInteger('11').equals(row.cols[1].value));
              assert.ok(new BigInteger('8776496549718567867543025521').equals(row.cols[2].name));
              assert.ok(new BigInteger('-8776496549718567867543025521').equals(row.cols[2].value));

              assert.ok(new BigInteger('11').equals(row.colHash['1']));
              assert.ok(new BigInteger('-11').equals(row.colHash['-1']));
              assert.ok(new BigInteger('-8776496549718567867543025521').equals(row.colHash['8776496549718567867543025521']));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testUUID = function(test, assert) {
  // make sure we're not comparing the same things.
  assert.ok(!UUID.fromString('6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(UUID.fromString('6fd589e0-65e0-11e0-0000-7fd66bb03aff')));
  assert.ok(!UUID.fromString('6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(UUID.fromString('fa6a8870-65fa-11e0-0000-fe8ebeead9fd')));
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      // again, demonstrate that we can use strings or objectifications.
      var updParms = ['6f8483b0-65e0-11e0-0000-fe8ebeead9fe', '6fd45160-65e0-11e0-0000-fe8ebeead9fe', '6fd589e0-65e0-11e0-0000-7fd66bb03aff', '6fd6e970-65e0-11e0-0000-fe8ebeead9fe', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd'];
      var selParms = ['6f8483b0-65e0-11e0-0000-fe8ebeead9fe', '6fd589e0-65e0-11e0-0000-7fd66bb03aff', 'fa6a8870-65fa-11e0-0000-fe8ebeead9fd'];
      con.execute('update CfUuid set ?=?, ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select ?, ? from CfUuid where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(2, row.colCount());

              assert.ok(UUID.fromString('6f8483b0-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[0].name));
              assert.ok(UUID.fromString('6fd45160-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[0].value));
              assert.ok(UUID.fromString('6fd589e0-65e0-11e0-0000-7fd66bb03aff').equals(row.cols[1].name));
              assert.ok(UUID.fromString('6fd6e970-65e0-11e0-0000-fe8ebeead9fe').equals(row.cols[1].value));

              assert.ok(row.colHash[(UUID.fromString('6fd589e0-65e0-11e0-0000-7fd66bb03aff'))].equals(row.cols[1].value));
              assert.ok(row.colHash[(row.cols[0].name)].equals(row.cols[0].value));
              assert.ok(row.colHash[(UUID.fromString('6f8483b0-65e0-11e0-0000-fe8ebeead9fe'))].equals(row.cols[0].value));
              assert.ok(row.colHash[(row.cols[1].name)].equals(row.cols[1].value));
            }
            test.finish();
          });
        }
      });
    }
  });
};

exports.testCustomValidators = function(test, assert) {
  connect(function(err, con) {
    if (err) {
      assert.ok(false);
      test.finish();
    } else {
      var updParms = ['normal', 25, 'int_col', 21, 'string_col', 'test_string_value', 'uuid_col', '6f8483b0-65e0-11e0-0000-fe8ebeead9fe', 211];
      var selParms = ['normal', 'int_col', 'string_col', 'uuid_col', 211];
      con.execute('update CfUgly set ?=?, ?=?, ?=?, ?=? where key=?', updParms, function(updErr) {
        if (updErr) {
          con.close();
          assert.ok(false);
          test.finish();
        } else {
          con.execute('select  ?, ?, ?, ? from CfUgly where key=?', selParms, function(selErr, rows) {
            con.close();
            if (selErr) {
              assert.ok(false);
            } else {
              assert.strictEqual(rows.rowCount(), 1);
              var row = rows[0];
              assert.strictEqual(4, row.colCount());

              assert.ok(row.colHash.normal.equals(new BigInteger('25')));
              assert.ok(row.colHash.int_col.equals(new BigInteger('21')));
              assert.ok(row.colHash.string_col.toString() === 'test_string_value');
              assert.ok(row.colHash.uuid_col.toString() == '6f8483b0-65e0-11e0-0000-fe8ebeead9fe');
            }
            test.finish();
          });
        }
      });
    }
  });
};

// this test only works an order-preserving partitioner.
// it also uses an event-based approach to doing things.
//exports.ZDISABLED_testMultipleRows = function(test, assert) {
//  // go through the motions of creating a new keyspace every time. we do this to ensure only the things in there are
//  // what I expect.
//
//  var sys = new Connection('127.0.0.1', CASSANDRA_PORT, 'system', null, null, {use_bigints: true});
//  sys.connect(function(err) {
//    if (err) {
//      assert.ok(false)
//      test.finish();
//    } else {
//      var ev = new EventEmitter();
//      // attempt to drop the keyspace on error.
//      ev.on('syserr', function() {
//        console.log('syserr');
//        sys.execute('drop keyspace ints', function(err) {});
//        sys.close();
//        assert.ok(false);
//        test.finish();
//      });
//
//      // keyspace is there for sure. don't know about the cf.
//      ev.on('ksready', function() {
//        console.log('keyspace created');
//        sys.close();
//        var con = new Connection('127.0.0.1', CASSANDRA_PORT, 'ints', null, null, {use_bigints: true});
//        con.execute('create columnfamily cfints (key int primary key) with comparator=int and default_validation=int', null, function(err) {
//          con.close();
//          if (err) {
//            ev.emit('syserr');
//          } else {
//            ev.emit('cfready');
//          }
//        });
//        con.close();
//      });
//
//      // column family is ready, do the test.
//      ev.on('cfready', function() {
//
//        // insert 100 rows.
//        var con = new Connection('127.0.0.1', 19170, 'ints', null, null, {use_bigints: true});
//        var count = 100;
//        var num = 0;
//        for (var i = 0; i < count; i++) {
//          con.execute('update cfints set ?=? where key=?', [1, i, i], function(err) {
//            if (err) {
//              con.close();
//              ev.emit('syserr');
//            } else {
//              num += 1;
//
//              // after all the rows are in, do a query.
//              if (num >= count) {
//                con.execute('select ? from cfints where key > ? and key < ?', [1, 10, 20], function(serr, rows) {
//                  con.close();
//                  assert.strictEqual(rows.rowCount(), 11);
//                });
//              }
//            }
//          });
//        }
//      });
//
//      // start everything off.
//      sys.execute('drop keyspace ints', null, function(dropErr) {
//        if (!dropErr) {
//          console.log('keyspace dropped');
//        }
//        sys.execute('create keyspace ints with strategy_class=SimpleStrategy and strategy_options:replication_factor=1', null, function(createKsErr) {
//          if (createKsErr) {
//            ev.emit('syserr');
//          } else {
//            ev.emit('ksready');
//          }
//        });
//      });
//    }
//  });
//};


exports.testPooledConnectionFailover = function(test, assert) {
  var hosts = ['google.com:8000', '127.0.0.1:6567', '127.0.0.1:19170', '127.0.0.2'];
  var conn = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1', use_bigints: true, 'timeout': 5000});

  async.series([
    function executeQueries(callback) {
      conn.execute('UPDATE CfUgly SET A=1 WHERE KEY=1', [], function(err) {
        assert.ifError(err);
        callback();
      });
    }
  ],

  function(err) {
    conn.shutdown();
    test.finish();
  });
};

exports.testLearnStepTimeout = function(test, assert) {
  var server = null;
  var hosts = ['127.0.0.1:8688', '127.0.0.1:19170'];
  var conn = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1', use_bigints: true, 'timeout': 5000});

  async.series([
    function startHttpServer(callback) {
      server = http.createServer(function (req, res) {
        res.end('test\n');
      });
      server.listen(8688, '127.0.0.1', callback);
    },

      function executeQueryPooledConnection(callback) {
        conn.execute('UPDATE CfUgly SET A=1 WHERE KEY=1', [], function(err) {
        assert.ifError(err);
        callback();
      });
    }
  ],

  function(err) {
    if (server) {
      server.close();
    }

    conn.shutdown();
    test.finish();
  });
};

exports.testPooledConnection = function(test, assert) {
  function bail(conn, err) {
    conn.shutdown();
    assert.ifError(err);
    test.finish();
  }

  //var hosts = ["127.0.0.2:9170", "127.0.0.1:9170"];
  var hosts = ["127.0.0.1:19170"];
  var conn = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1', use_bigints: true});

  var range = new Array(100).join(' ').split(' ');

  // Hammer time...
  conn.execute('UPDATE CfUgly SET A=1 WHERE KEY=1', [], function(err) {
    if (err) { bail(conn, err); }

    async.forEach(range, function (_, callback) {
      conn.execute('SELECT A FROM CfUgly WHERE KEY=1', [], function(err, rows) {
        if (err) { bail(conn, err); }
        assert.strictEqual(rows.rowCount(), 1);
        var row = rows[0];
        assert.strictEqual(row.cols[0].name.toString(), 'A');
        callback();
      });
    },

    function(err) {
      conn.shutdown();
      test.finish();
    });
  });
};

exports.testTimeLogging = function(test, assert) {
  var hosts = ["127.0.0.1:19170"];
  var baseOptions = {'hosts': hosts, 'keyspace': 'Keyspace1', use_bigints: true};
  var options1 = merge(baseOptions, {});
  var options2 = merge(baseOptions, {'log_time': true});
  var conn1 = new PooledConnection(options1);
  var conn2 = new PooledConnection(options2);

  var logObjsCql = [];
  var logObjsTime = [];

  var appendLog = function(level, message) {
    if (level === 'cql') {
      logObjsCql.push(message);
    }
    if (level === 'timing') {
      logObjsTime.push(message);
    }
  };
  conn1.on('log', appendLog);
  conn2.on('log', appendLog);

  conn1.execute('UPDATE CfUgly SET A=1 WHERE KEY=1', [], function(err) {
    var logObj;
    assert.ifError(err);

    // Timing log is disabled, logObjs should be empty.
    assert.equal(logObjsCql.length, 1);
    assert.equal(logObjsTime.length, 0);

    logObj = logObjsCql[0];
    assert.ok(logObj.hasOwnProperty('query'));
    assert.ok(logObj.hasOwnProperty('parameterized_query'));
    assert.ok(logObj.hasOwnProperty('args'));
    assert.ok(!logObj.hasOwnProperty('time'));

    conn2.execute('SELECT A FROM CfUgly WHERE KEY=1', [], function(err, rows) {
      var logObj;
      assert.ifError(err);
      assert.strictEqual(rows.rowCount(), 1);

      // Timing log is enabled, logObjs should have 1 item
      assert.equal(logObjsCql.length, 2);
      assert.equal(logObjsTime.length, 1);
      logObj = logObjsTime[0];

      assert.ok(logObj.hasOwnProperty('query'));
      assert.ok(logObj.hasOwnProperty('parameterized_query'));
      assert.ok(logObj.hasOwnProperty('args'));
      assert.ok(logObj.hasOwnProperty('time'));

      conn1.shutdown(conn2.shutdown(test.finish));
    });
  });
};


exports.testConnectionInPool = function(test, assert) {
  var con = new ConnectionInPool({
    host: '127.0.0.1',
    port: 19170,
    keyspace: 'Keyspace1',
    use_bigints: true
  });
  con.connect(function(err) {
    if (err) {
      assert.ifError(err);
    } else {
      con.close();
      test.finish();
    }
  });
};


exports.testPooledConnectionLoad = function(test, assert) {
  var hosts = ['127.0.0.1:19170'];
  var conn = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1', 'timeout': 10000});

  var count = 3000;

  async.waterfall([
    // establish connections prior to executing statements.
    //conn.connect.bind(conn),
    function(cb) {
      conn.execute('TRUNCATE CfUtf8', [], cb);
    },
    function(res, cb) {
      var executes = [];
      for (var i = 0; i < count; i++) {
        (function(index) {
          executes.push(function(parallelCb) {
            conn.execute('UPDATE CfUtf8 SET ? = ? WHERE KEY = ?', [
              'testCol',
              'testVal',
              'testKey'+index
            ], parallelCb);
          });
        })(i);
      }
      async.parallel(executes, function(err) {
        assert.ifError(err);
        cb();
      });
    },
    function(cb) {
      conn.execute('SELECT COUNT(*) FROM CfUtf8', [], cb);
    },
    function(res, cb) {
      assert.equal(res[0].colHash.count, count);
      cb();
    },
    function(cb) {
      conn.execute('TRUNCATE CfUtf8', [], cb);
    },
    function(res, cb) {
      conn.shutdown(cb);
    }
  ],
  function(err) {
    assert.ifError(err);
    test.finish();
  });
};


// We want to test if all executes of a pooled connection are finished before
// the shutdown callback is called.
exports.testPooledConnectionShutdown = function(test, assert) {
  var hosts = ['127.0.0.1:19170'];
  var conn = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1'});

  var expected = 100;
  var cbcount = 0;
  var spy = function(err, res) {
    assert.ifError(err);
    cbcount++;
  };

  for (var i = 0; i < expected; i++) {
    (function(index) {
      conn.execute('UPDATE CfUtf8 SET ? = ? WHERE KEY = ?', ['col', 'val', 'key'+index], spy);
    })(i);
  }
  conn.shutdown(function(err) {
    assert.ifError(err);
    assert.equal(cbcount, expected);
    test.finish();
  });
};

exports.testPooledConnectionShutdownTwice = function(test, assert) {
  var hosts = ['127.0.0.1:19170'];
  var conn = new PooledConnection({'hosts': hosts, 'keyspace': 'Keyspace1'});

  var expected = 100;
  var cbcount = 0;
  var spy = function(err, res) {
    assert.ifError(err);
    cbcount++;
  };

  for (var i = 0; i < expected; i++) {
    (function(index) {
      conn.execute('UPDATE CfUtf8 SET ? = ? WHERE KEY = ?', ['col', 'val', 'key'+index], spy);
    })(i);
  }

  assert.ok(!conn.shuttingDown);
  conn.shutdown(function(err) {
    assert.ifError(err);
    assert.equal(cbcount, expected);
    assert.ok(secondCbCalledImmediatelyWithError);
    test.finish();
  });

  // Make sure second callback gets called immediately with an error
  var secondCbCalledImmediatelyWithError = false;
  assert.ok(conn.shuttingDown);
  conn.shutdown(function(err) {
    assert.ok(err);
    secondCbCalledImmediatelyWithError = true;
  });
};
