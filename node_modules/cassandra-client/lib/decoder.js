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
/** [en|de]coder for cassandra types. */
var BigInteger = require('./bigint').BigInteger;
var UUID = require('./uuid');

var v6Buffers = require('buffer').Buffer.prototype.readFloatBE ? true : false;

// remember: values x such that -2^31 > x or x > 2^31-1 will make this routine puke.
var bytesToNum = module.exports.bytesToNum = function(bytes) {
  var num = 0;
  // if the sign bit is on, start wtih every bit asserted.  we only care about 32 bits because we lose precision after
  // that anyway.
  if ((0x0080 & bytes[0]) === 0x0080) {
    num = 0xffffffff;
  }
  for (var i = 0; i < bytes.length; i++) {
    num <<= 8;
    num |= bytes[i];
  }
  return num;
};

var bytesToBigInt = module.exports.bytesToBigInt = function(bytes) {
  // convert bytes (which is really a string) to a list of ints. then convert the bytes (ints) to a big integer.
  var ints = [];
  for (var i = 0; i < bytes.length; i++) {
    ints[i] = bytes[i]; // how does this handle negative values (bytes that overflow 127)?
    if (ints[i] > 255) {
      throw new Error('Invalid character in packed string ' + ints[i]);
    }
  }
  return new BigInteger(ints);
};

/** convert an 8 byte string to a BigInteger */
var bytesToBigLong = module.exports.bytesToBigLong = function(bytes) {
  /*if (bytes.length != 8) {
    //throw new Error('Longs are exactly 8 bytes, not ' + bytes.length);
  }*/

  // trim all leading zeros except the most significant one (we don't want to flip signs)
  while (bytes[0] === 0 && bytes[1] === 0) {
    bytes = bytes.slice(1);
  }

  // zero is a tricky bastard. new BigInteger([0]) != new BigInteger('0'). wtf?
  if (bytes.length === 1 && bytes[0] === 0) {
    return new BigInteger('0');
  }
  return bytesToBigInt(bytes);
};

// Cassandra datatypes according to
// http://www.datastax.com/docs/1.0/ddl/column_family
// Those commented out are not correctly dealt with yet and will appear as
// Buffer's in resultsets.
var AbstractTypes = {
  BytesType: 'org.apache.cassandra.db.marshal.BytesType',
  AsciiType: 'org.apache.cassandra.db.marshal.AsciiType',
  UTF8Type: 'org.apache.cassandra.db.marshal.UTF8Type',
  IntegerType: 'org.apache.cassandra.db.marshal.IntegerType',
  LongType: 'org.apache.cassandra.db.marshal.LongType',
  Int32Type: 'org.apache.cassandra.db.marshal.Int32Type',
  UUIDType: 'org.apache.cassandra.db.marshal.UUIDType',
  LexicalUUIDType: 'org.apache.cassandra.db.marshal.LexicalUUIDType',
  TimeUUIDType: 'org.apache.cassandra.db.marshal.TimeUUIDType',
  DateType: 'org.apache.cassandra.db.marshal.DateType',
  //BooleanType: 'org.apache.cassandra.db.marshal.BooleanType',
  FloatType: 'org.apache.cassandra.db.marshal.FloatType',
  //DoubleType: 'org.apache.cassandra.db.marshal.DoubleType',
  //DecimalType: 'org.apache.cassandra.db.marshal.DecimalType',
  CounterColumnType: 'org.apache.cassandra.db.marshal.CounterColumnType',
  //CompositeType: 'org.apache.cassandra.db.marshal.CompositeType',
  //DynamicCompositeType: 'org.apache.cassandra.db.marshal.DynamicCompositeType',
};

// source: https://github.com/joyent/node/blob/master/lib/buffer_ieee754.js
// license: LICENSE_buffer_ieee754.txt
function readIEEE754(buffer, offset, isBE, mLen, nBytes) {
  var e, m,
      eLen = nBytes * 8 - mLen - 1,
      eMax = (1 << eLen) - 1,
      eBias = eMax >> 1,
      nBits = -7,
      i = isBE ? 0 : (nBytes - 1),
      d = isBE ? 1 : -1,
      s = buffer[offset + i];

  i += d;

  e = s & ((1 << (-nBits)) - 1);
  s >>= (-nBits);
  nBits += eLen;
  for (; nBits > 0; e = e * 256 + buffer[offset + i], i += d, nBits -= 8);

  m = e & ((1 << (-nBits)) - 1);
  e >>= (-nBits);
  nBits += mLen;
  for (; nBits > 0; m = m * 256 + buffer[offset + i], i += d, nBits -= 8);

  if (e === 0) {
    e = 1 - eBias;
  } else if (e === eMax) {
    return m ? NaN : ((s ? -1 : 1) * Infinity);
  } else {
    m = m + Math.pow(2, mLen);
    e = e - eBias;
  }
  return (s ? -1 : 1) * m * Math.pow(2, e - mLen);
};

// These functions convert the raw bytes that cassandra gives us to meaningful
// JS datat ypes.
var converters = {
  'org.apache.cassandra.db.marshal.BytesType': function(bytes) {
    return bytes;
  },
  'org.apache.cassandra.db.marshal.AsciiType': function(bytes) {
    return bytes.toString('ascii');
  },
  'org.apache.cassandra.db.marshal.UTF8Type': function(bytes) {
    return bytes.toString('utf8');
  },
  'org.apache.cassandra.db.marshal.DateType': function(bytes) {
    return new Date(+bytesToBigInt(bytes).toString());
  },
  'org.apache.cassandra.db.marshal.FloatType': function(bytes) {
    // readFloatBE arrived with Node 0.5. Let's support 0.4.
    return v6Buffers ? bytes.readFloatBE(0) : readIEEE754(bytes, 0, true, 23, 4);
  },
  'org.apache.cassandra.db.marshal.Int32Type': function(bytes) {
    return bytesToNum(bytes);
  },
  'org.apache.cassandra.db.marshal.IntegerType': function(bytes, useBigints) {
    if (useBigints) {
      return bytesToBigInt(bytes);
    }
    return bytesToNum(bytes);
  },
  'org.apache.cassandra.db.marshal.LongType': function(bytes, useBigints) {
    if (useBigints) {
      return bytesToBigLong(bytes);
    }
    return bytesToNum(bytes);
  },
  'org.apache.cassandra.db.marshal.CounterColumnType': function(bytes, useBigints) {
    return converters['org.apache.cassandra.db.marshal.LongType'](bytes, useBigints);
  },
  'org.apache.cassandra.db.marshal.UUIDType': function(bytes) {
    // A uuid object. Use .toString() to stringify
    return UUID.fromBytes(bytes);
  },
  'org.apache.cassandra.db.marshal.TimeUUIDType': function(bytes) {
    return converters['org.apache.cassandra.db.marshal.UUIDType'](bytes);
  },
  'org.apache.cassandra.db.marshal.LexicalUUIDType': function(bytes) {
    return converters['org.apache.cassandra.db.marshal.UUIDType'](bytes);
  },
  'select count': function(bytes) {
    return converters['org.apache.cassandra.db.marshal.LongType'](bytes);
  }
};

/**
 * validators are a hash currently created in the Connection constructor. keys in the hash are: key, comparator,
 * defaultValidator, specificValidator.  They all map to a value in AbstractTypes, except specificValidator which
 * hashes to another map that maps specific column names to their validators (specified in ColumnDef using Cassandra
 * parlance).
 * e.g.: {key: 'org.apache.cassandra.db.marshal.BytesType',
 *        comparator: 'org.apache.cassandra.db.marshal.BytesType',
 *        defaultValidator: 'org.apache.cassandra.db.marshal.BytesType',
 *        specificValidator: {your_mother: 'org.apache.cassandra.db.marshal.BytesType',
 *                            my_mother: 'org.apache.cassandra.db.marshal.BytesType'}}
 * todo: maybe this is complicated enough that a class is required.
 */
var Decoder = module.exports.Decoder = function(validators, options) {
  this.validators = validators;
  this.options = options ? options : {};
};

/**
 * @param bytes raw bytes to decode.
 * @param which one of 'key', 'comparator', or 'value'.
 * @param column (optional) when which is 'value' this parameter specifies which column validator is to be used.
 */
Decoder.prototype.decode = function(bytes, which, column) {
  // determine which type we are converting to.
  var className = null;
  if (which == 'key') {
    className = this.validators.key;
  } else if (which == 'comparator') {
    className = this.validators.comparator;
  } else if (which == 'validator') {
    if (this.options.select_count) {
      className = 'select count';
    } else if (column && this.validators.specificValidators[column]) {
      className = this.validators.specificValidators[column];
    } else {
      className = this.validators.defaultValidator;
    }
  }
  if (!className) {
    className = AbstractTypes.BytesType;
  }

  if (className in converters) {
    return converters[className](bytes, this.options.use_bigints);
  }
  return bytes;
};
