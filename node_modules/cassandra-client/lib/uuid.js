/*
 *  Copyright 2011 Christoph Tavan
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

/**
 * @fileoverview UUID type for cassandra
 */

var uuid = require('node-uuid');

/**
 * UUID object for use in node-cassandra-client
 */
function UUID(bytes) {
  this._bytes = (Buffer.isBuffer(bytes) && bytes.length == 16) ?
    bytes :
    (Array.isArray(bytes) && bytes.length == 16 ?
      new Buffer(bytes) :
      new Buffer(16)
    );
  if (!bytes) {
    uuid.v4(null, this._bytes);
  }
}
module.exports = UUID;

/**
 * Converts a UUID to string hyphen-separated string-notations
 *
 * @return {string} UUID in hyphen-separated string-notation
 */
UUID.prototype.toString = function() {
  return uuid.unparse(this._bytes);
};

/**
 * Compare a given UUID-object with the current UUID object
 *
 * @param {object} other UUID object to compare with
 * @return {bool}
 */
UUID.prototype.equals = function(other) {
  return this.toString() === other.toString();
};

/**
 * Factory method that returns a UUID object.
 *
 * @param {Buffer} bytes The 16 bytes of a UUID
 * @return {object} UUID
 */
UUID.fromBytes = function(bytes) {
  return new UUID(bytes);
};

/**
 * Factory method that returns a UUID object.
 *
 * @param {string} string UUID in hyphen-separated string-notation
 * @return {object} UUID
 */
UUID.fromString = function(string) {
  return new UUID(uuid.parse(string));
};

/**
 * Factory method that generates a v1 UUID for a given timestamp.
 *
 * Successive calls using the same timestamp will generate incrementally
 * different UUIDs that sort appropriately.
 *
 * @param {int} timestamp Unix-timestamp
 * @return {object} UUID
 */
UUID.fromTime = function(timestamp) {
  var buf = new Buffer(16);
  uuid.v1({ msecs: timestamp }, buf);
  return new UUID(buf);
};

/**
 * Factory method generates the minimum (first) UUID for a given ms-timestamp
 *
 * @param {int} timestamp Unix-timestamp
 * @return {object} UUID
 */
UUID.minUUID = function(timestamp) {
  var buf = new Buffer(16);
  uuid.v1({ msecs: timestamp, nsecs: 0 }, buf);
  return new UUID(buf);
};

/**
 * Opposite of minUUID.
 *
 * @param {int} timestamp Unix-timestamp
 * @return {object} UUID
 */
UUID.maxUUID = function(timestamp) {
  var buf = new Buffer(16);
  uuid.v1({ msecs: timestamp, nsecs: 9999 }, buf);
  return new UUID(buf);
};

