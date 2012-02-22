// some tests for the uuid-js module.

var UUID = require('../lib/uuid');

var UUID_FORMAT = {
  v1: /[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i,
  v4: /[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i
};

exports['test_uuid_random_v4'] = function(test, assert) {
  var uuid = new UUID();
  assert.ok(uuid.toString().match(UUID_FORMAT.v4));
  test.finish();
};
exports['test_uuid_from_time_v1'] = function(test, assert) {
  var ts = 1314735336312;
  var uuid = UUID.fromTime(ts);
  assert.ok(uuid.toString().match(UUID_FORMAT.v1));
  ts += 1;
  uuid = UUID.minUUID(ts);
  assert.ok(uuid.toString().match(UUID_FORMAT.v1));
  uuid = UUID.maxUUID(ts);
  assert.ok(uuid.toString().match(UUID_FORMAT.v1));
  test.finish();
};

exports['test_uuid_from_buffer'] = function(test, assert) {
  var buf = new Buffer('\u00ee\u00a1\u006c\u00c0\u00cf\u00bd\u0011\u00e0\u0017' +
          '\u000a\u00dd\u0026\u0075\u0027\u009e\u0008', 'binary');
  var uuid = UUID.fromBytes(buf);
  assert.strictEqual(uuid.toString(), 'eea16cc0-cfbd-11e0-170a-dd2675279e08');
  test.finish();
};

// As per RFC 4122 regressions in time should result in the clockseq being
// incremented to ensure uniqueness of v1 UUIDs
exports['test_uuid_backwards_in_time'] = function(test, assert) {
  var ts = 1314735336316;
  var ns = 0;
  var uuidTs = UUID.fromTime(ts).toString();
  // This is like setting the system-time to a future value:
  var uuidFuture = UUID.fromTime(ts + 5000).toString();
  // Now this is like setting the system-clock back in time which must result
  // in the clock_seq field of the UUID being incremented to avoid collisions.
  var uuidTsSame = UUID.fromTime(ts).toString();

  // UUIDs generated from same TS after going back in time must differ
  // since clockseq must have been updated
  assert.ok(uuidTs !== uuidTsSame);
  assert.ok(uuidTs !== uuidFuture); // duh
  assert.strictEqual(uuidTs.split('-')[0], uuidTsSame.split('-')[0]);
  assert.strictEqual(uuidTs.split('-')[1], uuidTsSame.split('-')[1]);
  assert.strictEqual(uuidTs.split('-')[2], uuidTsSame.split('-')[2]);
  test.finish();
};

// Generating multiple successive UUIDs for the same millisecond should be
// supported by the uuid library for highly concurrent applications. Note that
// v1 UUIDs are based on 100ns intervals while javascript only offers
// millisecond resolution times, so the UUID lib should have an internal
// "100nanosecond-counter" to allow generating 10k v1 UUIDs/millisecond.
exports['test_uuid_same_ms'] = function(test, assert) {
  var ts = 1314735336316;
  var uuidTs = UUID.fromTime(ts).toString();
  var uuidTsSame = UUID.fromTime(ts).toString();
  // UUIDs generated for the same millisecond must differ
  assert.ok(uuidTs !== uuidTsSame);
  // time low should differ by a 100ns tick.
  assert.strictEqual(parseInt(uuidTsSame.split('-')[0], 16) - parseInt(uuidTs.split('-')[0], 16), 1);
  // but time mid and hi should definitely be the same.
  assert.strictEqual(uuidTs.split('-')[1], uuidTsSame.split('-')[1]);
  assert.strictEqual(uuidTs.split('-')[2], uuidTsSame.split('-')[2]);
  test.finish();
};

// v1 time-UUIDs are generated with 100ns resolution and cassandra compares all
// 60 time bits when comparing time-UUIDs. So for range-queries we always need
// UUIDs that have the 100ns count set to 0 for the beginning of a range
// and set to 9999 for the end of a range to miss no values.
exports['test_uuid_same_ms_min_max'] = function(test, assert) {
  var ts = 1314735336320;
  var uuidTs = UUID.minUUID(ts).toString();
  var uuidTsSame = UUID.maxUUID(ts).toString();
  // UUIDs generated for the same millisecond must differ
  assert.ok(uuidTs !== uuidTsSame);
  // time low should differ by the full range of 9999 100ns tick.
  assert.strictEqual(parseInt(uuidTsSame.split('-')[0], 16) - parseInt(uuidTs.split('-')[0], 16), 9999);
  // but time mid and hi should definitely be the same.
  assert.strictEqual(uuidTs.split('-')[1], uuidTsSame.split('-')[1]);
  assert.strictEqual(uuidTs.split('-')[2], uuidTsSame.split('-')[2]);
  test.finish();
};
exports['test_uuid_same_ms_min_nextmin'] = function(test, assert) {
  var ts = 1314735336320;
  var uuidTs = UUID.minUUID(ts).toString();
  var uuidTsNext = UUID.minUUID(ts+1).toString();
  // UUIDs generated for the same millisecond must differ
  assert.ok(uuidTs !== uuidTsNext);
  // time low should differ by the full range of 9999 100ns tick.
  assert.strictEqual(parseInt(uuidTsNext.split('-')[0], 16) - parseInt(uuidTs.split('-')[0], 16), 10000);
  // but time mid and hi should definitely be the same.
  assert.strictEqual(uuidTs.split('-')[1], uuidTsNext.split('-')[1]);
  assert.strictEqual(uuidTs.split('-')[2], uuidTsNext.split('-')[2]);
  test.finish();
};
