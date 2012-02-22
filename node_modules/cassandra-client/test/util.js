
// min is inclusive, max is exclusive.
function randomInt(min, max) {
  if (min === undefined) {
    min = -2147483648;
  }
  if (max === undefined) {
    max = 2147483647
  }
  return Math.round(Math.random() * (max - min) + min);
}

function randomBuffer(sz) {
  sz = sz || randomInt(10, 100);
  var buf = new Buffer(sz);
  for (var i = 0; i < sz; i++) {
    buf[i] = randomInt(0, 255);
  }
  return buf;
}

exports.randomInt = randomInt;
exports.randomBuffer = randomBuffer;