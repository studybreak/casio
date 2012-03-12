var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var conn_options = {
    hosts:['127.0.0.1:9160'],
    keyspace:'casio'
}
var casio = new Casio(conn_options);

var Friends = casio.array('Friends');

if (process.env.NODE_ENV && process.env.NODE_ENV==='debug'){
  casio.on('log', function (level, msg, details) {
    console.log(level, msg);
  });
}
// primary defaults to 'key' if not defined

exports.Friends = Friends;
