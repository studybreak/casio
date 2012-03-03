var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var options = {
    hosts:['127.0.0.1:9160'],
    keyspace:'casio',
}

var Friends = (new Casio(options)).array('Friends', options);

// primary defaults to 'key' if not defined

exports.Friends = Friends;
