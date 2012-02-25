var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var options = {
    host:'127.0.0.1', 
    port:9160, 
    keyspace:'casio',
}

var Friends = Casio.array('Friends', options);

Friends.connect();

exports.Friends = Friends;