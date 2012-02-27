var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var options = {
    host:'127.0.0.1', 
    port:9160, 
    keyspace:'casio',
}

var Groups = Casio.array('Groups', options);

Groups.connect();
Groups.primary('groupsId');
//////////////////////////////////////
exports.Groups = Groups;
//////////////////////////////////////