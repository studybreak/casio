var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var options = {
    hosts:['127.0.0.1:9160'],
    keyspace:'casio',
}

var Groups = (new Casio(options)).array('Groups', options);

Groups.primary('groupsId');
//////////////////////////////////////
exports.Groups = Groups;
//////////////////////////////////////
