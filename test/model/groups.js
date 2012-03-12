var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var conn_options = {
  hosts:['127.0.0.1:9160'],
  keyspace:'casio'  
}

var options = {
    keyAlias: 'groupsId'
}

var casio = new Casio(conn_options)

if (process.env.NODE_ENV && process.env.NODE_ENV==='debug'){
  casio.on('log', function (level, msg, details) {
    console.log(level, msg);
  });
}

var Groups = casio.array('Groups', options);

Groups.primary('groupsId');
//////////////////////////////////////
exports.Groups = Groups;
//////////////////////////////////////
