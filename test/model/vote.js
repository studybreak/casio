var Casio = require('../../').Casio;


var conn_options = {
  hosts:['127.0.0.1:9160'],
  keyspace:'casio',
  use_bigints: true
}

var options = {
    consistency:{
        select:'ONE',
        insert:'ONE',
        update:'ONE',
        delete:'ONE'
    }
}

var casio = new Casio(conn_options);

if (process.env.NODE_ENV && process.env.NODE_ENV==='debug'){
  casio.on('log', function (level, msg, details) {
    console.log(level, msg);
  });
}

var Vote = casio.model('Vote', options);
Vote.property('key', String, {
    primary:true
});
Vote.property('up', Casio.types.BigInteger, {});
Vote.property('down', Casio.types.BigInteger, {});

//////////////////////////////////////
exports.Vote = Vote
//////////////////////////////////////
