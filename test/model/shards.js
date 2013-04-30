var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var conn_options = {
    hosts:['127.0.0.1:9160'],
    keyspace:'casio'
}
var casio = new Casio(conn_options);

if (process.env.NODE_ENV && process.env.NODE_ENV==='debug'){
  casio.on('log', function (level, msg, details) {
    console.log(level, msg);
  });
}


var Shards = casio.array('Shards', {
  shards: ['', '1', '2', '3', '4', '5', '6', '7']
});

// primary key
Shards.primary('userId', String);

Shards.classMethods({

});

Shards.instanceMethods({

});


exports.Shards = Shards;
