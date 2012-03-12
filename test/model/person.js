var Casio = require('../../').Casio;
var CQL = require('../../').CQL;


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
    },
    keyAlias: 'personId'
}

var casio = new Casio(conn_options);

if (process.env.NODE_ENV && process.env.NODE_ENV==='debug'){
  casio.on('log', function (level, msg, details) {
    console.log(level, msg);
  });
}
var Person = casio.model('Person', options);

Person.property('personId', String, {
    primary:true
});

Person.property('address1', String, {});
Person.property('address2', String, {});
Person.property('city', String, {});
Person.property('state', String, {});
Person.property('zipcode', String, {});

// Person.belongsTo('user', User, {
//     on:'userId'
// });

Person.property('created_at', Date, {});
Person.property('updated_at', Date, {});

//////////////////////////////////////
exports.Person = Person
//////////////////////////////////////
