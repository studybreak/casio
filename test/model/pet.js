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
    keyAlias: 'petId'
}

var casio = new Casio(conn_options);

if (process.env.NODE_ENV && process.env.NODE_ENV==='debug'){
  casio.on('log', function (level, msg, details) {
    console.log(level, msg);
  });
}

var Pet = casio.model('Pet', options);

Pet.property('petId', String, {
    primary:true
});
Pet.property('userId', String, {});
Pet.property('name', String, {});
Pet.property('created_at', Date, {});
Pet.property('updated_at', Date, {});

// this needs to be defined on the package definition
// to assure the User object loads first...
// Pet.hasOne('owner', User, {
//     // fk:'userId',
//     // on:'userId'
// });


//////////////////////////////////////
exports.Pet = Pet
//////////////////////////////////////
