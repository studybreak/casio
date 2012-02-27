var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var User = require('./user').User;

var options = {

    host:'127.0.0.1', 
    port:9160, 
    keyspace:'casio',
    use_bigints: true,
    consistency:{
        select:'ONE',
        insert:'ONE',
        update:'ONE',
        delete:'ONE'
    }
}

var Pet = Casio.model('Pet', options);
Pet.connect();

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