var Casio = require('../../').Casio;
var CQL = require('../../').CQL;

var options = {

    hosts:['127.0.0.1:9160'],
    keyspace:'casio',
    use_bigints: true,
    consistency:{
        select:'ONE',
        insert:'ONE',
        update:'ONE',
        delete:'ONE'
    },
    keyAlias: 'personId'
}

var Person = (new Casio(options)).model('Person', options);


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
