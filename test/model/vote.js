var Casio = require('../../').Casio;

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

var Vote = Casio.model('Vote', options);

Vote.connect();

Vote.property('key', String, {
    primary:true
});
Vote.property('up', Casio.types.BigInteger, {});
Vote.property('down', Casio.types.BigInteger, {});

//////////////////////////////////////
exports.Vote = Vote
//////////////////////////////////////