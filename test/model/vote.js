var Casio = require('../../').Casio;

var options = {

    hosts:['127.0.0.1:9160'],
    keyspace:'casio',
    use_bigints: true,
    consistency:{
        select:'ONE',
        insert:'ONE',
        update:'ONE',
        delete:'ONE'
    }
}

var Vote = (new Casio(options)).model('Vote', options);


Vote.property('key', String, {
    primary:true
});
Vote.property('up', Casio.types.BigInteger, {});
Vote.property('down', Casio.types.BigInteger, {});

//////////////////////////////////////
exports.Vote = Vote
//////////////////////////////////////
