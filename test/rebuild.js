var util = require('util');
var async = require('async');
var _ = require('underscore');
var Cassandra = require('cassandra-client');
var ColumnFamilyDef = Cassandra.CfDef;
var KeyspaceDef = Cassandra.KsDef;
var ColumnDef = Cassandra.ColumnDef;

/////////////////////////
Cassandra.System.prototype.dropKeyspace = function(ksDef, callback){
    this.q.put(function(con) {
      con.thriftCli.system_drop_keyspace(ksDef, callback);
    });
    this.emit('checkq');
}

/////////////////////////
var KEYSPACE = 'casio';
/////////////////////////

//// load the keyspaces file up...
var cql_file = require('fs').readFileSync(__dirname + '/keyspaces/casio.cql', 'utf8')

var statements = [];
var system_statements=[];

var system_cmds = ['DROP KEYSPACE', 'CREATE KEYSPACE', 'USE']

_.each(cql_file.split(';\n'), function(st){
    st = st.replace(/\n|\t/g, '');

    var sys_cmd = false;
    _.each(system_cmds, function(cmd){
        if (st.indexOf(cmd) > -1) {
           sys_cmd = true; 
        }
    })
    if (sys_cmd) {
        if (st.indexOf('USE') == -1) system_statements.push(st);
    } else {
        statements.push(st);
    }    
})

// console.log(system_statements)
// console.log(statements)
var sys = new Cassandra.System('127.0.0.1:9160');

var system = new Cassandra.Connection({
    host:'127.0.0.1',
    port:9160,
    keyspace:'system'
});

var keyspace = new Cassandra.Connection({
    host:'127.0.0.1',
    port:9160,
    keyspace:'casio'
});

system.connect(function(err){
    if (err) console.log(err);
    run();
})

function run(){
    // prepare all the system statements...
    // drop keyspace, create keyspace, etc.
    
    var order = [];
    _.each(system_statements, function(cql){
        order.push(function(next){
            system.execute(cql, [], function(err, results){
                console.log(cql)
                if (err) {
                    console.log('ERROR:', err);
                    throw new Error(err)
                }
                next();
            })
        });
    })

    // connect to the keyspace now we've created it...
    order.push(function(next){
        keyspace.connect(function(err){
            if (err) {
                console.log('ERROR:', err);
                throw new Error(err)
            }
            next();
        })
    })

    _.each(statements, function(cql){
        order.push(function(next){
            keyspace.execute(cql, [], function(err, results){
                console.log(cql)
                if (err) {
                    console.log('ERROR:', err);
                    throw new Error(err)
                }
                next();
            })
        });
    })

    order.push(function(next){
        sys.describeKeyspace(KEYSPACE, function(err, ksDef) {        
            console.log("Keyspace", util.inspect(ksDef, true, null, true))
            next();
        });
        
    })


    //         if (err) console.log(err);
    //         if (results) {
    //             console.log("Added keyspace", util.inspect(casio, true, null, true))
    //             console.log(results);
    //         }
    //         next();

    async.series(order, function(err, results){
      // console.log(err, results)
      process.exit(0);
    })    
}
exports.run = run;

// add keyspace
// order.push(function(next){
//     system.addKeyspace(casio, function(err, results){
//         if (err) console.log(err);
//         if (results) {
//             console.log("Added keyspace", util.inspect(casio, true, null, true))
//             console.log(results);
//         }
//         next();
//     })
// });


