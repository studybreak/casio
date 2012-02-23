var Casio = require('../').Casio;
var Cassandra = require('cassandra-client');
var model = require('./model');
var _ = require('underscore')
var async = require('async');


//////////////////////////////
var now = new Date().getTime()
var USER = new model.User({
    // userId: new Date().getTime() + '-' + new Cassandra.UUID().toString(),
    name:'Dirty Harry ' + now,
    first_name:'Dirty',
    last_name:'Harry ' + now,
    birthday:'06/19/1974',
    gender:'M',
    state:'CA',
    email:'dirty@hairy.com',
    visits:0,
    is_admin: true
});

//////////////////////////////

function checkClient(){
    var total=0, connected=0;
    for (var name in model){
        isConnected = model[name].prototype._clientConnected;
        if (isConnected !== undefined){
            total++;
            if(isConnected !== null) connected++;
        }
    }
    console.log('Clients connected:', connected, '/', total);
}

exports.setUp = function(callback){
    // give the clients time to connect;
    setTimeout(function(){
        USER.create(function(err, results){
            if (err) {
                console.log(err);
                throw new Error(err)
            }
            callback()
        })
    }, 100)
}


exports.tearDown = function (callback) {
    // todo: drop the casio keyspace


    // set timeout to give 'npm test' a few seconds 
    // to do its thing
    setTimeout(function(){        
        process.exit(0);
    }, 2000);
    callback();
}

exports.test_user_something= function(test){
    // test the class method was attached
    test.equal(model.User.something(), 'this is something;')
    test.done();
}

exports.test_user_create= function(test){
    // test the class method was attached
    // test.equal(model.User.something(), 'this is something;')
    test.done();
}


exports.test_user_find=function(test){
    async.series([
        function(next){
            model.User.find({
                    // columns:'full_name, birth_year',
                    // where:['key = :key', {key:1}],
                    where:['email = :email', {email:'dirty@hairy.com'}],
                    // start:1,
                    // limit:10
                }, function(err, users){
                    if (err) {
                        console.log(err);
                        throw new Error(err);
                    }
                    if (users) {
                        console.log('--- Users ---')
                        console.log(users);
                    }
                    next();
            })        
        }, 
        function(next){
            model.User.find({
                    columns: ['first_name', 'last_name'],
                    where: ['email = :email', {email:'dirty@hairy.com'}],
                    as: model.UserShort
                }, function(err, users){
                    if (err) {
                        console.log(err);
                        throw new Error(err);
                    }
                    if (users) {
                        console.log('--- Users as UserShort ---')
                        console.log(users);
                    }
                    next();
            })
        
        }
    ], function(err, results){
        
        test.done()
    })    
}

exports.test_user_get=function (test){

    // Test the setUp user creation here...
    test.strictEqual(USER.created_at.getTime(), USER.updated_at.getTime())
    
    async.series([

        function(next){
            
            // get the user we created during setUp...
            model.User.get(USER.userId, function(err, user){
                if (err) console.log(err);
                if (user) {
                    console.log('--- User---')
                    console.log(user);

                    // test booleans
                    test.equal(USER.is_admin, true)
                    test.equal(USER._props.is_admin, true)

                    // test instance method was properly set
                    test.strictEqual(USER.hello(), 'Hello, ' + USER.first_name + ' ' + USER.last_name + ' (' + USER.email + ')');
                }
                next();
            });
        
        },
        function(next){
            
            // get the short version of the user we created during setUp...
            model.User.get({
                userId: USER.userId,
                columns:['first_name', 'last_name'],
                as: model.UserShort,
            }, function(err, userShort){
                if (err) console.log(err);
                if (userShort){
                    console.log('--- UserShort ---')
                    console.log(userShort);
                    console.log(userShort.hello())
                };
                next();
            });
        
        }
    ], function(err, results){
        test.done();
    })
}

exports.test_user_update = function(test){

    var order = [];
    order.push(function(next){
        
        USER.first_name = 'Max';
        USER.update({last_name:'Amillion'}, function(err, results){
           next() 
        });
    });
    
    async.series(order, function(err, results){
        test.done();
    })
    
    
}



// function test_create_indicies(){
//     User.createIndicies();
// }

exports.test_vote_create = function(test){

    // var vote = model.Vote();
    var key = USER.userId;
    var vote;
    var order = [];
    
    var i64 = 18446744073709551616;
    var i32 = 9007199254740992;
    
    // var bigint = new Casio.types.BigInteger(i64.toString())
    // console.log(bigint.toString(), i64.toString())
    
    var up = i32; //10;
    var down = i32;
    
    // test static incr method
    order.push(function(next){
        model.Vote.incr(key, 'up', up, function(err, results){
            next();
        })
    })

    // test static decr method
    order.push(function(next){
        model.Vote.decr(key, 'down', down, function(err, results){
            next();
        })
    })

    // test the above was set properly
    order.push(function(next){
        model.Vote.get(key, function(err, results){
            vote = results;

            console.log('---Vote---');
            console.log(vote);
            
            test.equal(vote.up, up)
            test.equal(vote.down, -(down))

            next();
        })
    })
    order.push(function(next){
        vote.incr('up', function(err, results){
            vote.decr('down', function(err, results){
                
                setTimeout(next, 1000);       
            
            })
        })
    })
    // test the above was set properly
    order.push(function(next){
        model.Vote.get(key, function(err, results){
            // vote = results;

            console.log('---Vote2---');
            console.log(results);
            
            // test.equal(results.up, up + 1)
            // test.equal(results.down, -(down))

            next();
        })
    })


    async.series(order, function(err, results){
        test.done();
    })

    
}



