var Casio = require('../').Casio;
var Cassandra = require('cassandra-client');
var model = require('./model');
var _ = require('underscore')
var async = require('async');


//////////////////////////////
var USER, USERS = [];
var USER_OBJECTS = [
    {
        name:'Dirty Harry',
        first_name:'Dirty',
        last_name:'Harry',
        birthday:'12/23/1971',
        gender:'M',
        state:'CA',
        email:'dirty@hairy.com',
        visits:0,
        is_admin: true
    },
    {
        name:'Mike Hunt',
        first_name:'Mike',
        last_name:'Hunt',
        birthday:'01/01/1979',
        gender:'M',
        state:'CA',
        email:'mike@hunt.com',
        visits:0,
        is_admin: true
    },
    {
        name:'Ben Dover',
        first_name:'Ben',
        last_name:'Dover',
        birthday:'11/10/1980',
        gender:'M',
        state:'CA',
        email:'ben@dover.com',
        visits:15,
        is_admin: false
    },
    {
        name:'Juwana Bone',
        first_name:'Juwana',
        last_name:'Bone',
        birthday:'8/18/1984',
        gender:'F',
        state:'WI',
        email:'juwana@bone.com',
        visits:421,
        is_admin: false
    },
    {
        name:'Oliver Clothesoff',
        first_name:'Oliver',
        last_name:'Clothesoff!',
        birthday:'2/13/1981',
        gender:'M',
        state:'KY',
        email:'oliver@clothesoff',
        visits:16001,
        is_admin: false
    }
    
]

//////////////////////////////
function createUsers(callback){
    var order = []
    _.each(USER_OBJECTS, function(props){
        order.push(function(next){
            var user = new model.User(props)
            user.create(function(err, results){
                console.log(results)
                USERS.push(user);
                next();
            })
        })
    })
    
    async.series(order, function(err, results){
        // set USER
        USER = USERS[0];
        callback()
    })

}


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
//////////////////////////////



exports.setUp = function(callback){
    // give the clients time to connect;
    setTimeout(function(){
        callback();
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

exports.test_user_count= function(test){
    
    
    var order = [];
    order.push(function(next){
        createUsers(next);
    });
    order.push(function(next){
        model.User.count(function(err, results){
            console.log(results);
            next();
        })
    });
    order.push(function(next){
        model.User.count(['email=:email', {email:'dirty@hairy.com'}], 
            function(err, results){
                console.log(results);
                next();
        })
    })
    async.series(order, function(err, results){
        test.done()
    });
    
}


exports.test_user_find=function(test){
    
    var order = [];
    order.push(function(next){
        createUsers(next);
    });
    order.push(function(next){
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
                test.equal(users.length, 1)
                next();
        })        
    });
    
    // test returning query as a completely differnt class
    order.push(function(next){
        model.User.find({
                columns: ['first_name', 'last_name'],
                where: ['email = :email', {email:'dirty@hairy.com'}],
                as: model.UserShort
            }, function(err, users){
                if (err) {
                    console.log(err);
                    throw new Error(err);
                }
                var user = users[0];
                test.equal(user._type, 'UserShort')                
                
                next();
        })
    
    });
    
    // instance delete
    order.push(function(next){
        var user = USERS[USERS.length-1]
        
        user.delete(function(err, results){
            
            test.strictEqual(results.success, true);
            test.equal(user.isDeleted(), true)
            next();
        });
    });    
    
    order.push(function(next){
        model.User.find({
                columns: ['*'],
                // where: ['email = :email', {email:'dirty@hairy.com'}],
            }, function(err, users){
                if (err) {
                    console.log(err);
                    throw new Error(err);
                }
                test.equal(users.length, USERS.length - 1)
                next();
        })
    
    });    
    
    
    
    
    
    async.series(order, function(err, results){
        test.done()
    })    
}

exports.test_user_get=function (test){


    var order = [];
    order.push(function(next){
        createUsers(next);
    });
    
    order.push(function(next){
        test.strictEqual(USER.created_at.getTime(), USER.updated_at.getTime())
        next();
    })
    
    order.push(function(next){
        
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
    
    });
    order.push(function(next){
        
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
    
    });
    
    async.series(order, function(err, results){
        test.done();
    })
}

exports.test_user_update = function(test){

    var order = [];
    
    order.push(function(next){
        createUsers(next);
    })

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

exports.test_user_delete = function(test){

    var order = [];
    var user2, user3;

    order.push(function(next){
        createUsers(function(err, results){
            user2 = USERS[USERS.length - 2];    
            user3 = USERS[USERS.length - 1];
            next();
        });
    })

    // instance delete
    order.push(function(next){
        user2.delete(function(err, results){
            
            test.strictEqual(results.success, true);
            test.equal(user2.isDeleted(), true)
            next();
        });
    });
    
    // did the instance delete work?
    order.push(function(next){
        model.User.get(user2.userId, function(err, user){
            test.strictEqual(user, null);
            next();
        })
    
    });
    
    // static delete
    order.push(function(next){
        model.User.delete(user3.userId, function(err, results){

            test.strictEqual(results.success, true);
            next();
        })
        
    });
    // did the static delete work?
    order.push(function(next){
        model.User.get(user3.userId, function(err, user){
            test.strictEqual(user, null);
            next();
        })
    
    })
    
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



