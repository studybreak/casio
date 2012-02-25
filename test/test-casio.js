var Casio = require('../').Casio;
var CQL = require('../').CQL;

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

exports.test_user_something = function(test){
    // test the class method was attached
    test.equal(model.User.something(), 'this is something;')
    test.done();
}

exports.test_user_create = function(test){
    // test the class method was attached
    // test.equal(model.User.something(), 'this is something;')
    
    var order = [];
    order.push(function(next){
        createUsers(next);
    });
    order.push(function(next){
        model.User.get(USER.userId, function(err, user){
            if (err) console.log(err);

            // test booleans
            test.ok(user.userId, USER.userId)
            next();
        });

    });    
    
    
    
    
    async.series(order, function(err, results){
        test.done()
    });
}

exports.test_user_count = function(test){
    
    
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


exports.test_user_find = function(test){
    
    var order = [];
    order.push(function(next){
        createUsers(next);
    });
    order.push(function(next){
        model.User.getByEmail('dirty@hairy.com', function(err, users){
                test.ok(users.length)
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
                var user = users[0];
                test.equal(user._type, 'UserShort')                
                next();
        })
    
    });
    
    // instance delete
    order.push(function(next){
        var user = USERS[USERS.length-1]
        
        user.delete(function(err, results){
            
            test.ok(results.success, true);
            test.ok(user.deleted(), true)
            next();
        });
    });    
    
    order.push(function(next){
        model.User.find({
                columns: ['*'],
                // where: ['email = :email', {email:'dirty@hairy.com'}],
            }, function(err, users){
                test.equal(users.length, USERS.length - 1)
                next();
        })
    
    });    
    
    
    
    
    
    async.series(order, function(err, results){
        test.done()
    })    
}

exports.test_user_get = function (test){


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

            // test booleans
            test.ok(USER.is_admin, true)
            test.ok(USER._props.is_admin, true)

            // test instance method was properly set
            test.strictEqual(USER.hello(), 'Hello, ' + USER.first_name + ' ' + USER.last_name + ' (' + USER.email + ')');
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
            test.equal(user2.deleted(), true)
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
//     model.User.createIndicies();
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
    
    var up = 100; //10;
    var down = 100;
    
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

            // console.log('---Vote---');
            // console.log(vote);

            test.equal(vote.up, up)
            test.equal(vote.down, -(down))

            next();
        })
    })
    order.push(function(next){
        vote.incr('up', function(err, results){
            vote.decr('down', function(err, results){
                next()
            })
        })
    })
    // test the above was set properly
    order.push(function(next){
        model.Vote.get(key, function(err, vote){
            test.equal(vote.up, up + 1)
            test.equal(vote.down, -(down + 1))

            next();
        })
    })

    async.series(order, function(err, results){
        test.done();
    })
}

exports.test_friends_range = function(test){
    var friends;
    var order = [];
    _.each(_.range(1, 5), function(id){

        var into = ['KEY'];
        var values = [id];

        // var args = {'KEY':id}
        
        var key;
        _.each(_.range(1, 1000), function(i){

            // var rnd = Math.ceil((Math.random() * 1000) * 86400)
            // 
            // var dt = new Date().getTime();
            // key = "'" + (dt + rnd) + ":" + i + "'";
            // args[key] = rnd

            // args[i] = i;
            
            var len = i.length;
            var size = 5;
            var s = []
            _.each(_.range(0, size - i.toString().length), function(x){
                s.push('0');
            })
            s.push(i.toString())
            into.push(s.join(''));
            values.push(i);

        })

        var q = new CQL('inserting into friends: ' + id);
        q.insert('Friends');
        q.into(into);
        q.values(values)

        order.push(function(next){
            model.Friends.cql(q.statement(), [], function(err, results){
                console.log("Done", q.name);
                next();
            })
        })
        
    })

    // test instance range method
    order.push(function(next){
        friends = new model.Friends(1);
        
        // should return everything!!!
        friends.range({start:null, end:'00010'}, function(err, results){
            test.equal(friends.rowCount(), 10);
            next();
        })
    })
    
    order.push(function(next){
        
        friends.next(10, function(err, results){
            test.equal(friends.rowCount(), 20);
            next();
        })
    })


    /// TEST PREV
    order.push(function(next){
        friends = new model.Friends(1);
        
        // should return everything!!!
        friends.range({start:'00020', end:'00029'}, function(err, results){
            test.equal(friends.rowCount(), 10);
            next();
        })
    })

    order.push(function(next){
        
        friends.prev(10, function(err, results){

            test.ok(friends.hasPrev(), true);
            test.equal(friends.rowCount(), 20);
            
            friends.prev(10, function(err, results){
                
                test.ok(!friends.hasPrev(), true);
                test.equal(friends.rowCount(), 29);
                next();
            })
        })
    })
    
    order.push(function(next){
        friends.next(10, function(err, results){
            test.equal(friends.rowCount(), 39);
            next();
        })
    })    

    async.series(order, function(err, results){
        test.done();
    })

}

exports.test_friends_create = function(test){
    var friends
    var order = [];
    order.push(function(next){
        friends = new model.Friends('3 Stooges');

        var rows = [
            {name:'000', value:'Larry'},
            {name:'001', value:'Curly'},
            {name:'002', value:'Moe'}        
        ];
        friends.set(rows);
        friends.set({'name':'003', 'value':'Curly Joe'});
        friends.set({'name':'001', 'value':'Curly Howard'});
        friends.set({'name':'004', 'value':'Shemp'});
        friends.set({'name':'002', 'value':'Moe Howard'});
        friends.set({'name':'000', 'value':'Larry Fine'});        
        friends.set({'name':'003', 'value':'Curly Joe'});
                
        friends.create(function(err, results){
            test.ok(friends.created)
            test.ok(results.success)
            next();
        })
    })
    async.series(order, function(err, results){
        test.done();
    })    
}

exports.test_friends_update = function(test){

    var friends
    var order = [];
    order.push(function(next){
        friends = new model.Friends();

        var rows = [
            {name:'000', value:'Tom'},
            {name:'001', value:'Dick'},
            {name:'002', value:'Harry'}
        ];

        friends.create(function(err, results){
            test.ok(friends.created)
            next();
        })
    })
    order.push(function(next){

        var rows = [
            {name:'003', value:'Garry'},
            {name:'004', value:'Walt'},                    
            {name:'000', value:'Tommy'},
            {name:'001', value:'Dickie'},
            {name:'002', value:'Harrison'}
        ];

        friends.update(rows, function(err, results){
            next();
        })
        
    });

    order.push(function(next){
        
        // set one for the hell of it...
        friends.set({name:'003', value:'Ozzy Osbourne'});

        // look ma, no rows!
        friends.update(function(err, results){
            next();
        })
        
    });

    order.push(function(next){

        // reset this and load all the rows to make sure the update went through
        friends.reset();

        // look ma, no rows!
        friends.range(function(err, results){
            test.strictEqual(friends.row('000').value, 'Tommy')
            test.strictEqual(friends.row(3).value, 'Ozzy Osbourne')
            test.strictEqual(friends.row(), null)
            test.strictEqual(friends.rows()[3].value, 'Ozzy Osbourne')
            test.strictEqual(friends.row(10), null)
            next();
        })
        
    });




    async.series(order, function(err, results){
        test.done();
    })    



}
