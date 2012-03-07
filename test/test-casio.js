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
        is_admin: false
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
                // console.log(results)
                USERS.push(user);
                next();
            })
        })
    })

    async.series(order, function(err, results){
        // set USER
        USER = USERS[0];
        callback();
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
    }, 5000);
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

            // check to see if we inserted our funky property
            test.ok(user['~tilde'], 'testing');

            // test booleans
            test.equal(user.userId, USER.userId);
            next();
        });

    });

    // test validators on create
    // notNull:true
    order.push(function(next){
      var notNull = new model.User({
        first_name:'Testi',
        last_name:'Cles'
      })
      notNull.create(function(err, results){
        test.equal(err.name.length, 1);
        next();
      })
    });

    async.series(order, function(err, results){
        test.done();
    });
}

exports.test_user_count = function(test){


    var order = [];
    order.push(function(next){
        createUsers(next);
    });
    order.push(function(next){
        model.User.count(function(err, results){
            // console.log(results);
            next();
        })
    });
    order.push(function(next){
        model.User.count(['email=:email', {email:'dirty@hairy.com'}],
            function(err, results){
                // console.log(results);
                next();
        })
    })
    async.series(order, function(err, results){
        test.done();
    });

}


exports.test_user_find = function(test){

    var order = [];
    order.push(function(next){
        createUsers(next);
    });
    order.push(function(next){
        model.User.getByEmail('dirty@hairy.com', function(err, users){
                test.ok(users.length);
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
                test.equal(user._type, 'UserShort');
                next();
        })

    });

    // instance delete
    order.push(function(next){
        var user = USERS[USERS.length-1]

        user.delete(function(err, results){

            test.equal(typeof err, 'undefined');
            test.equal(typeof results, 'undefined');
            test.ok(user.deleted(), true);
            next();
        });
    });

    order.push(function(next){
        model.User.find({
                columns: ['*'],
                // where: ['email = :email', {email:'dirty@hairy.com'}],
            }, function(err, users){
                test.equal(users.length, USERS.length - 1);
                next();
        })

    });

    async.series(order, function(err, results){
        test.done();
    })
}

exports.test_user_get = function (test){


    var order = [];
    order.push(function(next){
        createUsers(next);
    });

    order.push(function(next){
        test.strictEqual(USER.created_at.getTime(), USER.updated_at.getTime());
        next();
    })

    order.push(function(next){

        // get the user we created during setUp...
        model.User.get(USER.userId, function(err, user){

            if (err) console.log(err);

            // test booleans
            test.ok(user.is_admin, true);
            test.ok(user._props.is_admin, true);

            // test instance method was properly set
            test.strictEqual(user.hello(), 'Hello, ' + user.first_name + ' ' + user.last_name + ' (' + user.email + ')');
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

    var user;
    var order = [];

    order.push(function(next){
        createUsers(next);
    })

    order.push(function(next){
        user = USERS[USERS.length - 3];
        user['~tilde'] = 'testing_update';
        user.first_name = 'Max';
        user.update({last_name:'Amillion'}, function(err, results){
           next();
        });
    });

    order.push(function(next){

        model.User.get({
                userId:user.userId,
                columns:['*']
            }, function(err, userGet){

            test.equal(userGet['~tilde'], 'testing_update')
            test.equal(userGet.first_name, 'Max');
            test.equal(userGet.last_name, 'Amillion');

            next();
        })

    });

    // test validators on create
    // notNull:true
    order.push(function(next){
      user.update({name:null}, function(err, results){
        test.equal(err.name.length, 1);
        test.equal(typeof results, 'undefined');
        next();
      })
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

            test.equal(typeof err, 'undefined');
            test.equal(user2.deleted(), true);
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

            test.equal(err, null);
            test.equal(typeof results, 'undefined');
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

exports.test_user_eager = function (test){

    var order = [];
    var up = 100;
    var down = 50;

    order.push(function(next){
        createUsers(function(err, results){
            next();
        });
    })
    
    // ADD VOTE TO USER
    order.push(function(next){
        var key = USER.userId;

        model.Vote.incr(key, 'up', up, function(err, results){
            model.Vote.decr(key, 'down', down, function(err, results){
                next();
            })
        })
    })

    // ADD PERSON TO USER
    order.push(function(next){

        var person = new model.Person({
            address1:'1 Main St.',
            address2:'',
            city:'Pacifica',
            state:'CA',
            zipcode:'94044'
        });

        person.create(function(err, results){
            USER.person = person;
            USER.update(function(err, results){
                next();
            })
        })
    })
    
    // ADD PETS FOR THIS USER
    order.push(function(next){

        var pets = [
            {name:'Fido', userId: USER.userId},
            {name:'Wally', userId: USER.userId},
            {name:'Sharky', userId: USER.userId},
            {name:'Clifford', userId: USER.userId}
        ]
        var petsOrder = [];
        _.each(pets, function(p){

            // ADD PETS FOR THIS USER
            petsOrder.push(function(petsNext){
                var pet = new model.Pet(p);
                pet.create(function(err, results){
                    petsNext();
                })
            })
        })
        async.parallel(petsOrder, function(err, results){
            next();
        })
    })
    
    // ADD FRIENDS FOR THIS USER
    order.push(function(next){

        var friends
        var friendsOrder = [];
        friendsOrder.push(function(friendsNext){

            // associate these friends with our test user
            friends = new model.Friends(USER.userId);

            var rows = [
                {name:'000', value:'Waldo'},
                {name:'001', value:'George'},
                {name:'002', value:'Gretyl'},
                {name:'003', value:'Henry'},
                {name:'004', value:'Marge'},
                {name:'005', value:'Wilson'},
                {name:'006', value:'Champ'},
                {name:'007', value:'Buster'},
                {name:'008', value:'Ace'},
                {name:'009', value:'Mike'},
                {name:'010', value:'Animal'}
            ];
            friends.set(rows);
            friends.create(function(err, results){
                test.ok(friends.created);
                test.ok(results.success);
                friendsNext();
            })
        })
        async.series(friendsOrder, function(err, results){
            next();
        })
    })

    // ADD GROUPS FOR THIS USER
    order.push(function(next){

        var groups
        var groupsOrder = [];
        groupsOrder.push(function(groupsNext){

            // define a brand new groups
            groups = new model.Groups();

            var rows = [
                {name:'000', value:'The Eagles'},
                {name:'001', value:'Night Ranger'},
                {name:'002', value:'Stryper'},
                {name:'003', value:'KISS'},
                {name:'004', value:'UFO'},
                {name:'005', value:'Journey'},
            ];
            groups.set(rows);
            groups.create(function(err, results){
                test.ok(groups.created);
                test.ok(results.success);

                // set our groups on this user...
                USER.groups = groups;
                USER.update(function(err, results){
                    groupsNext();
                })

            })
        })
        async.series(groupsOrder, function(err, results){
            next();
        })
    })
    
    // EAGER LOAD USER
    order.push(function(next){
        model.User.get({
            userId:USER.userId,
            eager:{
                person:{},
                vote:{},
                pets:{
                    cql:{limit:3}
                },
                friends:{
                    cql:{first:3}
                },
                groups:{
                    cql:{first:3}
                }
            }

        }, function(err, user){

            // console.log(user)

            // test the person loaded
            test.equal(user.personId, user.person.personId);
            test.equal(user.vote.up, up);
            test.equal(user.vote.down, -(down));
            test.equal(user.pets.length, 3);
            test.equal(user.friends.rowCount(), 3);

            user.friends.each(function(friend, i){
                // console.log(i, friend.name, friend.value);
                test.equal(friend.name, '00' + i.toString());
            })

            test.equal(user.friends.first().value, 'Waldo');
            test.equal(user.friends.last().value, 'Gretyl');
            test.equal(user.friends.rowCount(), 3);

            test.equal(user.groups.first().value, 'The Eagles');
            test.equal(user.groups.last().value, 'Stryper');
            test.equal(user.groups.rowCount(), 3);
            next();
            
        })
    })
    
    // TEST Model.find eager loads
    order.push(function(next){
        model.User.find({
            // where:['userId=:key', {key:USER.userId}],
            eager:{
                person:{},
                vote:{},
                pets:{
                    cql:{limit:3}
                },
                friends:{
                    cql:{first:3}
                },
                groups:{
                    cql:{first:3}
                }
            }
        }, function(err, users){
            // console.log(users.length)
            _.each(users, function(user){
                if (USER.userId === user.userId){

                    test.equal(user.personId, user.person.personId);
                    test.equal(user.vote.up, up);
                    test.equal(user.vote.down, -(down));
                    test.equal(user.pets.length, 3);
                    test.equal(user.friends.rowCount(), 3);

                    user.friends.each(function(friend, i){
                        // console.log(i, friend.name, friend.value);
                        test.equal(friend.name, '00' + i.toString());
                    })

                    test.equal(user.friends.first().value, 'Waldo');
                    test.equal(user.friends.last().value, 'Gretyl');
                    test.equal(user.friends.rowCount(), 3);

                    test.equal(user.groups.first().value, 'The Eagles');
                    test.equal(user.groups.last().value, 'Stryper');
                    test.equal(user.groups.rowCount(), 3);
                }
            })
            next();
        })
      
    });

    async.series(order, function(err, results){
        test.done();
    })


}
exports.test_batch = function(test){


    var q1 = new CQL('q1');
    q1.insert('User');
    q1.into(['userId', 'name']);
    q1.values(['1234', 'name name']);

    var q2 = new CQL('q2');
    q2.update('User');
    q2.set({name:'Greg'});
    q2.where('userId=:userId', {userId:'1234'});

    var q3 = new CQL('q2');
    q3.delete([]);
    q3.from('User');
    q3.where('userId=:userId', {userId:'1234'});


    var cql = new CQL('batch');
    cql.batch('ALL');
    cql.query(q1);
    cql.query(q2);
    cql.query(q3);
    // console.log(cql.statement());
    // piggyback off User connection for now
    model.User.cql(cql.statement(), [], function(err, results){
        test.done();
    })


}

// function test_create_indicies(){
//     model.User.createIndicies();
// }

exports.test_vote_create = function(test){

    // var vote = model.Vote();

    var key = USERS[USERS.length-1].userId;
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

            test.equal(vote.up, up);
            test.equal(vote.down, -(down));

            next();
        })
    })
    order.push(function(next){
        vote.incr('up', function(err, results){
            vote.decr('down', function(err, results){
                next();
            })
        })
    })
    // test the above was set properly
    order.push(function(next){
        model.Vote.get(key, function(err, vote){
            test.equal(vote.up, up + 1);
            test.equal(vote.down, -(down + 1));

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
            s.push(i.toString());
            into.push(s.join(''));
            values.push(i);

        })

        var q = new CQL('inserting into friends: ' + id);
        q.insert('Friends');
        q.into(into);
        q.values(values);

        order.push(function(next){
            model.Friends.cql(q.statement(), [], function(err, results){
                // console.log("Done", q.name);
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
            test.ok(friends.created);
            test.ok(results.success);
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


exports.test_friends_delete = function(test){

    var friends
    var rows = [
        {name:'000', value:'Yellowman'},
        {name:'001', value:'Bob Marley'},
        {name:'002', value:'Barrington Levy'}
    ];

    var order = [];
    order.push(function(next){
        friends = new model.Friends();
        friends.set(rows);
        friends.create(function(err, results){
            test.ok(friends.created);
            next();
        })
    })
    order.push(function(next){

        // get rid of yellowman since
        // he's creepy...
        friends.delete(['000'], function(err, results){
            next();
        })

    });
    order.push(function(next){
        friends.reset();
        friends.range(function(err, results){
            test.equal(friends.rowCount(), rows.length - 1);
            next();
        })
    });
    async.series(order, function(err, results){
        test.done();
    })

}
