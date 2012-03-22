Casio 
=====
This is a model layer and CQL generator which uses the node-cassandra-client project.

Caveats Emptor
--------------

- **This is alphaware**
- Assumes all models have a primary key defined (untested otherwise)
- Nice-to-haves: more validators.

Modeling
--------

This library supports modeling two types of Objects.

##Model##
You can think of this a traditional Model in the ORM-sense.

You define a mix of properties and associations.

As of now, the only supported default_validation types are:

- text
- counter (enables support for incr/decr columns)

##ModelArray##
This is a wrapper around columnfamily's with an unknown number of columns.

Use this if you need to support SlicePredicate's (i.e. range queries)

It behaves similar to a list and has support for pagination.


##Model Options##
The options argument passed to model declarations defaults to this:

~~~
{
	consistency:{
	    select:'ONE',
	    insert:'ONE',
	    update:'ONE',
	    delete:'ONE'
	},
	get:{
	    columns:['*']
	},
	delete:{
	    columns:['*']
	}
}
~~~

You can define the consistency levels for all CRUD operations.

In addition, you can override what columns are selected by default or deleted by default.

##Property Definitions##

Model.property('name', Type, {options...})


##Getters/Setters##

Do need to define some properties on your model?

Model.getter('somevalue', function(){
	return this._somevalue;
})

Model.setter('somevalue', function(val){
	this._somevalue = val;
})

##Associations##

We support the following association types:

- Has One

~~~
User.hasOne('house', House, {
	on:'user_id'
})
~~~

In this example, we assume we have a House Model with a property of user_id.

- Has Many

~~~
User.hasMany('pets', Pet, {
	on:'owner_id'
})
~~~

This only requires setting an 'on' value if its different then the User primary key.

In this case, it is, so we set it to 'owner_id'.

- Belongs To

~~~
User.belongsTo('person', Person, {
	fk:'person_id',
	on:'person_id'
})
~~~

The columns on both side of the belongs to association ('fk' and 'on') default to the primary key of the Class which is being associated.

In this case, Person. If we defined Person.person_id as a primary key we wouldn't need to pass anything above.

##Property Validators##
We support one out-of-the box validator: notNull. (see model definitions below for example.)

~~~

function(prop, val){
  if (val===null) this.error(prop, ':prop is null.');
}

~~~

If you want to write your own, you can hook these into your property definitions.

Basic validator arguments:

~~~
function(prop, val){}
~~~

Pushing errors onto the instance is easy:

~~~
this.error(prop, ':prop is null.');
~~~

##Client Connections##

The client connections are handled by the [cassandra-client](https://github.com/racker/node-cassandra-client).

By default, we use the pooled connection feature (see client configuration options for master list).

To set this up, you'll need to do something like the following:

~~~

var Casio = require('casio').Casio;
var hosts = ['localhost:9160', 'domain:port'];
var casio = new Casio({
    hosts: hosts,
    keyspace: 'keyspace',
    use_bigints: true
});

// This is where you'll add your own hooks for
// client logging...
casio.on('connect', function () {
    winston.info('Casio connected', hosts);
});

casio.on('error', function (err) {
    winston.error('Casio error', err);
});

casio.on('log', function (tmp, name, level, msg) {
    if (msg instanceof Object && level ==='cql') {
        msg = msg.query + ' ' + msg.args;
    }
    winston.silly(util.format('[casio][%s] %s -- %s', name, level, msg));
});

~~~

##Model Examples##

With a ColumnFamily definition of:

~~~

-- Model CQL statement
CREATE COLUMNFAMILY Keyboard (
    id text PRIMARY KEY,
    make text,
    model text,
    serial int,
    works boolean,
    created_at timestamp,
    updated_at timestamp
) WITH default_validation=text AND comparator=text;

~~~

You would have a Keyboard Model definition as follows:

~~~

// extending from our connection example above:
var Keyboard = casio.model('Keyboard');

Keyboard.property('id', String, {
	primary:true
});

Keyboard.property('make', String, {
	default:'Casio'
});

Keyboard.property('model', String, {
	default:'Casiotone MT-820'
});

Keyboard.property('serial', Number, {
	validators:[
		function(prop, val){
			if (val < 0) this.error(prop, ':prop is less then zero.');
		}
	]
});

Keyboard.property('works', Boolean, {
	notNull:true
});

Keyboard.hasOne('vote', Vote)

Keyboard.classMethods({
	....
})

Keyboard.instanceMethods({
	....
})

~~~

Example of a Counter type column family...

~~~

-- Counter Model CQL statement
CREATE COLUMNFAMILY Vote (
    keyboard_id text PRIMARY KEY,
    up counter,
    down counter
) WITH default_validation=counter AND comparator=text;

~~~

Vote has a Model definition as follows:

~~~

// extending from our connection example above:
var Vote = casio.model('Vote', {model options here});

Vote.property('keyboard_id', String, {
    primary:true
});
Vote.property('up', Casio.types.BigInteger);
Vote.property('down', Casio.types.BigInteger);

~~~

Example of our Keyboard having some friends:

~~~

-- ModelArray CQL statement
CREATE COLUMNFAMILY Friend (
    keyboard_id text PRIMARY KEY
) WITH default_validation=text AND comparator=text;

~~~

And, a Friend Model definition as follows:

~~~

// extending from our connection example above:
// ModelArray primary columns default to 'key' if not defined
// notice the subtle difference for defining the primary key
var Friend = casio.array('Friend');
Friend.primary('keyboard_id');

~~~

See test/model and test/keyspaces for more examples.

CRUD
----
##Model.prototype.get##

##eager loading##

##as Model##


Test Suite
----------
Pre-requisites: 

- Standalone Cassandra server running on localhost:9160
- Creating this keyspace:

~~~

CREATE KEYSPACE casio WITH
    strategy_class=SimpleStrategy AND
    strategy_options:replication_factor=1;

~~~

To run:
 
- `npm test`

or...

- `./test/testsuite.sh`

Notes
=====

BigInt
------
in order to support bigints out of the box the cassandra-client connections must enable them.
still trying to figure out the best way to support these.

Unique Columns
--------------
Not currently supporting unique values
Requires doing a manual lookup if you need unique column support
Will eventually work, but requires validation hooks are in place, first.