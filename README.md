Casio 
=====
This is a model layer and CQL generator which uses the node-cassandra-client project.

Caveats Emptor
--------------

- **This is alphaware**
- Assumes all models have a primary key defined (untested otherwise)

Modeling
--------

The library support modeling two types of Objects.

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

##Examples##

With a ColumnFamily definition of:

~~~

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

You would have a Model definition as follows:

~~~
var Keyboard = Casio.model('Keyboard');

Keyboard.property('id', String, {
	primary:true
});

Keyboard.property('make', String, {
	default:'Casio'
});
Keyboard.property('model', String, {
	default:'Casiotone MT-820'
});
Keyboard.property('serial', Number, {});
Keyboard.property('works', Boolean, {});

Keyboard.classMethods({
	....
})

Keyboard.instanceMethods({
	....
})
~~~






See test/model and test/keyspaces for more examples.

Test Suite
----------
Pre-requisites: 

- Standalone Cassandra server running on localhost:9160

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