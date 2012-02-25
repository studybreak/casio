Casio 
=====
This is a model layer and CQL generator which uses the node-cassandra-client project.

Caveats
-------
- **This is alphaware**
- Missing has many and has one relationships
- Missing more complete tests
- Assumes all models have a primary key defined (untested otherwise)

Modeling
--------

As of now, the only default_validation types supported are:

- text
- counter (enables support for incr/decr columns)

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

Primary Keys
------------
Right now, the primary key must be the first property defined for a model. Otherwise, the CQL insert statement column ordering is going to be off.


BigInt
------
in order to support bigints out of the box the cassandra-client connections must enable them.
still trying to figure out the best way to support these.

Unique Columns
--------------
not currently supporting unique values
requires doing a manual lookup if we need to handle them