Modeling
========

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

Keyboard.property('make', String, {});
Keyboard.property('model', String, {});
Keyboard.property('serial', Number, {});
Keyboard.property('works', Boolean, {});

Keyboard.classMethods({
	....
})

Keyboard.instanceMethods({
	....
})
~~~

Test Suite
==========
- Pre-requisites: Standalone Cassandra server running on localhost:9160
- Creates a keyspace called casio

	`./test/testsuite.sh`

- See test/model for examples

Notes
=====

BigInt
------
in order to support bigints out of the box the cassandra-client connections must enable them.
still trying to figure out the best way to support these.

Unique Columns
--------------
not currently supporting unique values
requires doing a manual lookup if we need to handle them