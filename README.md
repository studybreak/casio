Modeling
========

> var Keyboard = Casio.model('Keyboard')
> Keyboard.property('key', {
> 	primary:true
> })


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