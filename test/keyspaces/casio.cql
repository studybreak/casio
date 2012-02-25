DROP KEYSPACE casio;

CREATE KEYSPACE casio WITH
    strategy_class=SimpleStrategy AND
    strategy_options:replication_factor=3;

USE casio;

CREATE COLUMNFAMILY User (
    userId text PRIMARY KEY,
    name text,
    first_name text,
    last_name text,
    email text,
    birthday text,
    gender text,
    is_admin boolean,
    visits int,
    visited_at timestamp,
    created_at timestamp,
    updated_at timestamp
) WITH default_validation=text AND comparator=text;
CREATE INDEX UserEmail ON User (email);

CREATE COLUMNFAMILY Vote (
    KEY text PRIMARY KEY,
    up counter,
    down counter
) WITH default_validation=counter AND comparator=text;

CREATE COLUMNFAMILY Friends (
    KEY text PRIMARY KEY
) WITH default_validation=text AND comparator=text;