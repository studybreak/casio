DROP KEYSPACE casio;

CREATE KEYSPACE casio WITH
    strategy_class=SimpleStrategy AND
    strategy_options:replication_factor=1;

USE casio;

CREATE COLUMNFAMILY User (
    userId text PRIMARY KEY,
    personId text,
    groupsId text,
    name text,
    first_name text,
    last_name text,
    email text,
    birthday text,
    gender text,
    is_admin boolean,
    visits int,
    access_token text,
    visited_at timestamp,
    created_at timestamp,
    updated_at timestamp,
    '~tilde' text
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

CREATE COLUMNFAMILY Groups (
    groupsId text PRIMARY KEY
) WITH default_validation=text AND comparator=text;

CREATE COLUMNFAMILY Person (
    personId text PRIMARY KEY,
    address1 text,
    address2 text,
    city text,
    state text,
    zipcode text,
    created_at timestamp,
    updated_at timestamp
) WITH default_validation=text AND comparator=text;

CREATE COLUMNFAMILY Pet (
    petId text PRIMARY KEY,
    userId text,
    name text,
    created_at timestamp,
    updated_at timestamp
) WITH default_validation=text AND comparator=text;
CREATE INDEX PetUserId ON Pet (userId);

CREATE COLUMNFAMILY Shards (
    KEY text PRIMARY KEY,
    name text,
    created_at timestamp,
    updated_at timestamp
) WITH default_validation=text AND comparator=text;