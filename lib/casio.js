var util = require('util');
var Buffer = require('buffer').Buffer;

var _ = require('underscore');
var async = require('async');
var Cassandra = require('cassandra-client');

// export the CQL module
var CQL = exports.CQL = require('./cql').CQL

// export the log so we can override it we need to...
var log = function () {};

function logger(fn){
  log = fn
}
exports.logger = logger

/*

API DOCS => http://wiki.apache.org/cassandra/API

todo:

Column Family Validation Types:
    x - Text
    x - Counter (support counter column types)

- Model._options
    - partition consitency for read vs. writes

- Model.property
    x - String
    x - Date
    x - Number
    x - Boolean
    x - BigInts
        x - convert bigint using parseInt(num, 0)
            (this essentially makes it a Int32)

    x - Object (has one & belongs to)
    x - List  (has many)

x - Model.find
    x - override returned class
    x - need to account for deleted tombstone markers
        x - filter from results

x - Model.delete

x - Model.get
    x - cql select statement gen
    x - override returned class
    x - need to account for deleted tombstone markers
        x - returns null if detected

x - Model.prototype.create
    x - cql insert statement gen
    x - make sure the primary key is always the first column in the cql statement

x - Model.prototype.update
    x - cql update statement gen

x - Model.prototype.delete
    x - cql delete statement gen
    x - mark the object as 'deleted'

- Pooled Connections
- Model TTL support
- Model Timestamp
- Model Validations
    on create
    on update

- Property Validations



- Model Event Hooks
x - Batch CQL queries

==================

- ModelArray.range
- ModelArray.find
- ModelArray.delete
x - ModeArray.cql
x - ModelArray.prototype.cql
x - ModelArray.prototype.hasNext
x - ModelArray.prototype.hasPrev
x - ModelArray.prototype.rows
x - ModelArray.prototype.row
x - ModelArray.prototype.rowCount
x - ModelArray.prototype.created
x - ModelArray.prototype.deleted
x - ModelArray.prototype.next
x - ModelArray.prototype.prev
x - ModelArray.prototype.set
x - ModelArray.prototype.create
x - ModelArray.prototype.update
x - ModelArray.prototype.delete


*/

/*
-- CQL Type     Description
-- ascii        US-ASCII character string
-- bigint       64-bit signed long
-- blob         Arbitrary hexadecimal bytes (no validation)
-- boolean      true or false
-- counter      Distributed counter value (64-bit long)
-- decimal      Variable-precision decimal
-- double       64-bit IEEE-754 floating point
-- float        32-bit IEEE-754 floating point
-- int          32-bit signed integer
-- text         UTF-8 encoded string
-- timestamp    Date plus time, encoded as 8 bytes since epoch
-- uuid         Type 1 or type 4 UUID
-- varchar      UTF-8 encoded string
-- varint       Arbitrary-precision integer


ZERO
ONE
QUORUM
ALL
DCQUORUM
DCQUORUMSYNC

Write
ANY
ONE
TWO
THREE
QUORUM
LOCAL_QUORUM
EACH_QUORUM
ALL

Read
ANY
ONE
TWO
THREE
QUORUM
LOCAL_QUORUM
EACH_QUORUM
ALL

*/


var Casio = module.exports.Casio = {
    _models:{},
    types:{
        BigInteger: Cassandra.BigInteger
    }
};


Casio.register = function(model){
    this._models[model.prototype._name] = model;
};


var AbstractModel = function () {};

AbstractModel.nullCallback = function () {};

/**
    AbstractModel.connect

        Explicit method to define when we need to connect our Cassandra client
**/
AbstractModel.connect = function (callback) {
    callback = callback || AbstractModel.nullCallback;

    var self = this;
    this.prototype._client = new Cassandra.Connection(this.prototype._options);

    this.prototype._client.on('log', function(level, message, obj) {
      log('CLIENT[%s] %s -- %j', self.prototype._name, level, message)
    });

    this.prototype._client.connect(function(err){
        if (err) return callback(err);
        self.prototype._clientConnected = true;
        callback();
    });
};

/**
    AbstractModel.classMethods

        Extend class methods

    @methods    - object of methods
**/
AbstractModel.classMethods = function(methods){
    // _.extend(this, methods)
    for (var name in methods){
        if (this[name] !== undefined){
            throw new Error('Falied to extend Model.classMethod. ' +
                this.prototype._name + '#' + name + '() already exists.');
        }
        this[name] = methods[name];
    }
};

/**
    AbstractModel.instanceMethods

        Extend methods as instance methods

    @methods    - object of methods
**/
AbstractModel.instanceMethods = function(methods){
    for (var name in methods){
        if (this.prototype[name] !== undefined){
            throw new Error('Falied to extend Model.instanceMethod. ' +
                this.prototype._name + '#' + name + '() already exists.');
        }
        this.prototype[name] = methods[name];
    }
};

/**
    AbstractModel.incr

        Increment a key.column counter this many times

    @key    - the key we want to work on
    @col    - the column counter
    @i      - the incr value (+/- ok); defaults to +1
    @callback
**/
AbstractModel.incr = function(key, col, i, callback) {
    var primary = this.primary();

    // Allow for optional increment count
    if (typeof(i) === 'function'){
        callback = i;
        i = 1;
    }

    i = i || 1;
    callback = callback || AbstractModel.nullCallback;

    var q = new CQL('incr counter');

    q.update(this.prototype._name);

    // set consitency...
    q.consistency(this.prototype._options.consistency.update)

    // todo: set ttl...

    var counter = {}; counter[col] = i;
    q.counter(counter);
    q.where(primary + '=:key', {key:key})

    this.cql(q.statement(), [], callback);
};

/**
    AbstractModel.decr

        Decrement a key.column counter (i) number of times
        This is just a wrapper for Model.incr and negates the i value

    @key    - the key we want to work on
    @col    - the column counter
    @i      - the incr value (+ num only); defaults to -1
    @callback

**/
AbstractModel.decr = function(key, col, i, callback) {
    // Allow for optional increment count
    if (typeof(i) === 'function'){
        callback = i;
        i = 1;
    }

    this.incr(key, col, -(i || 1), callback);
};

/**
    AbstractModel.prototype.cql

**/
AbstractModel.prototype.cql = function(qry, args, callback) {
    if (typeof args === 'function'){
        callback = args;
        args = [];
    }
    callback = callback || AbstractModel.nullCallback;
    this._client.execute(qry, args, callback);
};

/**
    AbstractModel.cql

        Execute a CQL query

    @qry        - cql string with optional replacement tokens (?)
    @args       - list of query arg tokens to replace
    @callback

**/
AbstractModel.cql = function(qry, args, callback) {
    if (typeof args === 'function'){
        callback = args;
        args = [];
    }
    callback = callback || AbstractModel.nullCallback;
    this.prototype._client.execute(qry, args, callback);
};


Casio.model = function(name, opts){

    // set up our default Model options
    _.defaults(opts, {

        host: '127.0.0.1',
        port: 9160,

        // todo: partition this different for reads vs writes
        //  see => http://wiki.apache.org/cassandra/API
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

    });

    // console.log('MODEL', name, opts)
    /**
        Model

            Constructor for the Model class

        @attrs   - the properties object to initialize
    **/
    var Model = function Model(attrs) {
        // console.log(attrs)

        // cache the attrs so we can check
        // for dirty values on upate...
        this._props = {};

        // should we eager load a relationship?
        this._eager = {};

        // placeholder for errors...
        this._errors = {}

        // seed defaults and copy attrs;
        var colDef;
        for (var p in this._schema){
            colDef = this._schema[p];
            this[p] = colDef.default;
            this._props[p] = colDef.default;

            // now update the instance with this property value
            if (attrs[p] !== undefined) {
                var val = attrs[p];

                switch(colDef.type){
                    case Boolean:
                        // coming from cassandra client these will be string buffers
                        if (Buffer.isBuffer(val)) {
                            val = (val[0] == 0) ? false : true;
                        // just-in-case someone sets a boolean with a 0 or 1
                        } else if(val.constructor === Number) {
                            if (val < 0 && val > 1) {
                                throw new Error('Tried setting a boolean with a value other then 0 or 1')
                            }
                            val = (val[0] == 0) ? false : true;
                        }
                        break;
                    case Casio.types.BigInteger:
                        // todo: figure what to with BigInteger values here...
                        val = parseInt(val.toString(), 0);
                        break
                    default:
                        break;
                }
                this[p] = val;
            }
        }

        // default all hasMany properties...
        for (var p in this._hasMany){
            this[p] = this._hasMany[p].default;
        }

        return this;
    };


    _.extend(Model, AbstractModel);
    _.extend(Model.prototype, AbstractModel.prototype);


    /**
        Model.primary

            Return the primary key column;
            caches a copy of the primary key name or null after first lookup
            returns: undefined or column name
    **/
    Model.primary = function(){

        if (this.prototype._primary !== undefined) {
            return this.prototype._primary;
        }

        var prop, primary=null;
        for (var p in this.prototype._schema) {
            prop = this.prototype._schema[p];
            if (prop.primary) {
                primary = p;
                break;
            }
        }

        // set this for later;
        this.prototype._primary = primary;
        return primary
    }

    /**
        Model.property

            define a prop with name as type with these options

        @p      - The name of the property to define
        @type   - String, Number, Date, etc.
        @opts   - options for this property type
    **/
    Model.property = function(p, type, opts){

      // don't allow defining a property with a name already existing on the class...
      if (this.prototype[p] !== undefined){
        throw new Error(this.prototype._name + '.' + p + ' already exists on this class. ' +
                        'Please choose a differnt name.')
      }

      opts = opts || {};
      opts.type = type;

      if (opts.default===undefined){
          switch(opts.type){
              case String:
              case Number:
              case Object:
              default:
                  _default = null;
                  break
          }
          opts.default = _default;
      }

      // its tempting to set notNull if we have a primary
      // but this should be explicit
      // because validation fires before the uuid kicks in.

      // set up validators
      if (opts.validators === undefined){
        opts.validators = [];
      } else {
        if (typeof(opts.validators) === 'function'){
          opts.validators = [opts.validators]
        }
      }
      if (opts.notNull){
        opts.validators.push(function(prop, val){
          if (val===null) this.error(prop, ':prop is null.');
        })
      }
      this.prototype._schema[p] = opts;
    }

    /**
        Model.belongsTo

            Define a belongsTo association as:
                A model.property is a primary key on another columnfamily

        @p      - the name of the has one property
        @type   - the class to instantiate as
        @opts   - the association options.
            {
                on:'association primary key column',
                fk:'the foreign key on the model'
            }

        Example:
        //todo: allow setting via: User.property('personId', Number, {has: 'person', as: Person});

        User.belongsTo('person', Person, {});

        By default, we set both 'on' and 'fk' to the defined Person.primary column name.
        So, there is no need to set these if they're are the same.

        User.belongsTo('person', Person, {
            on:'personId', // the Person column to associate
            fk:'personId'  // the User column to associate
        });

        You'll also want to define an additional Model.property for the 'personId'
        Otherwise, it won't be saved properly.

        User.property('personId', Number, {})

    **/
    Model.belongsTo = function(p, type, opts){
        opts = opts || {};
        opts.type = type;

        // must have opts.fk && opts.on defined
        // otherwise won't be able to eager load, etc.
        // default to the primary col name
        if (opts.on === undefined){
            opts.on = type.primary()
        }
        // default to the primary col name
        if (opts.fk === undefined){
            opts.fk = type.primary()
        }

        if (opts.default===undefined) {
            opts.default = null;
        }
        this.prototype._belongsTo[p] = opts;
    }

    /**
        Model.hasMany

            Define a hasMany association as:

                A list of objects where model.primary is a
                primary or secondary index on another columnfamily

        @p      - the name to define this list as
        @type   - the class to instantiate the list objects as
        @opts   - the options to configure for this association
            {
                on:'the primary or secondary index column'
            }

        Requires setting an 'on' value if its different then the model.primary.
        In addition, this column will also need an index.
        Since the CQL query looks something like:

            "select * from <columnfamily> where <key>=:key"

    **/
    Model.hasMany = function(p, type, opts){
        opts = opts || {};
        opts.type = type;


        // make sure we don't allow defining hasMany ModelArray;
        if (type.prototype.__type__ && type.prototype.__type__==='ModelArray'){
            throw new Error(this.prototype._name + '.' + p + ' error:' +
                            ' hasMany definitions using ModelArray aren\'t supported.' +
                            ' Try hasOne or belongsTo instead.')
        }

        // must have opts.fk && opts.on defined
        // otherwise won't be able to eager load, etc.
        // default to the primary col name
        if (opts.on === undefined){
            opts.on = Model.primary();
        }

        // default to the primary col name
        // if (opts.fk === undefined){
        //     opts.fk = type.primary()
        // }

        // defaults to empty list
        opts.default = [];
        this.prototype._hasMany[p] = opts;
    }

    /**
        Model.hasOne

            Define a hasOne association as
            model.primary is a primary or seconday index on another columnfamily

    **/
    Model.hasOne = function(p, type, opts){
        opts = opts || {};
        opts.type = type;

        if (opts.on === undefined){
            opts.on=Model.primary();
        }
        if (opts.default===undefined) {
            opts.default = null;
        }
        this.prototype._hasOne[p] = opts;
    }


    /**
        Model.prepareCqlArgs

            Massage a list of args for a CQL.where clause

        @args
    **/
    Model.prepareCqlArgs = function(args){
        var primary = this.primary();
        if (typeof(args) !== 'object'){
            // we should have a string here...
            args = {
                where:[primary + '=:key', {key:args}]
            }
        } else {
            // do we have a where clause?
            if (args.where === undefined){
                args.where = [primary + '=:key', {key:args[primary]}];
            }
        }
        if (args.columns !== undefined){
            if (typeof(args.columns)==='string'){
                args.columns = [args.columns];
            }
        }

        // don't automatically define this any more...
        // let the opts do this for now
        // else {
        //     args.columns = ['*'];
        // }

        return args
    }



    /**
        Model.find

            Take the args query plan and
            return them in the callback

        @args   - the cql args object
                {
                    // the columns to select for each row
                    columns:'*' || ['c1', 'c2', 'c3'],

                    // where clause to perform
                    where: ['key=:key', {key:'somthing'}]
                    -or-
                    where: ['key IN (:keys)', {keys:[1, 2, 3, 4, ...]}]
                    -or-
                    where: 'clause as a string'

                    // optional shallow class to instanitate rows as
                    as: ClassName

                }
        @callback

    **/
    Model.find = function(args, callback){
        // console.log(args)

        var q = new CQL('find');
        if (args.columns !== undefined){
            if (typeof(args.columns)==='string'){
                args.columns = [args.columns];
            }
        } else {
            args.columns = ['*'];
        }
        q.select(args.columns);
        q.from(this.prototype._name);

        if (args.where !== undefined){
            if (args.where instanceof Array){
                var clause = args.where[0];
                var bind = (args.where.length > 1) ? args.where[1] : {};
                q.where(clause, bind);
            } else if (args.where instanceof String){
                q.where(args.where, {});
            }
        }
        if (args.first !== undefined) {
            q.first(args.first);
        }
        if (args.limit !== undefined) {
            q.limit(args.limit);
        }

        q.consistency(this.prototype._options.consistency.select);
        var statement = q.statement();
        var self = this;
        this.cql(statement, [], function(err, results){
            if (err) return callback(err, null);
            var model, models=[];

            _.each(results, function(item){
                var props = item.colHash;
                props.key = item.key;

                // remove tombstoned rows...
                var found = true;
                if (args.columns.length > 1 || args.columns[0] === '*') {
                    if (item._colCount == 1 && item.cols[0].name === self.primary()){
                        found = false;
                    }
                }
                if (found){
                    // instantiate the model...
                    if (args.as === undefined){
                        model = new Model(props);
                        model.shadow();

                    } else {
                        model = new args.as(props);
                    }
                    models.push(model);
                }
            })
            callback(null, models)
        });
    }

    /**
        Model.get

            Return a single instance of a model.
            By default, the model is
                instantiated as this model
            Setting the '@args.as' value will overrride
                how results are transformed

        @args = key

            -or-

        @args = {} see Model.find for this pattern.
        @callback - [err, results]
    **/
    Model.get = function(args, callback){

        // grab the options
        var opts = this.prototype._options;

        // massage the args for a get statement
        args = this.prepareCqlArgs(args);

        // we didn't have columns here then look up the default options...
        if (args.columns===undefined){
            if (opts.get.columns){
                args.columns = opts.get.columns;
            } else if (opts.get.start || opts.get.end) {
                args.start = opts.get.start || '';
                args.end = opts.get.end || '';
            }
        }

        var q = new CQL('get');
        if (args.columns !== undefined){
            q.select(args.columns);
        } else if(args.start || args.end) {
            q.select();
            q.range(args.start, args.end);
        } else {
            throw new Error('Missing columns or a range for Model.get')
        }

        q.from(this.prototype._name);
        q.consistency(opts.consistency.select)

        if (args.where !== undefined){
            if (args.where instanceof Array){
                var clause = args.where[0];
                var bind = (args.where.length > 1) ? args.where[1] : {};
                q.where(clause, bind);
            } else if (args.where instanceof String){
                q.where(args.where, {});
            }
        }

        var statement = q.statement();
        var self = this;
        this.cql(statement, [], function(err, results) {
            if (err) return callback(err, null);
            var model=null;
            if (results.length) {
                var first = results[0];

                // we need to check if this item was marked for delete
                // the only way to determine is we'll only have on column
                // with a primary key because of the tombstone
                // { key: 'f23aeef7-8b95-4001-88a2-658a3df330e4',
                //   cols:
                //    [ { name: 'userId',
                //        value: 'f23aeef7-8b95-4001-88a2-658a3df330e4' } ],
                //   colHash: { userId: 'f23aeef7-8b95-4001-88a2-658a3df330e4' },
                //   _colCount: 1 }

                // checking for tombstones is rough...
                // depends on if the we selected columns or a range
                var found = true;
                if (args.columns && (args.columns.length > 1 || args.columns[0] === '*')) {
                    if (first._colCount == 1 && first.cols[0].name === self.primary()){
                        found = false;
                    }
                } else if (first._colCount == 0){
                    found = false;
                }

                // instantiate models?
                if (found){
                    var props = first.colHash;
                    props.key = first.key;

                    // we need to set the primary key column
                    // since it might be missing if we performed a range query.
                    props[Model.primary()] = first.key;

                    if (args.as === undefined){
                        model = new Model(props);
                        model.shadow();

                        // eager load a loadBelongsTo association
                        var loadBelongsTo = function loadBelongsTo(p) {
                            return function(next){
                                var colDef = Model.prototype._belongsTo[p];
                                var klass = colDef.type;
                                // console.log('HAS_ONE', p, colDef, props);
                                if (klass.prototype.__type__==='Model'){
                                    var getArgs = {};
                                    getArgs[colDef.on] = props[colDef.fk];
                                    // do we need to eager load the next level?
                                    if (_.keys(args.eager[p]).length){
                                        getArgs.eager = args.eager[p];
                                    }
                                    klass.get(getArgs, function(err, assoc){
                                        model[p] = assoc;
                                        next();
                                    });
                                } else if (klass.prototype.__type__==='ModelArray') {
                                    var getArgs = {};
                                    if (args.eager[p].cql !== undefined){
                                        _.extend(getArgs, args.eager[p].cql)
                                    }
                                    // console.log('BELONGTO MODELARRAY', args)
                                    // belongsTo ModelArray must use the ModelArray
                                    // primary key as its 'on'
                                    var assoc = new klass(props[colDef.on]);

                                    if (_.keys(getArgs).length){
                                        assoc.range(getArgs, function(err, results){
                                            model[p] = assoc;
                                            next();
                                        })
                                    } else {
                                        model[p] = assoc;
                                        next();
                                    }
                                }
                            }
                        }


                        var loadHasMany = function loadHasMany(p){
                            return function(next){
                                var colDef = Model.prototype._hasMany[p];
                                var klass = colDef.type;
                                // console.log('HAS_MANY', colDef)

                                if (klass.prototype.__type__==='Model'){

                                    var getArgs = {};
                                    getArgs[colDef.on] = props.key;

                                    // do we need to eager load the next level?
                                    if (_.keys(args.eager[p]).length){
                                        getArgs.eager = args.eager[p];
                                    }

                                    if (getArgs.eager.cql !== undefined){
                                        _.extend(getArgs, getArgs.eager.cql)
                                    }

                                    // console.log('HAS_MANY GETARGS', getArgs)
                                    klass.find(getArgs, function(err, manies){

                                        model[p] = manies;
                                        next();
                                    })
                                } else if (klass.prototype.__type__==='ModelArray') {
                                    throw new Error('Eager Loading for hasMany ModelArray isn\'t supported')
                                }
                            }

                        }


                        // eager load a loadHasOne association
                        var loadHasOne = function loadHasOne(p){
                            return function(next){
                                var colDef = Model.prototype._hasOne[p];
                                var klass = colDef.type;
                                if (klass.prototype.__type__==='Model'){

                                    // args for loading...
                                    var getArgs = {};
                                    getArgs[colDef.on] = props.key;

                                    // do we need to eager load the next level?
                                    if (_.keys(args.eager[p]).length){
                                        getArgs.eager = args.eager[p];
                                    }

                                    klass.get(getArgs, function(err, assoc){
                                        model[p] = assoc;
                                        next();
                                    })
                                } else if (klass.prototype.__type__==='ModelArray') {

                                    var getArgs = {};
                                    if (args.eager[p].cql !== undefined){
                                        _.extend(getArgs, args.eager[p].cql)
                                    }

                                    // the ModelArray must use the primary key as its 'on'
                                    var assoc = new klass(props.key);

                                    if (_.keys(getArgs).length){
                                        assoc.range(getArgs, function(err, results){
                                            model[p] = assoc;
                                            next();
                                        })
                                    } else {
                                        model[p] = assoc;
                                        next();
                                    }


                                }
                            }
                        }


                        // do we have an eager association to load?
                        var eagerOrder = [];
                        if (args.eager !== undefined){
                            for (var p in args.eager){
                                if (Model.prototype._hasOne[p] !== undefined) {
                                   eagerOrder.push(loadHasOne(p));
                               } else if (Model.prototype._hasMany[p] !== undefined) {
                                   eagerOrder.push(loadHasMany(p));
                                } else if (Model.prototype._belongsTo[p] !== undefined) {
                                    eagerOrder.push(loadBelongsTo(p));
                                }
                            }
                        }

                        async.series(eagerOrder, function(err, results){
                            callback(null, model);
                        })

                    } else {
                        // we're being told to not instantiate
                        // these props as new Model();
                        // instead as...
                        model = new args.as(props);
                        return callback(null, model);
                    }
                } else {
                    return callback(null, null);
                }
            } else {
                return callback(null, null);
            }

        });

    };


    /**
        Model.delete

            Delete the columns and primary keys passed by the args object

        @args       - see Model.find for this format
        @callback
    **/
    Model.delete = function(args, callback){

        var opts = this.prototype._options;

        // massage the args for a get statement
        args = this.prepareCqlArgs(args);

        // we didn't have columns here then look up the default options...
        if (args.columns===undefined){
            args.columns = opts.delete.columns;
        }

        // '*' columns aren't needed for delete statements
        if (args.columns.toString() === '*'){
            args.columns = [];
        }

        var q = new CQL('Model.delete');
        q.delete(args.columns);
        q.from(this.prototype._name);

        if (args.where !== undefined){
            if (args.where instanceof Array){
                var clause = args.where[0];
                var bind = (args.where.length > 1) ? args.where[1] : {};
                q.where(clause, bind);
            } else if (args.where instanceof String){
                q.where(args.where, {});
            }
        } // if no where delete everything??!!! scary

        // set consitency...
        q.consistency(opts.consistency.delete)

        // todo: set ttl...

        // set timestamp...
        q.timestamp(new Date().getTime());

        var statement = q.statement();
        // console.log(statement);

        this.cql(statement, [], function(err, results) {
            if (err) return callback(err, null);
            callback(null, {success:true});
        });

    }


    /**
        Model.count

            Return a count where the where clause passes some condition

        @where      - list in the form of ['key=:key', {key:value}] (optional)
        @callback
    **/
    Model.count = function(where, callback){
        // do we have a where clause?
        if (typeof(where)==='function'){
            callback=where;
            where=[];
        }
        callback = callback || function(err, results){};

        var q = new CQL('count');
        q.select(['count(*)']);     // columns
        q.from(this.prototype._name);
        q.where.apply(q, where)     // should skip empty values
        var statement = q.statement();
        this.cql(statement, [], function(err, results){
            if (err) return callback(err);
            callback(err, results[0].colHash);
        })
    }



    /**
        Model.createIndicies
            create indicies on all properties
            with index=true

        @callback
    **/
    Model.createIndicies = function(callback) {
        // loop over all schema properties and determine which ones are 'indexed'
        var prop, statements = [];
        for (var p in this.prototype._schema){
            prop = this.prototype._schema[p];
            if (prop.index !== undefined && prop.index) {
                // console.log('create index for this column:', p)

                var q = new CQL('create index for ' + this.prototype._name + '.' + p);
                q.createIndex(this.prototype._name, p);
                // statements.push(q.statement());

                this.cql(q.statement(), [], function(err, results){
                    if (err) return console.log(err);
                    if (results) console.log(results);
                })

            }
        }
    }

    _.extend(Model.prototype, {
        __type__:'Model',
        _name:null,
        _errors:null,
        _client:null,
        _clientConnected:null,
        _schema:{},
        _hasOne:{},
        _hasMany:{},
        _belongsTo:{},
        _options:{},
        _deleted:null,
        _created:null,
        _eager:null
    });
    Model.prototype._name = name;
    Model.prototype._options = opts;

    /**
        Model.prototype.error
          Set propperty error or Get all property errors or entire error graph
        @p    - if only p then we return all the errors for this property
        @msg  - if passing p and msg we push the message onto the property

    **/
    Model.prototype.error = function(p, msg){
      if (p === undefined) return this._errors
      if (msg !== undefined){

        if (this._errors[p] === undefined){
          this._errors[p] = [];
        }
        this._errors[p].push(msg.replace(/:prop/g, p));
        return
      } else {
        return this._errors[p]
      }
    }

    /**
        Model.prototype.validate
          Loop over all properties and run their validators
          A property validator should push an error
          onto _errors by property as key

    **/
    Model.prototype.validate = function(){
      // reset the errors since we're validating
      this._errors = {};

      var self = this;
      var colDef;
      for (var p in this._schema){
        colDef = this._schema[p];
        // console.log('VALIDATORS', colDef.validators)
        if (colDef.validators !== undefined){
          _.each(colDef.validators, function(fn){
            // we need to bind this to the the object
              fn.call(self, p, self[p]);
          })
        }
      }

      return (_.keys(this._errors).length) ? false : true;
    }

    /**
        Model.prototype.timestamp

            timestamp the column;
        @col    - the col to timestamp (mostly used for updatedat and createdat columns)
    **/
    Model.prototype.timestamp = function(col){
        for (var p in this._schema){
            if (p.replace(/[^a-zA-Z0-9]/g,'').toLowerCase() === col){
                this[p] = new Date();
                return p;
            }
        }
        return null;
    }

    /**
        Model.prototype.deleted

            Check to see if this model was deleted

    **/
    Model.prototype.deleted = function(){
        return this._deleted;
    }

    /**
        Model.prototype.created

            Check to see if this model was created

    **/
    Model.prototype.created = function(){
        return this._created;
    }

    /**
        Model.prototype.shadow
            refresh all model._props with current values
    **/
    Model.prototype.shadow = function(){
        for (var p in this._schema){
            this._props[p] = this[p];
        }
    }

    /**
        Model.prototype.eager
            The relationship graph we should load

    **/
    Model.prototype.eager = function(graph){
        this._eager = graph
    }

    /**
        Model.prototype.set - batch set the passed args
            @args - object of properties to set
    **/
    Model.prototype.set = function(args){
        if (args === undefined) return
        for (var p in args){
            if (this._schema[p] !== undefined){
                this[p] = args[p];
            }
        }
    }

    /**
        Model.prototype.update - update all the passed args or dirty properties

    **/
    Model.prototype.update = function(args, callback){

        if (args !== undefined){
            var type = typeof(args);
            // are the args an object?
            if (type === 'object'){
                this.set(args);
            } else if (type === 'function') {
                callback=args;
            }
        }
        // test if callback exists
        callback = callback || function(err, results){}

        // does this validate?
        if (!this.validate()) {
          return callback(this._errors, {success:false});
        }

        // update the hasOne props...
        var colDef;
        for (var p in this._belongsTo) {
            colDef = this._belongsTo[p];
            // console.log(colDef)
            if (this[p] !== undefined){
                var belongsTo = this[p]
                // console.log(colDef.type.primary(), belongsTo)
                this[colDef.fk] = belongsTo[colDef.type.primary()]
            }
        }

        // determine all dirty schema properties...
        var dirty = {}, val;
        for (var p in this._schema){
            if (this[p] !== this._props[p]){
                val = this[p];
                if (val && val.constructor === Date){
                    val = val.getTime();
                }
                dirty[p] = val;
            }
        }

        // console.log('DIRTY', dirty);

        // we should have at least one dirty value
        if (!_.keys(dirty).length) {
            return callback(null, {success:false, msg:'nothing to update'});
        }

        // now set updated at
        // we may not have an updated at column
        var updatedAtCol = this.timestamp('updatedat');
        if (updatedAtCol) {
            dirty[updatedAtCol] = this[updatedAtCol].getTime();
        }
        // console.log('DIRTY', dirty);

        // create query plan to save dirty properties
        // create cql query
        var q = new CQL('create');
        q.update(this._name);

        // set set args...
        q.set(dirty);

        // set consitency...
        q.consistency(this._options.consistency.update)

        // todo: set ttl...

        // set timestamp...
        q.timestamp(new Date().getTime());

        // todo: figure out what to do for models without primary keys
        var primary = Model.primary();
        q.where(primary + '=:key', {key:this[primary]})

        var statement = q.statement();
        var self = this;
        this.cql(statement, [], function(err, results){
            if (err) return callback(err);

            // update self._props with saved values
            self.shadow();

            callback(null, {success: true});
        });

    };

    /**
        Model.prototype.create - Create the model...

            todo: determine how to handle 'default' values

            @callback
    **/
    Model.prototype.create = function(callback){

        // does this validate?
        if (!this.validate()) {
          return callback(this._errors, {success:false});
        }

        // does this model have a primary key defined column
        // and is it set to something?
        var primary = Model.primary();
        if (primary !== undefined && primary !== null){
            var primaryDefined = (this[primary]) ? true : false;
            // console.log("primary:", primary, 'defined:', primaryDefined)
            // we don't have a primary key so define one
            if (!primaryDefined) {
                var uuid = new Cassandra.UUID().toString();
                // console.log('generating uuid:', uuid )
                this[primary] = uuid
            }
        }

        // timestamp updatedat && createdat (remove all formatting)
        this.timestamp('createdat');
        this.timestamp('updatedat');

        var into = [],
            values = [],
            val,
            primaryVal,
            colDef;

        for(var p in this._schema){
            colDef = this._schema[p];
            val = this[p];
            if (colDef.type === Date){
                if (val && val.constructor === Date){
                    val = val.getTime();
                }
            }

            // dont save undef or null values
            if (val!==undefined && val!=null){
                if (colDef.primary === undefined){
                    into.push(p);
                    values.push(val);
                } else {
                    primaryVal = val;
                }

            }
        }

        if (primaryVal) {
            into.push(primary);
            values.push(primaryVal);
        }

        // create cql query
        var q = new CQL('create');
        q.insert(this._name);

        // make it so primary is the first key...
        q.into(into.reverse());
        q.values(values.reverse());

        // set consitency...
        q.consistency(this._options.consistency.insert);

        // todo: set ttl...

        // set timestamp...
        q.timestamp(new Date().getTime());

        var statement = q.statement();

        var self = this;
        this.cql(statement, [], function(err, results){
            if (err) return callback(err);

            // update self._props with saved values
            self.shadow()
            self._created = true;

            callback(null, {success:true});
        })
    };

    /**
        Model.prototype.save

            The only way we can accurately gauge if a model has been created:
              1. If it has a primary key
              2. If createdat is set...

            Essentially a fork for either calling
            Model.prototype.create or
            Model.prototype.update

    **/
    Model.prototype.save = function(callback){

        var primary = Model.primary();
        var createdat;
        for (var p in this._schema){
            if (p.replace(/[^a-zA-Z0-9]/g,'').toLowerCase() === 'createdat'){
                createdat = this[p]
                break;
            }
        }
        if (this[primary] !== undefined && createdat){
            this.update(callback);
        } else {
            this.create(callback);
        }
    };

    /**
        Model.prototype.incr - wrapper for Model.incr

    **/
    Model.prototype.incr = function(col, i, callback){
        Model.incr(this[Model.primary()], col, i, callback)
    };

    /**
        Model.prototype.decr - wrapper for Model.incr

    **/
    Model.prototype.decr = function(col, i, callback){
        Model.decr(this[Model.primary()], col, i, callback)
    };


    /**
        Model.prototype.delete

            Delete the model by its primary key

        @callback(err, results)
    **/
    Model.prototype.delete = function(callback){
        var self = this;
        Model.delete(this[Model.primary()], function(err, results){
            if (err) return callback(err);

            self._deleted = true;
            callback(null, {success:true})

        })
    };

    this.register(Model);

    return Model;
}



/////////////////////////////////////
// Casio Model Array
/////////////////////////////////////
Casio.array = function(name, opts){

    // set up our default Model options
    _.defaults(opts, {

        host: '127.0.0.1',
        port: 9160,


        // todo: partition this different for reads vs writes
        //  see => http://wiki.apache.org/cassandra/API
        consistency:{
            select:'ONE',
            insert:'ONE',
            update:'ONE',
            delete:'ONE'
        }
    });

    /**
        ModelArray

            Constructor for the Model class

        @key    - the key for this model

    **/
    var ModelArray = function ModelArray(key) {
        this._key = key || new Cassandra.UUID().toString();

        // so we have a primary key with an public property name
        this[this._primary]=this._key;

        this.reset();
        return this;
    };

    _.extend(ModelArray, AbstractModel);
    _.extend(ModelArray.prototype, AbstractModel.prototype);


    ModelArray.primary = function(name) {
        if (name === undefined) return this.prototype._primary;
        this.prototype._primary = name;
    };


    /**
        ModelArray.find

            Return a list of ModelArray's

    **/
    ModelArray.find = function(args, callback) {
        throw new Error('Stub')
    }


    /**
        ModelArray.range

            Multi-key range queries

    **/
    ModelArray.range = function(args, callback) {
        throw new Error('Stub')
    }

    /**
        ModelArray.delete

            Multi-key delete queries

        @args       -   see Model.find for this format
        @callback
    **/
    ModelArray.delete = function(args, callback){
        throw new Error('Stub')
    }

    /**
        ModelArray.cql

    **/
    ModelArray.cql = function(qry, args, callback) {

        if (typeof args === 'function'){
            callback = args
            args = [];
        }
        // console.log(qry)
        this.prototype._client.execute(qry, args, callback)
    }

    _.extend(ModelArray.prototype, {
        __type__:'ModelArray',
        _name:null,
        _client:null,
        _clientConnected:null,
        _options:{},
        _primary:'key',
        _rows:null,
        _hasNext:null,
        _hasPrev:null,
        _created:null,
        _deleted:null
    });
    ModelArray.prototype._name = name;
    ModelArray.prototype._options = opts;

    /**
         ModelArray.prototype.range

            @args {
                start: '0',
                end: '',
                first: 10,
                reversed: true,
                limit: 10 // does nothing for range queries (use first, instead)
            }

         todo: if the original range query was reversed
                we need to set
                prev -> next
                and
                next -> prev

    **/
    ModelArray.prototype.range = function(args, callback) {

        if (typeof args === 'function'){
            callback = args;
            args = {};
        }

        // do we have a start and end?
        args.start = args.start || '';
        args.end = args.end || '';

        var q = new CQL('ModelArray.prototype#range');
        q.select();
        q.range(args.start, args.end);
        q.from(this._name);
        q.where(this._primary + '=:key', {key:this._key});
        if (args.first !== undefined) {

            // +1 for the first range query
            if (args.next===undefined && args.prev === undefined){
                args.first++
            }
            q.first(args.first);
        }
        if (args.reversed !== undefined) {
            q.reversed(args.reversed);
        }
        if (args.limit !== undefined) {
            q.limit(args.limit);
        }
        q.consistency(this._options.consistency.select);
        var statement = q.statement();
        var self = this;
        this.cql(statement, [], function(err, results){
            if (err) return callback(err);
            if (results.length){
                var cols = results[0].cols;
                if (args.next===undefined && args.prev === undefined){

                    if (args.first !== undefined){
                        args.first--
                    }

                    self._hasNext = (cols.length > args.first) ? true : false;

                    if (args.first !== undefined){
                        self._rows = cols.slice(0, args.first);
                    } else {
                        self._rows = cols;
                    }
                    // console.log('ROWS', self._rows)

                } else {
                    if (args.next){
                        // clean up args
                        delete args.next
                        if (args.first !== undefined){
                            args.first--
                        }

                        self._hasNext = (cols.length > args.first) ? true : false;

                        // we need to append these to ._rows
                        // console.log('NEXT', cols.length)
                        // console.log('FIRST', cols[0])
                        // console.log('LAST', cols[cols.length-1])

                        // account for the plus +1 on args.first
                        self._rows = _.uniq(self._rows.concat(cols), true, function(row){
                            return row.name;
                        })

                        // console.log('ROWS', self._rows)
                        // console.log('ARGS', args)
                    } else if (args.prev) {
                        delete args.prev
                        delete args.reversed

                        if (args.first !== undefined){
                            args.first--
                        }

                        // if cols length is greater then first
                        // we have more for next time....
                        self._hasPrev = (cols.length > args.first) ? true : false;

                        // we need to preappend these ._rows
                        // console.log('PREV', cols.length);
                        // console.log('FIRST', cols[0]);
                        // console.log('LAST', cols[cols.length-1]);

                        cols = cols.reverse().slice(0, args.first);

                        self._rows = _.uniq(cols.concat(self._rows), true, function(row){
                            return row.name;
                        })

                        // console.log('ROWS', self._rows)
                        // console.log('ARGS', args)
                    }
                }
                self._args = args;
            }
            callback(null, null);
        });
    }

    /**
         ModelArray.prototype.rowCount

            Return the current length of our _rows property
    **/
    ModelArray.prototype.rowCount = function() {
         return this._rows.length;
    }

    /**
         ModelArray.prototype.rows

            Return all the rows on this object
    **/
    ModelArray.prototype.rows = function() {
         return this._rows;
    }

    /**
         ModelArray.prototype.row(name)

            Lookup a single row and return it if it exits...

        @name   <string> the name of the row.name to return
        -or-
        @name   <number> the name of the row index to return

    **/
    ModelArray.prototype.row = function(name) {
        if (name===undefined) return null;
        if (name.constructor === String){
            for (var i = 0, ii = this._rows.length, row; i<ii;i++){
                row = this._rows[i];
                if (row.name === name){
                    return row;
                }
            }
        } else if (name.constructor===Number){
             if (this._rows.length >= name){
                 return this._rows[name]
             }
        }
        return null;
    }


    // Underscore methods that we want to implement on the ModelArray.
    var methods = ['forEach', 'each', 'map', 'reduce', 'reduceRight', 'find', 'detect',
    'filter', 'select', 'reject', 'every', 'all', 'some', 'any', 'include',
    'contains', 'invoke', 'max', 'min', 'sortBy', 'sortedIndex', 'toArray', 'size',
    'first', 'rest', 'last', 'without', 'indexOf', 'lastIndexOf', 'isEmpty', 'groupBy'];

    // Mix in each Underscore method as a proxy to `ModelArray#_rows`.
    _.each(methods, function(method) {
        ModelArray.prototype[method] = function() {
          return _[method].apply(_, [this._rows].concat(_.toArray(arguments)));
        };
    });


    /**
         ModelArray.prototype.hasNext

            After searching for a range and then a next query
            is there a next one?

    **/
    ModelArray.prototype.hasNext = function() {
         return this._hasNext;
    }

    /**
         ModelArray.prototype.hasPrev

            After searching for a range and then a previous query
            is there another previous query?

    **/
    ModelArray.prototype.hasPrev = function() {
         return this._hasPrev;
    }

    /**
         ModelArray.prototype.reset
            Except for the key, reset most object properties

    **/
    ModelArray.prototype.reset = function() {
        this._rows = [];
        this._args = null;
        this._hasNext = false;
        this._hasPrev = false;
    }

    /**
        ModelArray.prototype.set

            set rows to be inserted as columns for the primary key
            prevserves all existing instance rows
            removes duplicates matching on row.name
            sort by row.name

        @rows - A single row object {name:'', value:''}
        -or-
        @rows - An array of row objects {name:'', value:''}

    **/
    ModelArray.prototype.set = function(rows) {

        if (rows.constructor === Object){
            rows = [rows];
        }

        // this need to remove current rows matching the row.name
        var temp = [];
        for (var i = 0, ii = this._rows.length, row; i<ii;i++){
            row = this._rows[i];
            _.each(rows, function(r){
                if (row.name === r.name){
                    row.name = null;
                }
            })
        }
        _.map(this._rows, function(row){
            if (row.name){
                temp.push(row)
            }
        })

        // concat and sort these now
        temp = temp.concat(rows).sort(function(a, b){
            return a.name - b.name;
        });
        this._rows = temp;
    }

    /**
        ModelArray.prototype.deleted

            Check to see if this model was deleted

    **/
    ModelArray.prototype.deleted = function(){
        return this._deleted;
    }

    /**
        ModelArray.prototype.created

            Check to see if this model was created

    **/
    ModelArray.prototype.created = function(){
        return this._created;
    }


    /**
         ModelArray.prototype.next

            return more columns for this primary key

        @args   - num of rows to return going forward since the end last row
        -or-
        @args   - range query object structure (see ModelArray.prototype.range())

    **/
    ModelArray.prototype.next = function(args, callback) {

        if (!this._args) {
            return callback(new Error('Must call range() before calling next()'));
        }
        if (args.constructor !== Object){
            if (args.constructor === Number){
                args = {first:args};
            }
        }

        // copy the new args over to the _args
        for (var p in args){
            this._args[p] = args[p];
        };

        // we need to determine the start and end values...
        var last = this._rows[this._rows.length-1];

        // if we weren't passed a start base it off the end row value.
        if (args.start === undefined) {
            this._args.start = last.name
        }

        // if we weren't passed a start base it off the end row value.
        if (args.end === undefined) {
            this._args.end = '';
        }

        // we need to bump first +1
        if (this._args.first !== undefined){
            this._args.first++;
        }

        this._args.next = true;

        // call the range query...
        this.range(this._args, callback);
    }

    /**
         ModelArray.prototype.prev

            return more columns for this primary key

        @args   - num of rows to return going backward from the first row
        -or-
        @args   - range query object structure (see ModelArray.prototype.range())

    **/
    ModelArray.prototype.prev = function(args, callback) {

        if (!this._args) {
            return callback(new Error('Must call range() before calling next()'));
        }
        if (args.constructor !== Object){
            if (args.constructor === Number){
                args = {
                    first:args,
                    reversed:true
                };
            }
        }

        // copy the new args over to the _args
        for (var p in args){
            this._args[p] = args[p];
        };

        // we need to determine the start and end values...
        var first = this._rows[0];
        if (args.start === undefined) {
            this._args.start = first.name;
        }
        if (args.end === undefined) {
            this._args.end = '';
        }
        // set reversed == true;
        if (args.reversed === undefined) {
            this._args.reversed = true;
        } else {
            //this._args.reversed
        }

        // we need to bump first +1
        if (this._args.first !== undefined){
            this._args.first++;
        }

        // so we know where to insert the results
        this._args.prev = true;

        // call the range query...
        this.range(this._args, callback);
    }

    /**
         ModelArray.prototype.create

    **/
    ModelArray.prototype.create = function(callback) {


        // create cql query
        var q = new CQL('ModelArray.prototype.create');
        q.insert(this._name);

        var into = [this._primary],
            values = [this._key];
        _.each(this._rows, function(row){
            into.push(row.name);
            values.push(row.value);
        })

        // set set args...
        q.into(into);
        q.values(values);

        // set consitency...
        q.consistency(this._options.consistency.insert)

        // todo: set ttl...

        // set timestamp...
        q.timestamp(new Date().getTime());

        var statement = q.statement();
        var self = this;
        this.cql(statement, [], function(err, results){
            if (err) return callback(err);
            self._created = true;
            callback(null, {success:true});
        })
    }

    /**
        ModelArray.prototype.update

            Update the passed rows in the columnfamily by primary key

        @args - see ModelArray.prototype.set for parameter options


    **/
    ModelArray.prototype.update = function(rows, callback) {

        if (typeof(rows) === 'function'){
            callback = rows;
        } else {
            this.set(rows);
        }
        callback = callback || function(err, results){};

        var q = new CQL('ModelArray.prototype.update');
        q.update(this._name);

        var args = {};
        _.each(this._rows, function(row){
            args[row.name] = row.value;
        })

        // set set args...
        q.set(args);
        q.where(this._primary + '=:key', {key:this._key})

        // set consitency...
        q.consistency(this._options.consistency.update)

        // todo: set ttl...

        // set timestamp...
        q.timestamp(new Date().getTime());
        var statement = q.statement();
        var self = this;

        this.cql(statement, [], function(err, results){
            if (err) return callback(err);
            self._update=true;
            callback(null, {success: true});
        });

    }

    /**
         ModelArray.prototype.delete

            Delete an an entire row by its key
            or a list of columns on key

        @rows   - optional rows to remove. (defaults to entire row)

    **/
    ModelArray.prototype.delete = function(columns, callback){

        if (typeof(columns) === 'function'){
            callback = columns;
            columns = [];
        }

        if (columns.constructor === String){
            columns = [columns]
        }

        callback = callback || function(err, results){};

        var q = new CQL('ModelArray.prototype.delete');
        q.delete(columns);
        q.from(this._name);
        q.where(this._primary + '=:key', {key:this._key});
        q.consistency(this._options.consistency.delete)

        // todo: set ttl...
        q.timestamp(new Date().getTime());

        var statement = q.statement();

        var self = this;
        this.cql(statement, [], function(err, results) {
            if (err) return callback(err, null);

            if (columns.length) {
                self._deleted = true;
            }
            callback(null, {success:true});
        });
    };

    /**
        Model.prototype.incr - wrapper for Model.incr

    **/
    ModelArray.prototype.incr = function(col, i, callback){
        ModelArray.incr(this[ModelArray.primary()], col, i, callback)
    };

    /**
        Model.prototype.decr - wrapper for Model.incr

    **/
    ModelArray.prototype.decr = function(col, i, callback){
        ModelArray.decr(this[ModelArray.primary()], col, i, callback)
    };


    this.register(ModelArray);

    return ModelArray;

}


//////////////////


/*
    use this to auto-detect column family members???
*/
// var System = require('cassandra-client').System;
// var sys = new System('127.0.0.1:9160');
// sys.describeKeyspace('demo', function(err, def) {
//     _.each(def.cf_defs, function(cf){
//
//         _.each(cf.column_metadata, function(meta){
//             console.log(meta.name.toString(), meta.validation_class);
//         })
//
//     })
// });

