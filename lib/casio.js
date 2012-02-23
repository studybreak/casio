var util = require('util');
var Buffer = require('buffer').Buffer;

var _ = require('underscore');
var async = require('async');
var Cassandra = require('cassandra-client');

/*
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
        
    - Object (has one)
    - List  (has many)
        
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
    - make sure the primary key is always the first column in the cql statement

x - Model.prototype.update
    x - cql update statement gen

 - Model.prototype.delete
    x - cql delete statement gen
    x - mark the object as 'deleted'
    
- Slice/range queries

- Model TTL support

- Batch CQL queries
    
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
*/


var Casio = module.exports.Casio = {
    _models:{},
    types:{
        BigInteger: Cassandra.BigInteger
    }
};

Casio.register = function(model){
    this._models[model.prototype._name] = model
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
        }
    });
    
    /**
        Model

            Constructor for the Model class
            
        @attrs   - the properties object to initialize
    **/
    var Model = function(attrs) {
        // console.log(attrs)
        
        // cache the attrs so we can check 
        // for dirty values on upate...
        this._props = {};
        
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
        return this;
    }
    /**
    *
    *
    **/
    Model.connect = function(callback){
        callback = callback || function(err){};
        
        
        var self = this;
        this.prototype._client = new Cassandra.Connection(this.prototype._options);
        
        
        this.prototype._client.on('log', function(level, message, obj) {
          console.log('CLIENT %s -- %j', level, message);
        });        
        
        this.prototype._client.connect(function(err){
            if (err) return callback(err);

            self.prototype._clientConnected = true;
            
            console.log(self.prototype._name, 'client connected')
            
        });
    }

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
        this.prototype._schema[p] = opts;
    }

    /**
        Model.classMethods
    
            Extend class methods
            
        @methods    - object of methods
    **/
    Model.classMethods = function(methods){
        // _.extend(this, methods)
        for (var name in methods){
            if (this[name] !== undefined){
                throw new Error('Falied to extend Model.classMethod. ' +
                    this.prototype._name + '#' + name + '() already exists.');
            }
            this[name] = methods[name];
        }
        
    }

    /**
        Model.instanceMethods
            
            Extend methods as instance methods
        
        @methods    - object of methods
    **/
    Model.instanceMethods = function(methods){
        for (var name in methods){
            if (this.prototype[name] !== undefined){
                throw new Error('Falied to extend Model.instanceMethod. ' +
                    this.prototype._name + '#' + name + '() already exists.');
            }
            this.prototype[name] = methods[name];
        }
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
        } else {
            args.columns = ['*'];
        }
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
        if (args.start !== undefined) {
            q.first(args.start);
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

        // massage the args for a get statement
        args = this.prepareCqlArgs(args);

        var q = new CQL('get');
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
        var statement = q.statement();
        // console.log(statement);

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
                
                // do we have more then one column we asked for?
                var found = true;
                if (args.columns.length > 1 || args.columns[0] === '*') {
                    if (first._colCount == 1 && first.cols[0].name === self.primary()){
                        found = false;
                    }
                }

                // instantiate models?
                if (found){
                    var props = first.colHash;
                    props.key = first.key;

                    if (args.as === undefined){
                        model = new Model(props);
                        model.shadow();

                    } else {
                        // we're being told to not instantiate 
                        // these props as new Model();
                        // instead as...
                        model = new args.as(props);
                    }
                }

            }
            callback(null, model);
        });
        
    };


    /**
        Model.delete
            
            Delete the columns and primary keys passed by the args object

        @args       - see Model.find for this format
        @callback   
    **/    
    Model.delete = function(args, callback){

        // massage the args for a get statement
        args = this.prepareCqlArgs(args);
        
        // '*' columns aren't needed for delete statements
        if (args.columns.toString() === '*'){
            args.columns.pop()
        }
        
        var q = new CQL('delete');
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
        q.consistency(this.prototype._options.consistency.delete)
        
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
            
            create indicies on all defined properties 
            
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
            create indicies on all defined properties 
        
        @callback
    **/
    Model.createIndicies = function(callback) {
        // loop over all schema properties and determine which ones are 'indexed'
        var prop, statements = [];
        for (var p in this.prototype._schema){
            prop = this.prototype._schema[p];
            if (prop.index !== undefined && prop.index) {
                console.log('create index for this column:', p)

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


    /**
        Model.incr
            
            Increment a key.column counter this many times

        @key    - the key we want to work on
        @col    - the column counter
        @i      - the incr value (+/- ok); defaults to +1
        @callback 
    **/
    Model.incr = function(key, col, i, callback) {
        var primary = this.primary();
        var cb = function(err, results){};
        if (i===undefined){
            callback = cb;
            i = 1;
        } else if (typeof(i) === 'function'){
            callback = i;
            i = 1;
        }
        // make sure we have a callback 
        if (callback===undefined){
            callback=cb;
        }
        var q = new CQL('incr counter');

        q.update(this.prototype._name);

        // set consitency...
        q.consistency(this.prototype._options.consistency.update)
        
        // todo: set ttl...
        
        // set timestamp...
        q.timestamp(new Date().getTime());
        
        var counter = {}; counter[col] = i;
        q.counter(counter);
        q.where(primary + '=:key', {key:key})

        var qry = q.statement();
        this.cql(qry, [], function(err, results){
            if (err) return callback(err, null);
            callback(null, results);
        })
    }

    /**
        Model.decr

            Decrement a key.column counter (i) number of times
            This is just a wrapper for Model.incr and negates the i value
            
        @key    - the key we want to work on
        @col    - the column counter
        @i      - the incr value (+ num only); defaults to -1
        @callback 

    **/
    Model.decr = function(key, col, i, callback) {
        if (i===undefined){
            callback = function(err, results){}
            i = 1;
        } else if (typeof(i) === 'function'){
            callback = i;
            i = 1;
        } 
        this.incr(key, col, -(i), callback);
    }

    
    /**
        Model.cql

            Execute a CQL query
            
        @qry    - cql string with optional replacement tokens (?)
        @args   - list of query arg tokens to replace
    
    **/
    Model.cql = function(qry, args, callback) {
        
        if (typeof args === 'function'){
            callback = args
            args = [];
        }
        console.log(qry)
        this.prototype._client.execute(qry, args, callback)
    }
    
    Model.prototype = {
        _name:null,
        _client:null,
        _clientConnected:null,
        _schema:{},
        _options:{},
        _deleted:null,
        _created:null
    };
    Model.prototype._name = name;
    Model.prototype._options = opts;
    
    /**
        Model.prototype.timestamp - timestamp the column;
        
    **/
    Model.prototype.timestamp = function(col){
        for (var p in this._schema){
            if (p.replace(/_/g,'').toLowerCase() === col){
                this[p] = new Date();
                return p;
            }
        }
        return null;
    }
    
    /**
        Model.prototype.isDeleted
        
    **/
    Model.prototype.isDeleted = function(){
        return this._deleted;
    }

    /**
        Model.prototype.isCreated
        
    **/
    Model.prototype.isCreated = function(){
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
        
        // determine all dirty properties...
        var dirty = {}, val;
        for (var p in this._schema){
            if (this[p] !== this._props[p]){
                val = this[p];
                if (val.constructor === Date){
                    val = val.getTime();
                }
                dirty[p] = val;
            }
        }

        // we should have at least one dirty value
        if (!_.keys(dirty).length) return callback(null, {success:false, msg:'nothing to update'});
        

        // now set updated at
        // we may not have an updated at column
        var updatedAtCol = this.timestamp('updatedat');
        if (updatedAtCol) {
            dirty[updatedAtCol] = this[updatedAtCol].getTime();
        }
        console.log('DIRTY', dirty);

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
        console.log(statement);
        
        var self = this;
        this._client.execute(statement, [], function(err, results){
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

        
        // does this model have a primary key defined column
        // and is it set to something?
        var primary = Model.primary();
        if (primary !== undefined && primary !== null){
            var primaryDefined = (this[primary]) ? true : false;
            // console.log("primary:", primary, 'defined:', primaryDefined)
            // we don't have a primary key so define one
            if (!primaryDefined) {
                var uuid = new Cassandra.UUID().toString();
                console.log('generating uuid:', uuid )
                this[primary] = uuid
            }
        }
        
        // timestamp updatedat && createdat (remove all formatting)
        this.timestamp('createdat');
        this.timestamp('updatedat');
        
        var args = {}, 
            val, 
            colDef;
            
        for(var p in this._schema){
            colDef = this._schema[p]
            val = this[p];
            if (colDef.type === Date){
                if (val.constructor === Date){
                    val = val.getTime();
                }
            }
            args[p] = val;                
        }
        
        // create cql query
        var q = new CQL('create');
        q.insert(this._name);        
        
        // set set args...
        q.set(args);

        // set consitency...
        q.consistency(this._options.consistency.insert)
        
        // todo: set ttl...

        // set timestamp...
        q.timestamp(new Date().getTime());

        var statement = q.statement();
        console.log(statement);
        
        var self = this;
        this._client.execute(statement, [], function(err, results){
            if (err) return callback(err);
            
            // update self._props with saved values
            self.shadow()
            self._created = true;
            
            callback(null, {success:true});
        })
    };
    
    /**
        Model.prototype.save
        
    **/
    Model.prototype.save = function(callback){
        // check to see if this item has a primary key...
        // if yes and its null, create a uuid of some kind, and call update.
        // if yes and its not null, call update
        callback(null, null)
    };

    /**
    *
    *
    **/
    Model.prototype.delete = function(callback){
        var self = this;
        Model.delete(this[Model.primary()], function(err, results){
            if (err) return callback(err);
            
            self._deleted = true;
            callback(null, {success:true})

        })
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
    
    this.register(Model);
    
    return Model;
}

// escape all single-quotes on strings...
function quote(s){
    if (typeof(s) === 'string') {
        if (s.indexOf("\\'") == -1){
            s = s.replace(/'/g, "\\'");
        }
        s = "'" + s + "'";
    } else if (s instanceof Array) {
        var temp = [];
        _.each(s, function(v){
            temp.push(quote(v));
        })
        s = temp.join(', ');
    }
    
    return s;
}

var CQL = function(name){
    this.name = name;
}

CQL.prototype = {
    _name:null,
    _cmd:null,
    _first:null,
    _limit:null,
    _reversed:null,
    _cf:null,
    _col:null,
    _cols:null,
    _consistency:null,
    _where:null,
    _set:null,
    _ttl:null,
    _timestamp:null,
    _indexName:null,
    _counter:null,
}

CQL.prototype.first = function(num){
    this._first = num;
}
CQL.prototype.limit = function(num){
    this._limit = num;
}
CQL.prototype.reversed = function(b){
    this._reversed = b;
}

CQL.prototype.from = function(cf){
    this._cf = cf;
}

CQL.prototype.set = function(args){
    this._set = args;
}

CQL.prototype.counter = function(args){
    this._counter = args;
}

CQL.prototype.ttl = function(num){
    this._ttl = num;
}

CQL.prototype.timestamp = function(num){
    this._timestamp = num;
}

CQL.prototype.consistency = function(s){
    /*
    ZERO
    ONE
    QUORUM
    ALL
    DCQUORUM
    DCQUORUMSYNC
    */
    this._consistency = s;
}

CQL.prototype.where = function(s, args){
    if (args !== undefined){
        var v
        for (var p in args){
            v = args[p];
            s = s.replace(new RegExp(':' + p), quote(v))
        }
    }
    this._where = s;
}

CQL.prototype.select = function(cols){
    this._cmd = 'SELECT';
    if (cols instanceof String){
        cols = [cols];
    }
    this._cols = cols;
}

CQL.prototype.update = function(cf){
    this._cmd = 'UPDATE';
    this._cf = cf;
}

CQL.prototype.insert = function(cf){
    this._cmd = 'INSERT';
    this._cf = cf;    
}

CQL.prototype.delete = function(cols){
    this.select(cols);
    this._cmd = 'DELETE';
}

CQL.prototype.createIndex = function(cf, col){
    this._cmd = 'CREATE INDEX';
    this._indexName = cf + '_' + col;
    this._cf = cf;    
    this._col = col;    
}


CQL.prototype.count = function(){
    
}

CQL.prototype.statement = function(){
    var cql = []
    var cmd = this._cmd;

    cql.push(cmd);
    
    if (cmd === 'SELECT') {
        // SELECT [FIRST N] [REVERSED] <SELECT EXPR> FROM <COLUMN FAMILY> [consistency <CONSISTENCY>] [WHERE <CLAUSE>] [LIMIT N];
        
        if (this._first) {
            cql.push('FIRST');
            cql.push(this._first);
        }
        if (this._reversed) {
            cql.push('REVERSED');
        }

        cql.push(this._cols.join(', '));

        if (this._cf){
            cql.push('FROM');
            cql.push(this._cf)
        }

        if (this._consistency){
            cql.push('USING CONSISTENCY');
            cql.push(this._consistency)
        }

        if (this._where){
            cql.push('WHERE');
            cql.push(this._where)
        }
        
        if (this._limit){
            cql.push('LIMIT');
            cql.push(this._limit)
        }
    } else if (cmd === 'INSERT') {
        // INSERT INTO (KEY, v1, v2, …) VALUES (, , ,) [consistency CONSISTENCY [AND TIMESTAMP ] [AND TTL]];

        var values = []; into=[];
        for (var p in this._set){
            into.push(p);
            values.push(quote(this._set[p]));
        }
        cql.push('INTO');
        cql.push(this._cf);
        cql.push('(' + into.join(', ') + ')');

        cql.push('VALUES');
        cql.push('(' + values.join(', ') + ')');
        
        var using = [];
        if (this._consistency){
            using.push('CONSISTENCY ' + this._consistency);
        }

        if (this._timestamp){
            using.push('TIMESTAMP ' + this._timestamp);
        }
        if (this._ttl){
            using.push('TTL ' + this._ttl);
        }
        if (using.length){
            cql.push('USING')
            cql.push(using.join(' AND '));
        }


    } else if (cmd === 'UPDATE') {

        // UPDATE <COLUMN FAMILY>  
        //  [consistency <CONSISTENCY> [AND TIMESTAMP <timestamp>] [AND TTL <timeToLive>]] 
        //  SET name1 = value1, name2 = value2 WHERE KEY = keyname;
        
        cql.push(this._cf);

        var using = [];
        if (this._consistency){
            using.push('CONSISTENCY ' + this._consistency);
        }

        if (this._timestamp){
            using.push('TIMESTAMP ' + this._timestamp);
        }
        if (this._ttl){
            using.push('TTL ' + this._ttl);
        }
        if (using.length){
            cql.push('USING')
            cql.push(using.join(' AND '));
        }
        cql.push('SET');
        
        var values = [];
        for (var p in this._set){
            values.push(p + '=' + quote(this._set[p]));
        }
        for (var p in this._counter){
            var num = this._counter[p];
            var dir = (num > -1) ? '+' : ''; // ignore negative number dir
            values.push(p + '=' + p + dir + num);
        }
        cql.push(values.join(', '))
        
        if (this._where){
            cql.push('WHERE');
            cql.push(this._where)
        }        
    } else if (cmd==='DELETE'){
        // DELETE [COLUMNS] FROM <COLUMN FAMILY> [consistency <CONSISTENCY>] WHERE KEY = keyname1 
        // DELETE [COLUMNS] FROM <COLUMN FAMILY> [consistency <CONSISTENCY>] WHERE KEY IN (keyname1, keyname2);
        cql.push(this._cols.join(', '));

        if (this._cf){
            cql.push('FROM');
            cql.push(this._cf)
        }

        var using = [];
        if (this._consistency){
            using.push('CONSISTENCY ' + this._consistency);
        }

        if (this._timestamp){
            using.push('TIMESTAMP ' + this._timestamp);
        }
        if (using.length){
            cql.push('USING')
            cql.push(using.join(' AND '));
        }

        if (this._where){
            cql.push('WHERE');
            cql.push(this._where)
        }
        
    } else if (cmd==='CREATE INDEX'){
        // CREATE INDEX [index_name] ON <column_family> (column_name);
        cql.push(this._indexName);
        cql.push('ON');
        cql.push(this._cf);
        cql.push('(' + this._col + ')');
        
    }
    
    // BEGIN BATCH [consistency] 
    //  UPDATE CF1 SET name1 = value1, name2 = value2 WHERE KEY = keyname1; 
    //  UPDATE CF1 SET name3 = value3 WHERE KEY = keyname2; 
    //  UPDATE CF2 SET name4 = value4, name5 = value5 WHERE KEY = keyname3; 
    // APPLY BATCH

    return cql.join(' ');
}

//////// SELECT

// var q = new CQL('selecting something');
// 
// q.select(['first_name', 'birth_year'])
// q.from('users');
// q.first(10);
// q.reversed(true);
// q.limit(10);
// 
// q.where('full_name = :full_name', {full_name:"Greg Melton's"})
// 
// console.log(q.statement());

//////// UPDATE

// var q = new CQL('update something');
// 
// q.update('users');
// q.consistency('QUORUM');
// q.timestamp(123345);
// q.ttl(86400);
// q.set({full_name:'Willard Hill'});
// q.where('KEY=:key', {key:1})
// 
// console.log(q.statement());
// 
// 
// var q = new CQL('update something2');
// 
// q.update('users');
// q.consistency('QUORUM');
// q.timestamp(123345);
// q.ttl(86400);
// q.set({full_name:'Pitty da Fool'});
// q.where('KEY IN (:keys)', {keys:['1', '2', '3', '4']})
// 
// console.log(q.statement());

//////// INSERT

// var q = new CQL('insert something');
// 
// q.insert('users');
// q.set({key:2, full_name:'Sam the Butcher', birth_year:2000});
// q.consistency('QUORUM');
// q.timestamp(123345);
// q.ttl(86400);
// 
// console.log(q.statement());

//////// DELETE

// var q = new CQL('insert something');
// 
// q.delete(['*']); // columns
// q.from('users');
// q.where('KEY=:key', {key:1})
// q.consistency('QUORUM');
// q.timestamp(123345);
// console.log(q.statement());
// 
// var q = new CQL('delete something');
// q.delete(['first_name', '']); // columns
// q.from('users');
// q.where('KEY IN (:keys)', {keys:[1,2,3,4]})
// q.consistency('QUORUM');
// q.timestamp(123345);
// console.log(q.statement());


//////// CREATE INDEX

// var q = new CQL('create index');
// q.createIndex('users', 'birth_year');
// console.log(q.statement());




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

