var _ = require('underscore')


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
    this._queries=[];
}

CQL.prototype = {
    name:null,
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
    _range:null,
    _queries:null,
}

CQL.prototype.first = function(num){
    this._first = num;
    return this;
}
CQL.prototype.limit = function(num){
    this._limit = num;
    return this;
}
CQL.prototype.reversed = function(b){
    this._reversed = b;
    return this;
}

CQL.prototype.from = function(cf){
    this._cf = cf;
    return this;
}

CQL.prototype.set = function(args){
    this._set = args;
    return this;
}

CQL.prototype.into = function(into){
    // this._into = _.invoke(into, quote);
    this._into = _.map(into, function(v){
      return quote(v);
    });
    return this;
}

CQL.prototype.values = function(values){
    // this._values = _.invoke(values, quote);
    this._values = _.map(values, function(v){
      return quote(v);
    });

    return this;
}

CQL.prototype.counter = function(args){
    this._counter = args;
    return this;
}

CQL.prototype.range = function(start, end){
    start = quote(start);
    end = quote(end);
    this._cols = [[start, end].join('..')]
    return this;
}

CQL.prototype.ttl = function(num){
    this._ttl = num;
    return this;
}

CQL.prototype.timestamp = function(num){
    this._timestamp = num;
    return this;
}

CQL.prototype.query = function(cql){
    if (cql instanceof CQL){
        this._queries.push(cql.statement());
    } else if(cql.constructor===String){
        this._queries.push(cql);
    }
    return this;
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
    return this;
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
    return this;
}

CQL.prototype.select = function(cols){
    this._cmd = 'SELECT';
    if (cols instanceof String){
        cols = [cols];
    }
    this._cols = cols;
    return this;
}

CQL.prototype.update = function(cf){
    this._cmd = 'UPDATE';
    this._cf = cf;
    return this;
}

CQL.prototype.insert = function(cf){
    this._cmd = 'INSERT';
    this._cf = cf;
    return this;
}

CQL.prototype.delete = function(cols){
    this.select(cols);
    this._cmd = 'DELETE';
    return this;
}

CQL.prototype.batch = function(c){
    this._cmd = 'BEGIN BATCH';
    if(c !== undefined){
        this.consistency(c);
    }
    return this;
}

CQL.prototype.createIndex = function(cf, col){
    this._cmd = 'CREATE INDEX';
    this._indexName = cf + '_' + col;
    this._cf = cf;
    this._col = col;
    return this;
}


CQL.prototype.count = function(){

    return this;
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

        cql.push('INTO');
        cql.push(this._cf);
        cql.push('(' + this._into.join(', ') + ')');

        cql.push('VALUES');
        cql.push('(' + this._values.join(', ') + ')');

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
            values.push(quote(p) + '=' + quote(this._set[p]));
        }
        for (var p in this._counter){
            var num = this._counter[p];
            var dir = (num > -1) ? '+' : ''; // ignore negative number dir
            values.push(quote(p) + '=' + quote(p) + dir + num);
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

    } else if (cmd==='BEGIN BATCH'){

        var using = [];
        if (this._consistency){
            using.push('CONSISTENCY ' + this._consistency);
        }

        if (this._timestamp){
            using.push('TIMESTAMP ' + this._timestamp);
        }
        if (using.length){
            cql[0] += ' USING ' + using.join(' AND ');
        }
        // cql[0] += '\n';
        // BEGIN BATCH [consistency]
        //  UPDATE CF1 SET name1 = value1, name2 = value2 WHERE KEY = keyname1;
        //  UPDATE CF1 SET name3 = value3 WHERE KEY = keyname2;
        //  UPDATE CF2 SET name4 = value4, name5 = value5 WHERE KEY = keyname3;
        // APPLY BATCH

        this._queries.push('')
        cql.push(this._queries.join(';  '))
        cql.push('APPLY BATCH')
    }


    return cql.join(' ');
}

exports.CQL = CQL;

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
//
//
// var q = new CQL('selecting something');
// q.select();
// q.range('0', 'Z');
// q.from('users');
// q.where('key = :key', {key:1})
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
// q.insert('users');
// q.into(['key', 'full_name', 'birth_year'])
// q.values(['2', 'Sam the Butcher', 2000])
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


//////// BATCH


// var q1 = new CQL('q1');
// q1.select(['*'])
// q1.from('User')
//
// var q2 = new CQL('q2');
// q2.select(['*'])
// q2.from('Pet')
//
// var cql = new CQL('batch');
// cql.batch();
// cql.query(q1);
// cql.query(q2);
// console.log(cql.statement());




