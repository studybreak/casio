var _ = require('underscore');

var belongsTo = exports.belongsTo = function belongsTo(colDef, model, p, args) {
    return function(next){
        var klass = colDef.type;
        if (klass.prototype.__type__==='Model'){
            var getArgs = {};
            getArgs[colDef.on] = model[colDef.fk];
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
                _.extend(getArgs, args.eager[p].cql);
            }
            // belongsTo ModelArray must use the ModelArray
            // primary key as its 'on'
            var assoc = new klass(model[colDef.on]);

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

var hasMany = exports.hasMany = function hasMany(colDef, model, p, args){
    return function(next){

        var klass = colDef.type;
        if (klass.prototype.__type__==='Model'){

            var getArgs = {};
            getArgs[colDef.on] = model[model._primary];

            // do we need to eager load the next level?
            if (_.keys(args.eager[p]).length){
                getArgs.eager = args.eager[p];
            }

            if (getArgs.eager.cql !== undefined){
                _.extend(getArgs, getArgs.eager.cql);
            }
            
            klass.find(getArgs, function(err, manies){
                model[p] = manies;
                next();
            })
        } else if (klass.prototype.__type__==='ModelArray') {
            throw new Error('Eager Loading for hasMany ModelArray isn\'t supported')
        }
    }
}

var hasOne = exports.hasOne = function hasOne(colDef, model, p, args){
    return function(next){
        var klass = colDef.type;
        if (klass.prototype.__type__==='Model'){

            // args for loading...
            var getArgs = {};
            getArgs[colDef.on] = model[model._primary];

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
            var assoc = new klass(model[model._primary]);
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