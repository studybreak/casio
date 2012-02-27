var User = exports.User = require('./user').User;

exports.UserShort = require('./user').UserShort;
exports.Person = require('./person').Person;

// we need to load this here otherwise User never loads.
var Pet = require('./pet').Pet;
Pet.hasOne('owner', User, {
    fk:'userId',    
    on:'userId'
});
exports.Pet = Pet;

exports.Vote = require('./vote').Vote;
exports.Friends = require('./friends').Friends;