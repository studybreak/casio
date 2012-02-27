var User = require('./user').User;
var Pet = require('./pet').Pet;

// we need to load this here otherwise User never loads.
Pet.belongsTo('owner', User, {
    on:'userId'
});

exports.User = User;
exports.Pet = Pet;
exports.UserShort = require('./user').UserShort;
exports.Person = require('./person').Person;
exports.Vote = require('./vote').Vote;
exports.Friends = require('./friends').Friends;
exports.Groups = require('./groups').Groups;