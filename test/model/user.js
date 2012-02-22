var Casio = require('../../').Casio;

var options = {

    host:'127.0.0.1', 
    port:9160, 
    keyspace:'casio',
    use_bigints: true,
    consistency:{
        select:'ONE',
        insert:'ONE',
        update:'ONE',
        delete:'ONE'
    }
}

var User = Casio.model('User', options);

User.connect(function(err, results){
    console.log('client connected');
    if (err) console.log(err);
    if (results) console.log(results);

});

User.property('userId', String, {
    primary:true
});

User.property('name', String, {});
User.property('first_name', String, {});
User.property('last_name', String, {});
User.property('email', String, {});
User.property('birthday', String, {});
User.property('gender', String, {});
User.property('visits', Number, {});
User.property('is_admin', Boolean, {
    default:false
});
User.property('created_at', Date, {});
User.property('updated_at', Date, {});

User.classMethods({
    something:function(){
        return "this is something;"
    }
});

User.instanceMethods({
    hello:function(){
        return 'Hello, ' + this.first_name + ' ' + this.last_name + ' (' + this.email + ')';
    }
});


//////////////////////////////////////
exports.User = User
//////////////////////////////////////
// short version of user
function UserShort(props){
    this._type = 'UserShort';
    
    for(p in props){
        this[p] = props[p];
    }
}

UserShort.prototype = {
    _type:null,
    first_name:null,
    last_name:null
}
UserShort.prototype.hello = function(){
    return 'Hello, ' + this.first_name + ' ' + this.last_name ;
}

//////////////////////////////////////
exports.UserShort = UserShort
//////////////////////////////////////