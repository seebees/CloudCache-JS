
var crypto          = require('crypto'),
    EventEmitter    = require('events').EventEmitter,
    util            = require('util'),
    flow            = require('flow'),
    /**
     * A function that will return an Date string formated
     * ISO 8601 
     */
    timestamp = (function () {
        var pad = function (n) {
            return n < 10 ? '0' + String(n) : String(n);
        };
        return function (d) {
            d = d && d instanceof Date ? d : new Date();
            return [d.getUTCFullYear(),     '-',
                  pad(d.getUTCMonth() + 1), '-',
                  pad(d.getUTCDate()),      'T',
                  pad(d.getUTCHours()),     ':',
                  pad(d.getUTCMinutes()),   ':',
                  pad(d.getUTCSeconds()),   'Z'].join('');
        };
    }()),
    /**
     * Nice function to apply the properties of source to target
     */
    mixin     = function (target, source) {
        target = target || {};
        Object.keys(source).forEach(function (key) {
            target[key] = source[key];
        });
      
        return target;
    },
    inherit = function(parent, child, prototype ) {
        var i;
        if ( typeof child !== 'function' ) {
            if (typeof child === 'object' ) {
                prototype = child;
            }
            child = function () {
                parent.apply(this, arguments);
            };
        }
        util.inherits(child, parent);
        child.prototype = mixin(child.prototype, prototype);
        
        return child;
    };

//TODO write a rational if statement
var debug, inspect,
    isDebug = false;
if (isDebug) { 
    debug = function (x) { console.log('CC: %s', x); };
    inspect = util.inspect;
} else {
    debug = function () {};
    inspect = function () { return ''; };
}


//TODO should I implement Connection:keep-alive?
//TODO should I implement Content-length or Buffer for calling data
//TODO should I emit an event for each ID?

var restricted_ids = {
    'auth'      : 1,
    'list'      : 1,
    'listkeys'  : 1,
    'myusage'   : 1,
    'getmulti'  : 1,
    'flush'     : 1
};
    
/*
    Base Class
*/
var CloudCache = inherit(
    EventEmitter,                     
    function (a_key, s_key, service_url) {
        var key     = a_key,
            secret  = s_key,
            url     = service_url || 'http://cloudcache.ws',
            self    = this;
        
        EventEmitter.call(self);
        
        self._REST = new flow.Service({
            options : {
                uri     : url,
                headers : {
                    'user-agent'    : 'CloudCace node.JS Client'
                },
                uri     : url,
                'auth'  : function () {
                    var ts = timestamp(),
                        sig = crypto.createHmac('sha1', secret).update(
                            'CloudCache' + this.options.cmd + ts
                        ).digest('base64');
                        
                    this.headers({
                        'Akey'      : key,
                        'Timestamp' : ts,
                        'Signature' : sig
                    });
                },
                request : false,
                '404OK' : true
            },
            'cmd'   : function (cmd) {
                if (cmd) {
                    this.options.cmd = cmd;
                    if (restricted_ids.hasOwnProperty(cmd)) {
                        this.path(cmd);
                    }
                }
                
                return this;
            },
            'id'    : function (id) {
                if (id) {
                    if (!restricted_ids.hasOwnProperty(id)) {
                        this.path(id);
                        this.options.id = id;
                    } else {
                        throw new TypeError(id + ' is not an allowed id in CloudCache');
                    }
                } else {
                    throw new TypeError('Request to CloudCache must have an id');
                }
                
                return this;
            },
            'ttl'   : function (ttl) {
                if (!isNaN(ttl)) {
                    this.header('ttl', parseInt(ttl, 10));
                }
                
                return this;
            },
            'callback'  : function (callback) {
                if (typeof callback === 'function') {
                    this.options.callback = callback;
                    this.on('error', function (ex) {
                        this.options.callback(ex);
                    });
                } else if (callback) {
                    throw new TypeError('A callback must be a function');
                }
                
                return this;
            },
            'success'   : function (ret) {
                this.emit('success', ret);
                if (typeof this.options.callback === 'function') {
                    this.options.callback(null, ret);
                }
                if (this.options.id) {
                    self.emit(
                        this.options.id,
                        this.options.cmd.toLowerCase(),
                        ret
                    );
                }
            }
        });
    },
    {
        auth : function (callback) {
            return this._REST.
                get().
                cmd('auth').
                callback(callback).
                on('end', function (data) {
                    this.success(!!data);
                }).
                end();
        },
        put : function (id, data, options) {
            var self = this;
            
            debug('PUT on id:' + id + ' data:\r\n' + data);
            
            if (typeof options === 'function') {
                options = {
                    callback : options
                };
            }
            
            options             = options           || {};
            options.encoding    = options.encoding  || 'utf-8';
            if (typeof arguments[3] === 'function') {
                options.callback = arguments[3];
            }
            
            var request = self._REST.
                put().
                cmd('PUT').
                id(id).
                ttl(options.ttl).
                encoding(options.encoding).
                callback(options.callback).
                on('end', function (data) {
                    this.success(!!data);
                }).
                request();
                
            //TODO should I JSON encode ALL data so I get the right type back on primitives?
            var data_type = typeof data;
            if ( data_type === 'string' || Buffer.isBuffer(data) ) {
                request.write(data);
            } else {
                if ( data_type === 'number' ) {
                    request.write(String(data));
                } else if ( data_type === 'boolean' ) {
                    request.write(data ? '1' : '0');
                } else if ( data_type === 'function' ) {
                    throw new TypeError('You can not create an item in CloudCache that is a function');
                } else {
                    request.write(JSON.stringify(data))
                }
            }
            
            if (!options.stream) {
                request.end();
            }
            
            return request;
        },
        get : (function () {
            //Hoist out the RegEx so we don't have to create it ever time
            var multi_split = /(VALUE|\r\nVALUE) (.+?) (.+?)\r\n|\r\nEND\r\n$/;
            
            return function get (id, options) {
                debug('GET id:' + id);
                
                if (typeof options === 'function') {
                    options = {
                        callback : options
                    };
                }
            
                options = options           || {};
                options.encoding = options.encoding || 'utf-8';
                
                if (typeof arguments[2] === 'function') {
                    options.callback = arguments[2];
                }
                
                var request = this._REST.
                    get().
                    cmd('GET').
                    encoding(options.encoding || 'utf-8').
                    callback(options.callback);
                
                var typeofID = typeof id;
                if (typeofID === 'string' || typeofID === 'number') {
                    //TODO option.streamResponse
                    return request.
                        id(id).
                        on('end', request.success).
                        end();
                } else if (typeofID === 'object') {
                    var ids = Array.isArray(id) ? id : Object.keys(id);
                    var self = this;
                     
                    return request.
                        path('getmulti').
                        header('keys', JSON.stringify(ids)).
                        on('end', function (data_string) {
                            var data = data_string.split(multi_split);
                            
                            data.shift();
                            var out = {};
                            var i;
                            var l = data.length;
                            for (i = 3; i < l; i += 4) {
                                if (data[i - 2]) {
                                    out[data[i - 2]] = data[i];
                                }
                            }
                            
                            this.success(out);
                            
                            var id;
                            for (id in out) {
                                if (out.hasOwnProperty(id)) {
                                    self.emit(id, 'get', out[id]);
                                }
                            }
                        }).
                        end();
                } else {
                    //How would I even get here?
                    throw new TypeError('You can not get an item in CloudCache with an ID of that type');
                }
            };
        }()),
        del : function (id, callback) {
            return this._REST.
                del().
                cmd('DELETE').
                id(id).
                on('end', function (data) {
                    /*
                      CloudCache returns the string "$id was deleted from CloudCache"
                      for a successful delete and 404 for a "successful" delete of 
                      an id that does not exists
                      Therefore we return true for actual deletes and false for
                      deletes that are not there
                    */
                    var ret = data !== null ? !!data : null;
                    
                    this.success(ret);
                }).
                callback(callback).
                end();
        },
        exists : function (id, callback) {
            var self = this;
            return this._REST.
                get().
                cmd('GET').
                id(id).
                callback(callback).
                on('404', function () {
                    this.emit('success', false);
                    if (typeof this.options.callback === 'function') {
                        this.options.callback(null, false);
                    }
                    if (this.options.id) {
                        self.emit(id, 'exits', false);
                    }
                }).
                on('2XX', function (data) {
                    this.emit('success', true, data);
                    if (typeof this.options.callback === 'function') {
                        this.options.callback(null, true, data);
                    }
                    if (this.options.id) {
                        self.emit(id, 'exits', true, data);
                    }
                }).
                end();
        },
        list_keys : function (callback) {
            return this._REST.
                get().
                cmd('listkeys').
                on('end', function (data) {
                    this.success(JSON.parse(data));
                }).
                callback(callback).
                end();
        },
        list : function (callback) {
            return this._REST.
                get().
                cmd('list').
                on('end', function (data) {
                    this.success(JSON.parse(data));
                }).
                callback(callback).
                end();
        },
        incr : function (id, options) {
            if (typeof options === 'function') {
                options = {
                    callback : options
                };
            } else if (!isNaN(options)) {
                options = {
                    amount : options
                };
            }
            
            options         = options        || {};
            options.amount  = options.amount || 1;
            if (typeof arguments[2] === 'function') {
                options.callback = arguments[2];
            }
            
            return this._REST.
                post().
                cmd('POST').
                id(id).
                path('/incr').
              //header('x-cc-set-if-not-found',1). //I don't find that this works
                on('end', function (data) {
                    var ret = isNaN(data) ? false : parseInt(data, 10);
                    this.options.cmd = 'incr';
                    this.success(ret);
                }).
                callback(options.callback).
                end('val=' + String(options.amount) + '&');
        },
        decr : function (id, options) {
            if (typeof options === 'function') {
                options = {
                    callback : options
                };
            } else if (!isNaN(options)) {
                options = {
                    amount : options
                };
            }
            
            options         = options        || {};
            options.amount  = options.amount || 1;
            if (typeof arguments[2] === 'function') {
                options.callback = arguments[2];
            }
            
            return this._REST.
                post().
                cmd('POST').
                id(id).
                path('/decr').
              //header('x-cc-set-if-not-found',1). //I don't find that this works
                on('end', function (data) {
                    var ret = isNaN(data) ? false : parseInt(data, 10);
                    this.options.cmd = 'decr';
                    this.success(ret);
                }).
                callback(options.callback).
                end('val=' + String(options.amount) + '&');
        },
        myusage : function (callback) {
            return this._REST.
                get().
                cmd('myusage').
                on('end', function (data) {
                    this.success(data);
                }).
                callback(callback).
                end();
        },
        flush : function (callback) {
            return this._REST.
                get().
                cmd('flush').
                on('end', function (data) {
                    this.success(!!data);
                }).
                callback(callback).
                end();
        }
    }
);

//All you can do with this module is get an object that
//will do work for you
exports.newCache = function (a_key, s_key, host) {
    return new CloudCache(a_key, s_key, host);
};

//Just in case you want to make it yourself.  Or
//modify it
exports.CloudCache = CloudCache;
