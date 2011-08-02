
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

//ID's that mean something to CloudCache.  So you can't use them
var restricted_ids = {
    'auth'      : 1,
    'list'      : 1,
    'listkeys'  : 1,
    'myusage'   : 1,
    'getmulti'  : 1,
    'flush'     : 1
};
    
/**
 *  Base class.
 *
 *  @param {String} arg1    a_key from CloudCache
 *  @param {String} arg2    s_key from CloudCache
 *  @param {String} arg3    optional argument to point at a different URL
 */
var CloudCache = inherit(
    EventEmitter,                     
    function (a_key, s_key, service_url) {
        //closure for our private variables
        var key     = a_key,
            secret  = s_key,
            url     = service_url || 'http://cloudcache.ws',
            self    = this;
        
        //inherit
        EventEmitter.call(self);
        
        //lets get a new service
        self._REST = new flow.Service({
            options : {
                //all request are relative to the base URL
                uri     : url,
                //update the user agent
                headers : {
                    'user-agent'    : 'CloudCace node.JS Client'
                },
                //auth function to build our signature
                'auth'  : function () {
                    var ts = timestamp(),
                        //take the timestamp, build the signature
                        sig = crypto.createHmac('sha1', secret).update(
                            'CloudCache' + this.options.cmd + ts
                        ).digest('base64');
                    //append the magic headers
                    this.headers({
                        'Akey'      : key,
                        'Timestamp' : ts,
                        'Signature' : sig
                    });
                },
                //let me control when a request is made
                request : false,
                //404 just means nothing was there
                '404OK' : true
            },
            //option setters
            //set the CloudCache command
            'cmd'   : function (cmd) {
                if (cmd) {
                    this.options.cmd = cmd;
                    if (restricted_ids.hasOwnProperty(cmd)) {
                        this.path(cmd);
                    }
                }
                
                return this;
            },
            //set the CloudCache id for this request
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
            //set the time to live for this cache item
            'ttl'   : function (ttl) {
                if (!isNaN(ttl)) {
                    this.header('ttl', parseInt(ttl, 10));
                }
                
                return this;
            },
            //set our callback if you want one
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
            //handy function to standardised my returns 
            'success'   : function (ret) {
                //like a promise, things that work get 'success'
                this.emit('success', ret);
                //callbacks are called expecting the first param to hold error
                if (typeof this.options.callback === 'function') {
                    this.options.callback(null, ret);
                }
                //I kind of like this idea, you can listen to the base class for
                //an ID and I will emit any event for an ID
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
        /**
         *  Takes a callback, tells you if you have a good, a_key and s_key
         */
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
        /**
         *  @param {String} id      The ID you want to put
         *  @param          data    The data you want to put
         *  @param          options an options object
         *                              options.encoding
         *                              options.callback
         *                              options.ttl a time to live (default is forever)
         *                              options.stream  if you want to stream data
         *                          If you pass a function insted of an object
         *                          I will take that as a callback.
         *  @param {Function}       If you add a function at the end, I will treat
         *                          it as a callback
         *
         *  @returns    true on success
         *              false on failure
         */
        put : function (id, data, options) {
            var self = this;
            
            debug('PUT on id:' + id + ' data:\r\n' + data);
            //you passed a function insed of an options object, take care of it
            if (typeof options === 'function') {
                options = {
                    callback : options
                };
            }
            //inital conditions
            options             = options           || {};
            options.encoding    = options.encoding  || 'utf-8';
            //you appended a callback, I can handle that.
            if (typeof arguments[3] === 'function') {
                options.callback = arguments[3];
            }
            //make the request
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
            //write the different data types
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
            //if you want a stream you can take care of ending it
            if (!options.stream) {
                request.end();
            }
            
            return request;
        },
        /**
         *  @param      id          The id you want to get
         *                          If you pass an object as an id,
         *                          I will use they keys as ids.
         *                          If you pass an array, I will use
         *                          the elements as id's
         *  @param      options     options.encoding
         *                          options.callback
         *                          If you pass a function, I will use
         *                          it as your callback.
         *  @param                  if your last argument is a function
         *                          then I will use it as your callback
         *  
         *  @returns    if you ask for one id I will return your data
         *              if you ask for more then one id, I will return an object
         *              where they keys are your id's and the values are, uh the values
         */
        get : (function () {
            //Hoist out the RegEx so we don't have to create it ever time
            var multi_split = /(VALUE|\r\nVALUE) (.+?) (.+?)\r\n|\r\nEND\r\n$/;
            //actual working function
            return function get (id, options) {
                debug('GET id:' + id);
                //you passed a function insed of an options object, take care of it
                if (typeof options === 'function') {
                    options = {
                        callback : options
                    };
                }
                //inital conditions
                options = options           || {};
                options.encoding = options.encoding || 'utf-8';
                //you appended a callback, I can handle that.
                if (typeof arguments[2] === 'function') {
                    options.callback = arguments[2];
                }
                //get a request
                var request = this._REST.
                    get().
                    cmd('GET').
                    encoding(options.encoding || 'utf-8').
                    callback(options.callback);
                
                var typeofID = typeof id;
                if (typeofID === 'string' || typeofID === 'number') {
                    //If you asked for one id things are simple
                    //TODO option.streamResponse
                    return request.
                        id(id).
                        on('end', request.success).
                        end();
                } else if (typeofID === 'object') {
                    //get the ids
                    var ids = Array.isArray(id) ? id : Object.keys(id);
                    var self = this;
                     //make the request
                    return request.
                        path('getmulti').
                        header('keys', JSON.stringify(ids)).
                        on('end', function (data_string) {
                            //split the data
                            var data = data_string.split(multi_split);
                            
                            //if you really care, go look at the data.
                            //this loop will get it for you
                            data.shift();
                            var out = {}, i, l = data.length;
                            for (i = 3; i < l; i += 4) {
                                if (data[i - 2]) {
                                    out[data[i - 2]] = data[i];
                                }
                            }
                            //send a response
                            this.success(out);
                            
                            var id;
                            //emit an event for each id.
                            //maybe this is a dumb thing to do?  If so it
                            //should be removed from here, success and exits
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
        /**
         *  @param  {String}    id          the id you want to delete
         *  @param  {Function}  callback    a callback
         *
         *  @returns    true on successful
         *              false on failure
         */
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
        /**
         *  @param  {String}    id          the id you want to delete
         *  @param  {Function}  callback    a callback
         *
         *  @returns    true/false if the value exists
         *              and the value if it exits
         *              I could not bear the thought that you would
         *              have to go back to the server to get the value
         *  this is the only function that returns two paramaters
         */
        exists : function (id, callback) {
            var self = this;
            return this._REST.
                get().
                cmd('GET').
                id(id).
                callback(callback).
                //we need to treat 404 special in this case
                //becuase we want to return real falses for things
                //that don't exist
                on('404', function () {
                    //I did not want to make success really flexable
                    //call me lazy.  I'll probobly pay for it latter
                    //all this to return 2 paramaters
                    this.emit('success', false);
                    if (typeof this.options.callback === 'function') {
                        this.options.callback(null, false);
                    }
                    if (this.options.id) {
                        self.emit(id, 'exits', false);
                    }
                }).
                //Any kind of 200 is good enough for me.
                on('2XX', function (data) {
                    //I did not want to make success really flexable
                    //call me lazy.  I'll probobly pay for it latter
                    //all this to return 2 paramaters
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
        /**
         *  does just what it says
         */
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
        /**
         *  @returns    an object with all the key/value pairs from the cache
         */
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
        /**
         *  a function to increment values in the cache e.g. i++
         *
         *  @param {String} id      the id you want to add to
         *  @param          options
         *                  options.amount the amount to increment.  default = 1
         *                  options.callback
         *
         *  @returns    the new value
         */
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
        /**
         *  a function to decrement values in the cache e.g. i--
         *
         *  @param {String} id      the id you want to add to
         *  @param          options
         *                  options.amount the amount to increment.  default = 1
         *                  options.callback
         *
         *  @returns    the new value
         */
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
        /**
         *  tells you how much space you are useing
         *  @returns    a string
         */
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
        /**
         *  carefull it will flush the whole cache
         *  @returns    true for success
         *              false for failure
         */
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
