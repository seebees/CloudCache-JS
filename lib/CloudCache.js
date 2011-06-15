
var http = require('http'),
    crypto = require('crypto'),
    EventEmitter = require('events').EventEmitter,
    util = require('util'),
    parse_url = require('url').parse,
    ISODateString = function (d) {
        var pad = function (n) {
            return n < 10 ? '0' + String(n) : String(n);
        };
        d = d && d instanceof Date ? d : new Date();
        return d.getUTCFullYear()         + '-'
             + pad(d.getUTCMonth() + 1) + '-'
             + pad(d.getUTCDate())      + 'T'
             + pad(d.getUTCHours())     + ':'
             + pad(d.getUTCMinutes())   + ':'
             + pad(d.getUTCSeconds())   + 'Z'
    },
    slice = (function (slice) {
        return function (object) {
            return slice.apply(object, slice.call(arguments, 1));
        };
    }(Array.prototype.slice));

//TODO write a rational if statement
var debug, inspect
    isDebug = false;
if ( isDebug ) { 
    debug = function (x) { console.log('CC: %s', x); };
    inspect = util.inspect;
} else {
    debug = function () {};
    inspect = function () { return '';};
}

//TODO should I implement Connection:keep-alive?
//TODO should I implement Content-length or Buffer for calling data
//TODO should I emit an event for each ID?

//objects to simplify if a=b||a=c||a=d...
var http_cmds = {'GET':'GET','POST':'POST','PUT':'PUT','DELETE':'DELETE'};
    
/*
    Base Class
*/
var CloudCache = function (a_key, s_key, service_url) {
    var key = a_key,
        secret = s_key,
        url = parse_url(service_url || 'http://cloudcache.ws');
    
    EventEmitter.call(this);
     
     //closure for a_key and s_key so we can have as many objects
     //running around as we like
    this._requestData = function (cloudCacheCmd, path, additionalHeaders) {
        var ts = ISODateString(),
            sig = crypto.createHmac('sha1', secret).update(
                'CloudCache' + cloudCacheCmd + ts).digest('base64'),
            headers = {
                'User-Agent'    : 'CloudCache node.JS Client',
                'Akey'          : key,
                'Timestamp'     : ts,
                'Signature'     : sig,
                'Connection'    : 'close'
            }, out;

        if ( additionalHeaders ) {
            for ( i in additionalHeaders ) {
                if ( !headers[i] && additionalHeaders.hasOwnProperty(i) ) {
                    headers[i] = additionalHeaders[i];
                }
            }
        }
        
        out = {
            'host'      : url.host,
            'port'      : url.port,
            'path'      : path,
            'method'    : http_cmds[cloudCacheCmd] || 'GET',
            'headers'   : headers
        };
        
        debug('_requestData:\r\n' + inspect(out) );
        
        return out; 
    };
};
util.inherits(CloudCache, EventEmitter);

//internal function to process the request
CloudCache.prototype._request = function (cmd, path, request_data, encoding, headers, callback) {
    
    var self = this,
        encoding = String(encoding || 'utf8').toLowerCase(),
        request_event = cmd in http_cmds ? cmd.toLowerCase() : cmd;
        
    //send the request
    http.request(
        this._requestData(cmd, path, headers),
        this._response.bind(this, request_event, request_data, callback)
    ).end(request_data, encoding);

    //return self to allow chaining
    return self;
};

//internal function to process the response
CloudCache.prototype._response = function (request_event, request_data, callback, response) {
    var response_data = '',
        self = this;
    
    debug('Response   <<<<<<<\r\n' + util.inspect( {
        'Request Event'    : request_event,
        'Request Data'     : request_data,
        'Response Code'    : response.statusCode
    }));
        
    //404 means that the item does not exist
    //return null, there is no need to parse the response
    if ( response.statusCode === 404 && typeof callback === 'function' ) {
        callback.call(self, null, null);
        return;
    }
    
    response.setEncoding('utf-8');
    response.on('data', function (chunk) {
        //put the data back together
        response_data += chunk;
    }).on('end', function () {
        //Send the data back to the client
        debug('Response Data   <<<<<<<\r\n' + response_data);
        
        switch ( this.statusCode ) {
            case 200:
            case 201:
            case 202:
                if ( typeof callback === 'function' ) {
                    callback.call(self, null, response_data);
                }
                break;
            default:
                var ex = new Error(
                    'Unexpected response code: ' + response.statusCode + 
                    ' for event: ' + request_event +
                    ' with request data:\r\n' + request_data + 
                    '\r\n Response Data:\r\n' + response_data
                );

                if ( typeof callback === 'function' ) {
                    callback.call(self, true, ex);
                } else {
                    self.emit('error', ex);
                }
                break;
        };
    }).on('error',function (ex) {
        //whoops, problem.  spread the pain around equally
        this.removeAllListeners('end');
        self.emit.apply(self, ['error'].concat(slice(arguments)));
    });
    
    return self;
};

CloudCache.prototype._forward = function (error, forwardTo, forwardArgs /*, additionalEmit, ... */ ) {
    var self = this;
    if ( typeof forwardTo === 'function' ) {
        forwardTo.apply(null, [error].concat(forwardArgs));
    }
    if ( !error ) {
        self.emit.apply(self, ['success'].concat(forwardArgs));
        slice(arguments, 3).forEach(function (additionalEmit) {
            this.emit.apply(this, additionalEmit);
        }, self);
    } else {
        self.emit.apply(self, ['error'].concat(forwardArgs));
    }
};

CloudCache.prototype.auth = function (callback) {
    return this._request('auth', '/auth', callback);
};

//TODO should I JSON encode ALL data so I get the right type back on primitives?
var restricted_ids = {'auth':1,'list':1,'listkeys':1,'myusage':1}
CloudCache.prototype.put = function (id, data /* ttl, encoding, callback */) {
    debug('PUT on id:' + id + ' data:\r\n' + data );
    
    var ttl       = isNaN(arguments[2]) ? 
                    0                    : 
                    parseInt(arguments[2],10),
        encoding = typeof arguments[3] === 'string'              ? 
                    String(arguments[3] || 'utf8').toLowerCase() :
                    'utf8',
        callback = typeof arguments[arguments.length - 1] === 'function' ? 
                    arguments[arguments.length - 1]                      : 
                    null,
        data_type = typeof data,
        to_put;
    
    if ( data_type === 'string' || Buffer.isBuffer(data) ) {
        to_put = data;
    } else {
        if ( data_type === 'number' ) {
            to_put = String(data);
        } else if ( data_type === 'boolean' ) {
            to_put = data ? '1' : '0';
        } else if ( data_type === 'function' ) {
            throw new TypeError('You can not create an item in CloudCache that is a function');
        } else {
            to_put = JSON.stringify(data);
        }
    }
    
    if ( id && !(id in restricted_ids) ) {
        this._request(
            'PUT',
            '/' + encodeURIComponent(id),
            to_put,
            encoding,
            {'Ttl':ttl},
            function (error, ret) {
                this._forward(
                    error, 
                    callback, [!!ret], 
                    [id,'PUT', !!ret]
                );
            }
        );
    } else {
        if     ( id in restricted_ids ) {
            throw new TypeError('You can not create an item in CloudCache with an ID of:' + id);
        } else {
            throw new TypeError('You can not create an item without and ID');

        }
    }
    return this;
};

//Hoist out the RegEx so we don't have to create it ever time
var multi_split = /(VALUE|\r\nVALUE) (.+?) (.+?)\r\n|\r\nEND\r\n$/
CloudCache.prototype.get = function (id /* encoding, callback */) {
    //TODO logic to deal with function last
    debug('GET id:' + id);
    
    var encoding = typeof arguments[1] === 'string'               ? 
                    String(arguments[1] || 'utf8').toLowerCase() :
                    'utf8',
        callback = typeof arguments[arguments.length - 1] === 'function' ? 
                    arguments[arguments.length - 1]                      : 
                    null;

    if ( id ) {
        if ( typeof id === 'string' || typeof id === 'number' ) {
            this._request(
                'GET', 
                '/' + encodeURIComponent(id),
                '',
                'utf8',
                {},
                function (error, data) {
                    var out = (encoding === 'utf8' ?
                            data                   :
                            (new Buffer(data[i],'utf8')).toString(encoding));
                    
                    this._forward(
                        error, 
                        callback, [out], 
                        [id,'GET', out]
                    );
                }
            );
        } else if ( typeof id === 'object' ) {
             var ids = Array.isArray(id) ? id : Object.keys(id);
             debug('getmulti:' + util.inspect(id));
             this._request(
                 'GET',
                 '/getmulti',
                '',
                'utf8',
                {'keys':JSON.stringify(ids)},
                function (error, data_string) {
                    var data = data_string.split(multi_split),
                        out=null, l, i=3, emitIDs = [];
                    data.shift();
                    l = data.length;
                    if ( l % 4 === 0 ) {
                        out={};
                        do {
                            if ( data[i-2] ) {
                                out[data[i-2]] = encoding === 'utf8' ? 
                                    data[i]                          : 
                                    (new Buffer(data[i],'utf8')).toString(encoding);
                            }
                            i += 4;
                        } while (i < l );
                        
                        for ( i in out ) {
                            if (out.hasOwnProperty(i) ) {
                                emitIDs.push([ i, 'GET', out[i] ]);
                            }
                        }                        
                    }
                    
                    this._forward.apply(this,
                        [
                            error, 
                            callback, [out]
                        ].concat(emitIDs)
                    );
                }
            );
         } else {
             //How would I even get here?
             throw new TypeError('You can not get an item in CloudCache with an ID of that type');
         }
    } else {
        throw new TypeError('You can not get an item in CloudCache without and ID');
    }
    return this;
};

CloudCache.prototype.delete = function (id, callback) {
    return this._request(
        'DELETE',
        '/' + encodeURIComponent(id),
        '', 'utf8', {},
        function (error, ret) {
            /*
              CloudCache returns the string "$id was deleted from CloudCache"
              for a successful delete and 404 for a "successful" delete of 
              an id that does not exists
              Therefore we return true for actual deletes and false for
              deletes that are not there
            */
            var out = ret !== null ? !!ret : null;
            this._forward(
                error, 
                callback, [out], 
                [id,'DELETE', out]
            );
        }
    );
};

//currently the only function that returns multiple elements
//I just could not bring myself to force you to go back to the server
//to get the data, since I have the data anyway
CloudCache.prototype.exists = function (id /* encoding, callback*/) {
    var encoding = typeof arguments[1] === 'string'               ? 
                    String(arguments[1] || 'utf8').toLowerCase() :
                    'utf8',
        callback = typeof arguments[arguments.length - 1] === 'function' ? 
                    arguments[arguments.length - 1]                      : 
                    null;
                    
    return this.get(
        id, 
        encoding,
        (function (error, ret) {
            var out = !!ret;
            this._forward(
                error, 
                callback, [out, ret], 
                [id,'EXISTS', out, ret]
            );
        }).bind(this)
    );
};

CloudCache.prototype.list_keys = function (callback) {
    return this._request(
        'listkeys', 
        '/listkeys',
        '', 'utf8', {},        
        function (error, data) {
            var out = JSON.parse(data);
            this._forward(
                error, 
                callback, [out]
            );
        }
    );
};

CloudCache.prototype.list = function (callback) {
    return this._request(
        'list', 
        '/list', 
        '', 'utf8', {},
        function (error, data) {
            var out = JSON.parse(data);
            this._forward(
                error, 
                callback, [out]
            );
        }
    );
};

//Both incr and decr are effectivly the same function
//So the implementation is in _cr
var incr = ['incr'],
    decr = ['decr'];
CloudCache.prototype.incr = function (id) {
    return _cr.apply(this, incr.concat(slice(arguments)));
};
CloudCache.prototype.decr = function () {
    return _cr.apply(this, decr.concat(slice(arguments)));
};

function _cr(action, id /* amount, callback */) {
    var amount   = isNaN(arguments[2]) ? 1 : parseInt(arguments[2],10),
        callback = typeof arguments[arguments.length - 1] === 'function' ? 
                    arguments[arguments.length - 1]                      : 
                    null;

    if ( id ) {
        this._request(
            'POST',
            '/' + encodeURIComponent(id) + '/' + action,
            'val=' + amount + '&', //'x-cc-set-if-not-found=1', 
            'utf8',
            {},  //{'x-cc-set-if-not-found':1} //the claim is that this header should work
            function (error, ret) {
                var out   = isNaN(ret) ? false : parseInt(ret, 10);
                this._forward(
                    error, 
                    callback, [out],
                    [id, action.toUpperCase(), out]
                );
            }
        );
        //TODO should you be able to incr/decr an array of ids?
    } else {
        throw new TypeError('You can not modify an item in CloudCache without and ID');
    }
    return this;
}

CloudCache.prototype.myusage = function (callback) {
    return this._request(
        'myusage', 
        '/myusage',
        '', 'utf8', {},
        function (error, data) {
            this._forward(
                error, 
                callback, [data]
            );
        }
    );
};

CloudCache.prototype.flush = function (callback) {
    return this._request(
        'flush', 
        '/flush',
        '', 'utf8', {},
        function (error, ret) {
            this._forward(
                error, 
                callback, [!!ret]
            );
        }
    );
};

//All you can do with this module is get an object that
//will do work for you
exports.createCache = function (a_key, s_key, host) {
    return new CloudCache(a_key, s_key, host);
};

exports.CloudCache = CloudCache;
