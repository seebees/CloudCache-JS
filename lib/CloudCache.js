
var http = require('http'),
	crypto = require('crypto'),
	EventEmitter = require('events').EventEmitter,
	util = require('util'),
	parse_url = require('url').parse,
	ISODateString = function (d) {		
	    var pad = function(n){
	        return n>10 ? '0'+n : n
	    };
	    d = d && d instanceof Date ? d : new Date();
	    return d.getUTCFullYear()+'-'
	    + pad(d.getUTCMonth()+1)+'-'
	    + pad(d.getUTCDate())+'T'
	    + pad(d.getUTCHours())+':'
	    + pad(d.getUTCMinutes())+':'
	    + pad(d.getUTCSeconds())+'Z'
	},
	curry = function(func /*...*/){
		var fn = func,
            args = Array.prototype.slice(arguments, 1);
        return function() {
            return fn.apply(this, args.concat(Array.prototype.slice(arguments, 0)));
        }; 
	};

//TODO write a rational if statement
var debug;
//  debug = function(x) { console.error('CC: %s', x); };
  debug = function() { };


//TODO should I implement Connection:keep-alive?
//TODO should I implement Content-lenght or Buffer for calling data

//objects to simplify if a=b||a=c||a=d...
var http_cmds = {'GET':'GET','POST':'POST','PUT':'PUT','DELETE':'DELETE'},
	http_success = {200:1,201:1,202:1};
	
/*
	Base Class
*/
var CloudCache = function(a_key, s_key, service_url) {
	var key = a_key,
		secret = s_key,
		url = parse_url( service_url || 'http://cloudcache.ws');
	
	EventEmitter.call(this);
	 
	 //closure for a_key and s_key so we can have as many objects
	 //running around as we like
	this.transport = function(cmd, path, callback, data, headers) {
		//TODO error handleing
		var sig = crypto.createHmac('sha1', secret),
			self = this,
			event = cmd in http_cmds ? cmd.toLowerCase() : cmd,
			ts = ISODateString(),
			req, i;
		
		sig.update('CloudCache' + cmd + ts);
		debug('CloudCache' + cmd + ts);

		//make the request
		req = http.request({
				'host'		: url.host,
				'port'		: url.port,
				'path'		: path,
				'method'	: http_cmds[cmd] || 'GET',
				'headers'	: {
					'User-Agent' : 'CloudCache node.JS Client',
					'Akey'		 : key,
					'Timestamp'	 : ts,
					'Signature'	 : sig.digest('base64'),
					'Connection' : 'close'				
				}
			}, function(response) {
				var response_data = '';
				
				debug('ResponseCode:' + response.statusCode);
				
				if ( response.statusCode in http_success ) {
					//joy, we have data
					self.response(response, event, callback);
				} else if ( response.statusCode === 404 ) {
					//request for something that does not eixst, return null
					callback(null);
//					self.response(response, 'error');
				} else {
					//Something bad happend
					//TODO to test my errors a delete request without / will result in 400
					//TODO getmulti will throw PHP errors
					self.response(response, 'error');
				}
				
			}
		);
		
		//add any hedders (only ttl right now...)
		if ( headers ) {
			for ( i in headers ) {
				if ( headers.hasOwnProperty(i) ) {
					req.setHeader(i,headers[i]);
				}
			}
		}
		
		//send the request
		req.end(data);

		//return self to alow chaining
		return self;
	};
};
util.inherits(CloudCache, EventEmitter);

//internal function to proccess the response
//I suppose someone may want to override it so I put it on the prototype
CloudCache.prototype.response = function(response, event, callback) {
	var response_data = '';
	response.setEncoding('utf-8');
	response.on('data', function(chunk) {
		response_data += chunk;
	});
	response.on('end', function() {					
		//TODO if no errors were emited
		debug('Event:' + event + '\r\nResponse:' + response_data);
		if ( typeof callback === 'function' ) {
			callback(response_data);
		}
		this.emit(event, response_data);
	});
};

CloudCache.prototype.auth = function(callback) {
	return this.transport('auth', '/auth', callback);
};

//TODO if I put an array, object, number ClientRequest will complain that it wants a buffer
var restricted_ids = {'auth':1,'list':1,'listkeys':1,'myusage':1}
CloudCache.prototype.put = function(id, data, callback, ttl, encoding) {
	encoding = String(encoding || 'utf8').toLowerCase();
	
	var t = isNaN(ttl) ? 0: parseInt(ttl,10),
		d = typeof data === 'string' ? data :
				data instanceof Buffer ? 
					data.toString(encoding) : 
					JSON.parse(data);
	
	
	if ( typeof id === 'string' && !(id in restricted_ids) ) {
		return this.transport(
			'PUT',
			'/' + encodeURIComponent(id),
			function(ret){
				callback(!!ret);
			},
			d,
			{'Ttl':t}
		);
	}
};

//Hoist out the RegEx so we don't have to create it ever time
var multi_split = /(VALUE|\r\nVALUE) (.+?) (.+?)\r\n|\r\nEND\r\n$/

CloudCache.prototype.get = function(id, callback, encoding){
	encoding = String(encoding || 'utf8').toLowerCase();
	debug('GET:' + id);
	if ( id ) {
		if ( typeof id === 'string' ) {
			return this.transport(
				'GET', 
				'/' + encodeURIComponent(id), 
				function(data){
					callback( encoding === 'utf8' ?
						data 					  :
						(new Buffer(data[i],'utf8')).toString(encoding)
					);
				}
			);
		} else if ( typeof id === 'object' ) {
	 		var ids = id instanceof Array ? id : Object.keys(id);
	 		return this.transport('GET','/getmulti',
				function(data_string){
					var data = data_string.split(multi_split),
						out=null, l, i=3;
					data.shift();
					l = data.length;
					if ( l % 4 === 0 ) {
						out={};
						do {
							if ( data[i-2] ) {
								out[data[i-2]] = encoding === 'utf8' ? 
									data[i] 						 : 
									(new Buffer(data[i],'utf8')).toString(encoding);
							}
							i += 4;
						} while (i < l );
					}
					callback(out);
				},
				null,
				{'keys':JSON.stringify(ids)}
			);
	 	} else {
	 		//TODO throw error
	 	}
	} else {
		//TODO throw error
	}
};

CloudCache.prototype.delete = function(id, callback) {
	return this.transport(
		'DELETE',
		'/' + encodeURIComponent(id),
		function(ret){
			/*
			  CloudCache returns the string "$id was deleted from CloudCache"
			  for a successful delete and 404 for a "successful" delete of 
			  an id that does not eixst
			  Therfore we return true for actual deletes and false for
			  deletes that are not there
			*/
			callback(!!ret);
		}
	);
};

//currently the only function that returns multipule elements
//I just could not bring myself to force you to go back to the server
//since I have the data anyway
CloudCache.prototype.exists = function(id, callback, encoding){
	return this.get(id, function(ret){
		callback(!!ret, ret);
	},encoding);
};

CloudCache.prototype.list_keys = function(callback){
	return this.transport('listkeys', '/listkeys', function(data){
		callback(JSON.parse(data));
	});
};

CloudCache.prototype.list = function(callback){
	return this.transport('list', '/list', function(data){
		callback(JSON.parse(data));
	});
};

//Both incr and decr are effectivly the same function
//So the implementation is in _cr
CloudCache.prototype.incr = curry(_cr,'incr');
CloudCache.prototype.decr = curry(_cr,'decr');

function _cr(action, id, callback, amount){
	if ( typeof id === 'string' ) {
		return this.transport(
			'POST',
			'/' + encodeURIComponent(id) + '/' + action,
			callback,
			'val=' + amount + '&'
		);
	}
}

CloudCache.prototype.myusage = function(callback){
	return this.transport('myusage', '/myusage', callback);
};

CloudCache.prototype.flush = function(callback) {
	return this.transport('flush', '/flush', callback);
};

//All you can do with this module is get an object that
//will do work for you
exports.createCloudCache = function(a_key, s_key, host){
	return new CloudCache(a_key, s_key, host);
};












