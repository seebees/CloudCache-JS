
var http = require('http'),
	crypto = require('crypto'),
	EventEmitter = require('events').EventEmitter,
	util = require('util'),
	parse_url = require('url').parse,
	ISODateString = function (d) {		
	    var pad = function(n){
	        return n<10 ? '0'+n : n
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
            args =  Array.prototype.slice.call(arguments, 1);
        return function() {
            return fn.apply(
            	this, 
            	args.concat(Array.prototype.slice.call(arguments, 0))
           	);
        }; 
	};

//TODO write a rational if statement
//var debug = function(x) { console.error('CC: %s', x); };
var  debug = function() { };

//TODO should I implement Connection:keep-alive?
//TODO should I implement Content-length or Buffer for calling data

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
	this.createRequest = function(CloudCacheCmd, path, callback, additionalHeaders) {		
		var ts = ISODateString(),
			sig = crypto.createHmac('sha1', secret).update(
				'CloudCache' + CloudCacheCmd + ts).digest('base64'),
			headers = {
				'User-Agent' : 'CloudCache node.JS Client',
				'Akey'		 : key,
				'Timestamp'	 : ts,
				'Signature'	 : sig,
				'Connection' : 'close'
			};

		if ( additionalHeaders ) {
			for ( i in additionalHeaders ) {
				if ( !headers[i] && additionalHeaders.hasOwnProperty(i) ) {
					headers[i] = additionalHeaders[i];
				}
			}
		}
		
		debug('Headers:\r\n' + util.inspect(headers));
		
		return http.request({
				'host'		: url.host,
				'port'		: url.port,
				'path'		: path,
				'method'	: http_cmds[CloudCacheCmd] || 'GET',
				'headers'	: headers
			}, callback
		);	
	};
};
util.inherits(CloudCache, EventEmitter);

////internal function to process the request
CloudCache.prototype.transport = function(cmd, path, callback, data, encoding, headers) {
	//TODO error handling
	var self = this,
		encoding = String(encoding || 'utf8').toLowerCase(),
		event = cmd in http_cmds ? cmd.toLowerCase() : cmd;
		
	//send the request
	this.createRequest(
			cmd,
			path,
			function(response) {
				if ( response.statusCode in http_success ) {
					//joy, we have data
					self.response(response, event, callback, event, data);
				} else if ( response.statusCode === 404 ) {
					//request for something that does not exists, return null
					callback(null);
					self.response(response, '404', null, event, data);
				} else {
					//Something bad happened
					//TODO to test my errors a delete request without / will result in 400
					//TODO getmulti will throw PHP errors
					//Yes, yes, I am hijacking the third parameter
					self.response(response, 'error', null, event, data);
				}
				
			},
			headers
	).end(data, encoding);

	//return self to allow chaining
	return self;
};

//internal function to process the response
CloudCache.prototype.response = function(response, emit_event, callback, request_event, request_data) {
	var response_data = '',
		self = this;
		
	response.setEncoding('utf-8');
	response.on('data', function(chunk) {
		//put the data back together
		response_data += chunk;
	}).on('end', function() {
		//Send the data back to the client	
		debug('Request Data<<<<<<<\r\n' + util.inspect( {
			'Request Event'	: request_event,
			'Request Data'	: request_data,
			'Emit Event' 	: emit_event,
			'Response Code'	: response.statusCode, 
			'Response Data'	: response_data
		}));
		
		//Call the callback first
		if ( typeof callback === 'function' ) {
			callback(response_data);
		}
		
		//TODO should I be emitting these?  if yes, should I also emit on the ID?
		//Emit the event (not a switch because the default case should be first
		if ( emit_event && emit_event !== 'error' ) {
			self.emit(emit_event, response_data);
		} else if ( emit_event === '404' ) {
			self.emit(request_event, null);
		} else if ( emit_event === 'error' ) {
			self.emit('error', 
				new Error('Unexpected response code: ' + response.statusCode + 
					' for event: ' + callback.event +
					' with request data:\r\n' + callback.data + 
					'\r\n Response Data:\r\n' + response_data)
			);
		}
	}).on('error',function(ex) {
		//whoops, problem.  spread the pain around equally
		this.removeAllListeners('end');
		self.emit('error', ex);
	});
	return this;
};

CloudCache.prototype.auth = function(callback) {
	return this.transport('auth', '/auth', callback);
};

//TODO should I JSON encode ALL data so I get the right type back on primitives?
var restricted_ids = {'auth':1,'list':1,'listkeys':1,'myusage':1}
CloudCache.prototype.put = function(id, data, callback, ttl, encoding) {	
	var ttl  = isNaN(ttl) ? 0: parseInt(ttl,10),
		encoding = String(encoding || 'utf8').toLowerCase(),
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
		this.transport(
			'PUT',
			'/' + encodeURIComponent(id),
			function(ret){
				callback(!!ret);
			},
			to_put,
			encoding,
			{'Ttl':ttl}
		);
	} else {
		if 	( id in restricted_ids ) {
			throw new TypeError('You can not create an item in CloudCache with an ID of:' + id);
		} else {
			throw new TypeError('You can not create an item without and ID');

		}
	}
	return this;
};

//Hoist out the RegEx so we don't have to create it ever time
var multi_split = /(VALUE|\r\nVALUE) (.+?) (.+?)\r\n|\r\nEND\r\n$/
CloudCache.prototype.get = function(id, callback, encoding){
	var encoding = String(encoding || 'utf8').toLowerCase();
	debug('GET:' + id);
	if ( id ) {
		if ( typeof id === 'string' || typeof id === 'number' ) {
			this.transport(
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
	 		var ids = Array.isArray(id) ? id : Object.keys(id);
	 		debug('getmulti:' + util.inspect(id));
	 		this.transport('GET','/getmulti',
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
				'utf8',
				{'keys':JSON.stringify(ids)}
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

CloudCache.prototype.delete = function(id, callback) {
	return this.transport(
		'DELETE',
		'/' + encodeURIComponent(id),
		function(ret){
			/*
			  CloudCache returns the string "$id was deleted from CloudCache"
			  for a successful delete and 404 for a "successful" delete of 
			  an id that does not exists
			  Therefore we return true for actual deletes and false for
			  deletes that are not there
			*/
			callback(!!ret);
		}
	);
};

//currently the only function that returns multiple elements
//I just could not bring myself to force you to go back to the server
//to get the data, since I have the data anyway
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
	var amount  = isNaN(amount) ? 1: parseInt(amount,10);
	
	debug(action + ' id:' + id + ' by ' + amount); 
	if ( id ) {
		this.transport(
			'POST',
			'/' + encodeURIComponent(id) + '/' + action,
			callback,
			'val=' + amount + '&' //'x-cc-set-if-not-found=1',
			//'utf8',
			//{'x-cc-set-if-not-found':1} //the claim is that this header should work
		);
	//TODO should you be able to incr/decr an array of ids?
	} else {
		throw new TypeError('You can not modify an item in CloudCache without and ID');
	}
	return this;
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












