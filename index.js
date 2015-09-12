
/**
 * Module dependencies.
 */

var uid2 = require('uid2');
var redis = require('redis').createClient;
var msgpack = require('msgpack-js');
var Adapter = require('socket.io-adapter');
var Emitter = require('events').EventEmitter;
var debug = require('debug')('socket.io-redis');
var async = require('async');
var Q = require('q');

/**
 * Module exports.
 */

module.exports = adapter;

/**
 * Returns a redis Adapter class.
 *
 * @param {String} optional, redis uri
 * @return {RedisAdapter} adapter
 * @api public
 */

function adapter(uri, opts){
  opts = opts || {};

  // handle options only
  if ('object' == typeof uri) {
    opts = uri;
    uri = null;
  }

  // handle uri string
  if (uri) {
    uri = uri.split(':');
    opts.host = uri[0];
    opts.port = uri[1];
  }

  // opts
  var host = opts.host || '127.0.0.1';
  var port = Number(opts.port || 6379);
  var pub = opts.pubClient;
  var sub = opts.subClient;
  var prefix = opts.key || 'socket.io';

  var commandTimeout = opts.commandTimeout || 1000;

  // init clients if needed
  if (!pub) pub = redis(port, host);
  if (!sub) sub = redis(port, host, { detect_buffers: true });

  var commandListener = redis(port, host, { detect_buffers: true });
  var responseListener = redis(port, host, { detect_buffers: true });
  var pubsubManager = redis(port, host, { detect_buffers: true });

  // this server's key
  var uid = uid2(6);

  // Callback for 'getClients' request
  opts.getClients = getClients;
  
  /**
   * Adapter constructor.
   *
   * @param {String} namespace name
   * @api public
   */

  function Redis(nsp){
    Adapter.call(this, nsp);

    this.uid = uid;
    this.nsp = nsp;
    this.prefix = prefix;
    this.pubClient = pub;
    this.subClient = sub;

    var self = this;
    sub.subscribe(prefix + '#' + nsp.name + '#', function(err){
      if (err) self.emit('error', err);
    });
    sub.on('message', this.onmessage.bind(this));

    commandListener.subscribe(prefix + '#commands#' + nsp.name + '#', function(err){
      if (err) self.emit('error', err);
    });
    commandListener.on('message', this.oncommand.bind(this));
  }

  /**
   * Inherits from `Adapter`.
   */

  Redis.prototype.__proto__ = Adapter.prototype;

  /**
   * Called with a subscription message
   *
   * @api private
   */

  Redis.prototype.onmessage = function(channel, msg){
    var args = msgpack.decode(msg);
    var packet;

    if (uid == args.shift()) return debug('ignore same uid');

    packet = args[0];

    if (packet && packet.nsp === undefined) {
      packet.nsp = '/';
    }

    if (!packet || packet.nsp != this.nsp.name) {
      return debug('ignore different namespace');
    }

    args.push(true);

    this.broadcast.apply(this, args);
  };

  /**
   * Broadcasts a packet.
   *
   * @param {Object} packet to emit
   * @param {Object} options
   * @param {Boolean} whether the packet came from another node
   * @api public
   */

  Redis.prototype.broadcast = function(packet, opts, remote){
    Adapter.prototype.broadcast.call(this, packet, opts);
    if (!remote) {
      if (opts.rooms) {
        opts.rooms.forEach(function(room) {
          var chn = prefix + '#' + packet.nsp + '#' + room + '#';
          var msg = msgpack.encode([uid, packet, opts]);
          pub.publish(chn, msg);
        });
      } else {
        var chn = prefix + '#' + packet.nsp + '#';
        var msg = msgpack.encode([uid, packet, opts]);
        pub.publish(chn, msg);
      }
    }
  };

  /**
   * Subscribe client to room messages.
   *
   * @param {String} client id
   * @param {String} room
   * @param {Function} callback (optional)
   * @api public
   */

  Redis.prototype.add = function(id, room, fn){
    debug('adding %s to %s ', id, room);
    var self = this;
    this.sids[id] = this.sids[id] || {};
    this.sids[id][room] = true;
    this.rooms[room] = this.rooms[room] || {};
    this.rooms[room][id] = true;
    var channel = prefix + '#' + this.nsp.name + '#' + room + '#';
    sub.subscribe(channel, function(err){
      if (err) {
        self.emit('error', err);
        if (fn) fn(err);
        return;
      }
      if (fn) fn(null);
    });
  };

  /**
   * Unsubscribe client from room messages.
   *
   * @param {String} session id
   * @param {String} room id
   * @param {Function} callback (optional)
   * @api public
   */

  Redis.prototype.del = function(id, room, fn){
    debug('removing %s from %s', id, room);

    var self = this;
    this.sids[id] = this.sids[id] || {};
    this.rooms[room] = this.rooms[room] || {};
    delete this.sids[id][room];
    delete this.rooms[room][id];

    if (this.rooms.hasOwnProperty(room) && !Object.keys(this.rooms[room]).length) {
      delete this.rooms[room];
      var channel = prefix + '#' + this.nsp.name + '#' + room + '#';
      sub.unsubscribe(channel, function(err){
        if (err) {
          self.emit('error', err);
          if (fn) fn(err);
          return;
        }
        if (fn) fn(null);
      });
    } else {
      if (fn) process.nextTick(fn.bind(null, null));
    }
  };

  /**
   * Unsubscribe client completely.
   *
   * @param {String} client id
   * @param {Function} callback (optional)
   * @api public
   */

  Redis.prototype.delAll = function(id, fn){
    debug('removing %s from all rooms', id);

    var self = this;
    var rooms = this.sids[id];

    if (!rooms) return process.nextTick(fn.bind(null, null));

    async.forEach(Object.keys(rooms), function(room, next){
      if (rooms.hasOwnProperty(room)) {
        delete self.rooms[room][id];
      }

      if (self.rooms.hasOwnProperty(room) && !Object.keys(self.rooms[room]).length) {
        delete self.rooms[room];
        var channel = prefix + '#' + self.nsp.name + '#' + room + '#';
        return sub.unsubscribe(channel, function(err){
          if (err) return self.emit('error', err);
          next();
        });
      } else {
        process.nextTick(next);
      }
    }, function(err){
      if (err) {
        self.emit('error', err);
        if (fn) fn(err);
        return;
      }
      delete self.sids[id];
      if (fn) fn(null);
    });
  };


  Redis.prototype.requestToAll = function(command, options){
    var requestId = uid2(6);
    
    var responseChannel = prefix + '#response#' + this.nsp.name + '#' + requestId;
    var commandsChannel = prefix + '#commands#' + this.nsp.name + '#';
    
    var msg = msgpack.encode([requestId, this.uid, this.nsp.name, command, responseChannel, options]);
    
    var deferred = Q.defer();
    var deferedPromise = deferred.promise;
    
    var quantity = 1;
    
    
    function responseCallback(channel, msg) {
      var params = msgpack.decode(msg),
          answererRequestId = params[0], 
          answererServerId = params[1], 
          answererResponse = params[2];

      deferedPromise.then(function(data) {
        if (data.request != answererRequestId) {
          return data;
        }
        data.servers = data.servers || {};
        data.servers[answererServerId] = answererResponse;
        return data;
      });

      quantity--;
      
      if ( !quantity ) { resolve(); }
    }

 
    function resolve() {
      deferred.resolve({request: requestId});
      responseListener.removeListener('message', responseCallback);
      responseListener.unsubscribe(responseChannel);
    }


    pubsubManager.pubsub('NUMSUB', commandsChannel, function (err, subs){
      if (err) return deferred.reject(err);
      
      quantity = parseInt( subs[1], 10 ) || 1;

      quantity--;

      if( quantity > 0) {
        responseListener.subscribe(responseChannel, function(err){
          if (err) this.emit('error', err);
        }.bind(this));
        responseListener.on('message', responseCallback);
        pub.publish(commandsChannel, msg);
        setTimeout(resolve, commandTimeout);
      } else {
        deferred.resolve({request: requestId});
      }
    });

    return deferedPromise.then(function(data) {
      data.servers = data.servers || {};
      data.servers[this.uid] = this.getCallbackOnRedisCommand(command)(requestId, options);
      return data; 
    }.bind(this) );
  };


  Redis.prototype.oncommand = function(channel, msg){
    var args = msgpack.decode(msg), 
        nspName, command, options, response, result, requestId;

    requestId = args[0];
    uid = args[1];
    nspName = args[2];

    if (uid == this.uid) return debug('ignore same uid');

    if (!nspName || nspName != this.nsp.name) {
      return debug('ignore different namespace');
    }

    command = args[3];
    response = args[4];
    options = args[5] || {};

    result = this.getCallbackOnRedisCommand(command)(requestId, options);

    var commandMessage = msgpack.encode([requestId, this.uid, result]);
    pub.publish(response, commandMessage);
  };

  
  Redis.prototype.getCallbackOnRedisCommand = function(command, request, options) {
    if ( typeof( opts[command] ) == "function") {
      return opts[command].bind( this.nsp );
    } else {
      return this.callbackOnCommand.bind(this, command);
    }
  }  
  
  // Default callback for any commands
  Redis.prototype.callbackOnCommand = function(command, request, options) {
    return {
      request: request,
      response: {
        options: options,
        info: 'Method for command "' + command + '" not implemented'
      }
    }; 
  }

  Redis.uid = uid;
  Redis.pubClient = pub;
  Redis.subClient = sub;
  Redis.prefix = prefix;

  return Redis;

}

/**
 * Function that runs on each of socket.io nodes. It returns list of active connections(ID)
 * 
 * @param  {String} request 
 * @param  {Object} options
 * @return {Object} { request: "request ID", response: {...} }
 */
function getClients (request, options){
  var clients = [];

  if(!options){ options = {}; }

  for (var id in this.connected) {
    if(options.room) {
      var index = this.connected[id].rooms.indexOf(options.room);
      if(index !== -1) {
        clients.push(this.connected[id]);
      }
    } else {
      clients.push(this.connected[id]);  
    }
  }

  return {
    request: request,
    response: clients.map(function(socket){ return socket.id; })
  }; 
}  
