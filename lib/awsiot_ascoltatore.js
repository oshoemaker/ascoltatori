"use strict";

var util = require("./util");
var wrap = util.wrap;
var defer = util.defer;
var TrieAscoltatore = require("./trie_ascoltatore");
var AbstractAscoltatore = require('./abstract_ascoltatore');
var debug = require("debug")("ascoltatori:awsiot");
var SubsCounter = require("./subs_counter");
var steed = require("steed")();

/**
 * AWSIoTAscoltatore is a class that inherits from AbstractAscoltatore.
 * It is implemented through the `mqtt` package and it could be
 * backed up by any MQTT broker out there.
 *
 * The options are:
 *  - `url`: the URL to connect to, as defined in https://www.npmjs.com/package/mqtt#connect
 *  -  ... all the options defined in https://www.npmjs.com/package/mqtt#connect
 *
 * @api public
 * @param {Object} opts The options object
 */
function AWSIoTAscoltatore(opts) {
  AbstractAscoltatore.call(this, opts, {
    separator: '/',
    wildcardOne: '+',
    wildcardSome: '#'
  });

  this._opts = opts || {};
  this._opts.keepalive = this._opts.keepalive || 3000;
  this._opts.awsiot = this._opts.awsiot || require('aws-iot-device-sdk');

  this._subs_counter = new SubsCounter();

  this._ascoltatore = new TrieAscoltatore(opts);
  this._startConn();
}

/**
 * AWSIoTAscoltatore inherits from AbstractAscoltatore
 *
 * @api private
 */
AWSIoTAscoltatore.prototype = Object.create(AbstractAscoltatore.prototype);

/**
 * Starts a new connection to an MQTT server.
 * Do nothing if it is already started.
 *
 * @api private
 */
AWSIoTAscoltatore.prototype._startConn = function() {
  var that = this;

  if (this._client === undefined) {
    debug("connecting..");
    this._client = this._opts.awsiot.device(that._opts);

    this._client.setMaxListeners(0);
    this._client.on("connect", function() {
      debug("connected");
      that.reconnectTopics(function(){
        that.emit("ready");
      });
    });

    this._client.on("message", function(topic, payload, packet) {
      debug("received new packet on topic " + topic);
      // we need to skip out this callback, so we do not
      // break the client when an exception occurs
      defer(function() {
        that._ascoltatore.publish(that._recvTopic(topic), payload, packet);
      });
    });
    this._client.on('error', function(e) {
      debug("error in the client");

      delete that._client;
      that.emit("error", e);
    });
  }
  return this._client;
};

AWSIoTAscoltatore.prototype.reconnectTopics = function reconnectTopics(cb) {
  var that = this;

  var subscribedTopics = that._subs_counter.keys();

  var opts = {
    qos: 1
  };

  steed.each(subscribedTopics, function(topic, callback) {
    that._client.subscribe(that._subTopic(topic), opts, function() {
      debug("re-registered subscriber for topic " + topic);
      callback();
    });
  }, function(){
    cb();
  });

};

AWSIoTAscoltatore.prototype.subscribe = function subscribe(topic, callback, done) {
  this._raiseIfClosed();

  if (!this._subs_counter.include(topic)) {
    debug("registering new subscriber for topic " + topic);

    var opts = {
      qos: 1
    };

    this._client.subscribe(this._subTopic(topic), opts, function() {
      debug("registered new subscriber for topic " + topic);
      defer(done);
    });

  } else {
    defer(done);
  }

  this._subs_counter.add(topic);
  this._ascoltatore.subscribe(topic, callback);
};

AWSIoTAscoltatore.prototype.publish = function publish(topic, message, options, done) {
  this._raiseIfClosed();

  this._client.publish(this._pubTopic(topic), message, {
    qos: (options && (options.qos !== undefined)) ? options.qos : 1,
    retain: (options && (options.retain !== undefined)) ? options.retain : false
  }, function() {
    debug("new message published to " + topic);
    wrap(done)();
  });
};

AWSIoTAscoltatore.prototype.unsubscribe = function unsubscribe(topic, callback, done) {
  this._raiseIfClosed();

  var newDone = null;

  newDone = function() {
    debug("deregistered subscriber for topic " + topic);
    defer(done);
  };

  this._ascoltatore.unsubscribe(topic, callback);
  this._subs_counter.remove(topic);

  if (this._subs_counter.include(topic)) {
    newDone();
    return;
  }

  debug("deregistering subscriber for topic " + topic);
  this._client.unsubscribe(this._subTopic(topic), newDone);
};

AWSIoTAscoltatore.prototype.close = function close(done) {
  var that = this;
  debug("closing");
  if (!this._closed) {
    this._subs_counter.clear();
    this._client.once("close", function() {
      debug("closed");
      that._ascoltatore.close();
      delete that._client;
      that.emit("closed");
      defer(done);
    });
    this._client.end();
  } else {
    wrap(done)();
  }
};

util.aliasAscoltatore(AWSIoTAscoltatore.prototype);

/**
 * Exports the AWSIoTAscoltatore
 *
 * @api public
 */
module.exports = AWSIoTAscoltatore;
