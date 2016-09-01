var Promise     = require('bluebird')
  , _           = require('lodash')
  , config      = require('config')
  , redis       = require('redis')
  , winston     = require('winston')
  ;

var client = redis.createClient({ 
    host      : config.get('metering.redis.host')
  , port      : config.get('metering.redis.port')
  , db        : config.get('metering.redis.db')
  , password  : config.get('metering.redis.password')
});

function meter(type, sub) {
  let date  = new Date()
    , month = date.getUTCMonth()
    , year  = date.getUTCFullYear()
    , uid   = sub
    ;

  if (_.isObject(sub)) {
    uid = sub._id;
  }

  return {
      key       : `metering:${year}:${month + 1}:${uid}:${type}`
    , expire_at : new Date(year, month + 1).getTime()
  };
}

function can(type, sub, cb) {
  return Promise
          .resolve(sub)
          .then((sub) => {
            if (!sub) { return true; }

            let data = meter(type, sub)
            return Promise
                    .fromCallback((cb) => client.get(data.key, cb))
                    .then((count) => {
                      let limit = config.get(`metering.limits.${type}`);
                      if (count >= limit) {
                        throw new Error(`reached limit of ${limit} ${type} per month`);
                      }
                    });
          })
          .asCallback(cb);
}

function did(type, sub, cb) {
  return Promise
          .resolve(sub)
          .then((sub) => {
            if (!sub) { return; }

            let data = meter(type, sub)
            return Promise
                    .fromCallback((cb) => {
                      client.multi()
                        .incrby(data.key, 1)
                        .pexpireat(data.key, data.expire_at)
                        .exec(cb);
                    });
          })
          .asCallback(cb);
}

module.exports = {
    can : can
  , did : did
};