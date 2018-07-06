var Promise     = require('bluebird')
  , _           = require('lodash')
  , config      = require('config')
  , redis       = require('redis')
  , crypto      = require('crypto')
  ;

let redis_opts = {
    host      : config.get('metering.redis.host')
  , port      : config.get('metering.redis.port')
  , db        : config.get('metering.redis.db')
};

if (config.has('metering.redis.password')) {
  let password = config.get('metering.redis.password');
  if (!_.isEmpty(password)) {
    redis_opts.password = password;
  }
}

let client = redis.createClient(redis_opts);

function type_key(type) {
  if (_.isSymbol(type)) {
    switch(type) {
      case METER_TYPES.run:
      return 'run';
      default:
      throw new Error('unknown type symbole');
    }
  }

  return crypto.createHmac('sha256', '').update(type).digest('hex');
}

function meter(type, sub) {
  let date  = new Date()
    , month = date.getUTCMonth()
    , year  = date.getUTCFullYear()
    , uid   = sub
    , tkey  = type_key(type)
    ;

  if (_.isObject(sub)) {
    uid = sub._id;
  }

  return {
      key       : `metering:${year}:${month + 1}:${uid}:${tkey}`
    , type_key  : tkey
    , expire_at : new Date(year, month + 1).getTime()
  };
}

function can(types, sub, cb) {
  if (!_.isArray(types)) {
    types = [types];
  }

  return Promise
          .map(types, (type) => {
            // todo [akamel] this should be
            if (!sub) { return true; }

            let data  = meter(type, sub)
              , ckey  = `metering.limits.${data.type_key}`
              ;

            if (config.has(ckey)) {
              let limit = config.get(ckey);

              return Promise
                      .fromCallback((cb) => client.get(data.key, cb))
                      .then((count) => {
                        if (count >= limit) {
                          throw new Error(`reached limit of ${limit} ${type} per month`);
                        }
                      });
            }

            return true;
          })
          // todo [akamel] this 'false' throws exception and not 'false'
          .reduce((aggregate, can) => aggregate && can)
          .asCallback(cb);
}

function get(key, cb) {
  return Promise
          .fromCallback((cb) => client.get(key, cb))
          .asCallback(cb);
}

function did_meter(type, sub) {
  let meter = METER[type];

  return Promise
          .try(() => {
            if (meter) {
              return meter(type, sub);
            }

            return true;
          });
}

function did(types, sub, cb) {
  if (!_.isArray(types)) {
    types = [types];
  }

  return Promise
          .map(types, (type) => {
            return did_meter(type, sub)
                    .then((cont) => {
                      if (cont) {
                        return METER['*'](type, sub);
                      }
                    });
          })
          .asCallback(cb);
}

const METER_TYPES = {
    'run' : Symbol('run')
};

const METER = {
    [METER_TYPES.run] : (type, sub) => {
      client.incr('blob.run');

      return true;
    }
  , '*' : (type, sub) => {
      if (sub) {
        let data = meter(type, sub)
        return Promise
                .fromCallback((cb) => {
                  client
                    .multi()
                    .incrby(data.key, 1)
                    .pexpireat(data.key, data.expire_at)
                    .exec(cb);
                });
      }
    }
}

module.exports = {
    can
  , did
  , get
  , types : METER_TYPES
};
