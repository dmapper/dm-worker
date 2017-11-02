const racer = require('racer')
const shareDbMongo = require('sharedb-mongo')
const redis = require('redis-url')
const redisPubSub = require('sharedb-redis-pubsub')
const Redlock = require('redlock')
const {promisifyAll} = require('bluebird')
const {Model} = racer
const _ = require('lodash')
const fs = require('fs')
const ROOT_PATH = process.cwd()

promisifyAll(Model.prototype)

getDbs = () => {
  let mongoUrl = process.env['MONGO_URL']
  let mongoOpts = process.env['MONGO_OPTS']
  if (_.isString(mongoOpts)) {
    try {
      mongoOpts = JSON.parse(mongoOpts)
    } catch (e) {}
  }

  let mongoOptions

  if (mongoOpts && fs.existsSync(ROOT_PATH + '/config/' + mongoOpts['key'])) {
    mongoOptions = {
      sslKey: fs.readFileSync(ROOT_PATH + '/config/' + mongoOpts['key']),
      sslCert: fs.readFileSync(ROOT_PATH + '/config/' + mongoOpts['cert']),
      sslCA: fs.readFileSync(ROOT_PATH + '/config/' + mongoOpts['ca'])
    }
  }

  let shareMongo = shareDbMongo(mongoUrl, {allowAllQueries: true, mongoOptions})
  let redis1 = redis.connect()
  let redis2 = redis.connect()

  let redlock = new Redlock([redis1], {
    driftFactor: 0.01,
    retryCount:  2,
    retryDelay:  10,
    retryJitter: 10
  })

  let pubsub = redisPubSub({client: redis1, observer: redis2})

  let backend = racer.createBackend({db: shareMongo, pubsub})

  return {backend, shareMongo, redis1, redis2, redlock}
}

initBackend = () => {

}

module.exports = {
  getDbs,
  initBackend
}