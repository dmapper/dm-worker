const racer = require('racer')
const shareDbMongo = require('sharedb-mongo')
const redis = require('redis-url')
const redisPubSub = require('sharedb-redis-pubsub')
const Redlock = require('redlock')
const {promisifyAll} = require('bluebird')
const {Model} = racer

promisifyAll(Model.prototype)

getDbs = () => {
  let shareMongo = shareDbMongo(process.env['MONGO_URL'], {allowAllQueries: true})
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