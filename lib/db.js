const racer = require('racer')
const shareDbMongo = require('sharedb-mongo')
const Redis = require('ioredis')
const redisPubSub = require('sharedb-redis-pubsub')
const Redlock = require('redlock')
const {promisifyAll} = require('bluebird')
const {Model} = racer
const _ = require('lodash')
const fs = require('fs')
const ROOT_PATH = process.cwd()
const MongoClient = require('mongodb').MongoClient

promisifyAll(Model.prototype)

getDbs = () => {
  let mongoUrl = process.env['MONGO_URL']
  let mongoOpts = process.env['MONGO_OPTS']
  if (_.isString(mongoOpts)) {
    try {
      mongoOpts = JSON.parse(mongoOpts)
    } catch (e) {}
  }

  
  let shareMongo

  if (mongoOpts && fs.existsSync(ROOT_PATH + '/config/' + mongoOpts['key'])) {
    shareMongo = shareDbMongo({
      mongo: (callback) => {
        MongoClient.connect(mongoUrl, {
          sslKey: fs.readFileSync(ROOT_PATH + '/config/' + mongoOpts['key']),
          sslCert: fs.readFileSync(ROOT_PATH + '/config/' + mongoOpts['cert']),
          sslCA: fs.readFileSync(ROOT_PATH + '/config/' + mongoOpts['ca']),
        }, (...args) => {
          let [db] = args
          db.collection('tasks').ensureIndex({status: 1})
          db.collection('tasks').ensureIndex({startTime: 1})
          db.collection('tasks').ensureIndex({executingTime: 1})
          callback(...args)
        })
      },
      allowAllQueries: true
    })
  } else {
    shareMongo = shareDbMongo(mongoUrl, {
      allowAllQueries: true
    })
  }

  let redis1
  let redis2

  if (process.env['REDIS_OPTS']) {
    let redisOpts = process.env['REDIS_OPTS']
    if (_.isString(redisOpts)) {
      try {
        redisOpts = JSON.parse(redisOpts)
      } catch (e) {}
    }

    let tls = {}
    if (fs.existsSync(ROOT_PATH + '/config/' + redisOpts['key'])) {
      tls = {
        key: fs.readFileSync(ROOT_PATH + '/config/' + redisOpts['key'], 'utf8'),
        cert: fs.readFileSync(ROOT_PATH + '/config/' + redisOpts['cert'], 'utf8'),
        ca: fs.readFileSync(ROOT_PATH + '/config/' + redisOpts['ca'], 'utf8')
      }
    }

    redis1 = new Redis({
      sentinels: redisOpts['sentinels'],
      sslPort: redisOpts['ssl_port'] || '6380',
      tls: tls,
      name: 'mymaster',
      db: redisOpts['db'] || 0,
      password: redisOpts['password']
    })

    redis2 = new Redis({
      sentinels: redisOpts['sentinels'],
      sslPort: redisOpts['ssl_port'] || '6380',
      tls: tls,
      name: 'mymaster',
      db: redisOpts['db'] || 0,
      password: redisOpts['password']
    })
  } else {
    redis1 = new Redis(process.env['REDIS_URL'])
    redis2 = new Redis(process.env['REDIS_URL'])
  }

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