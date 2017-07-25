const _ = require('lodash')
const autoStop = require('./utils/autoStop')
const WorkerManager = require('../WorkerManager')
const {getDbs} = require('../db')
const {delay} = require('../utils')
const MongoQueue = require('./MongoQueue')
const RedisQueue = require('./RedisQueue')
const env = process.env

require('./utils/defaults')

module.exports = class TaskDispatcher {
  constructor (num) {
    this.num = num
    this.started = false
    autoStop.once('exit', async () => {
      await this.stop()
    })

    this.dbs = getDbs()
    this.mongoQueue = new MongoQueue(this.dbs, num)
    this.redisQueue = new RedisQueue(this.dbs, this.executeTask.bind(this), num)
  }

  async start(){
    if (this.started) return
    this.workerManager = new WorkerManager(this.num)
    await this.workerManager.start()
    this.started = true
    this._startLoops()
  }

  async stop() {
    //console.log('stopping Task dispatcher')
    if (!this.started) return
    await this.workerManager.stop()
    this.workerManager = null
    this.started = false
  }

  async executeTask(taskId, status = 'executing', statusError) {
    const {backend} = this.dbs
    const model = backend.createModel()
    const collection = env['WORKER_TASK_COLLECTION']
    const $task = model.at(collection + '.' + taskId)
    let workerId

    await model.fetchAsync($task)
    if (!$task.get()) {
      return console.log('ERROR: cant get task', taskId)
    }
    const {uniqId, type} = $task.get()

    switch (status) {
      case ('executing'):
        const start = Date.now()
        await model.setEachAsync($task.path(), {
          status: 'executing',
          executingTime: Date.now()
        })

        try {
          workerId = await this.workerManager.executeTask(taskId)
          await model.setEachAsync($task.path(), {
            status: 'done',
            doneTime: Date.now()
          })

        } catch (err) {
          console.log(`Task error - tId: ${taskId}, uId: ${uniqId}, type: ${type}, error: '${err}'`)

          await model.setEachAsync($task.path(), {
            status: 'error',
            error: err,
            errorTime: Date.now()
          })
        }

        const duration = Date.now() - start
        console.log(`Task executed - tId: ${taskId}, uId: ${uniqId}, type: ${type}, wId: ${workerId}, ${duration}`)

        break
      case ('refused'):
        console.log(`Task refused - tId: ${taskId}, uId: ${uniqId}, type: ${type}`)
        $task.setEach({
          status: 'refused',
          error: statusError,
          refusedTime: Date.now()
        })
        break
      default:
        console.log('Unknown status', taskId, status)
    }

    await model.unfetchAsync($task)
    //console.log('Done task', taskId)
  }

  _startLoops () {
    this._mongoLoop().catch((err) => {
      console.log('Something wrong in mongo loop', err)
    })
    this._redisLoop().catch((err) => {
      console.log('Something wrong in redis loop', err)
    })
  }

  async _mongoLoop(){
    const timeout = Number(env['WORKER_MONGO_QUERY_INTERVAL'])
    await delay(0)
    while (this.started) {
      await this.mongoQueue.doLoop()
      const time = _.random(timeout - 50, timeout + 50)
      await delay( time )
    }
  }

  async _redisLoop(){
    const timeout = 100
    await delay(0)
    while (this.started) {
      await this.redisQueue.doLoop()
      const time = _.random(timeout - 50, timeout + 50)
      await delay( time )
    }
  }
}