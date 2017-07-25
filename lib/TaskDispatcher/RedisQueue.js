
const env = process.env
const {delay} = require('../utils')
const _ = require('lodash')

module.exports = class RedisQueue {
  constructor (dbs, runTask, dispatcherNum) {
    this.dispatcherNum = dispatcherNum
    this.backend = dbs.backend
    this.redlock = dbs.redlock
    this.redis = dbs.redis1
    this.runTask = runTask
  }

  async getTaskList () {
    return new Promise((res) => {
      this.redis.get('tasks:list', (err, result = "[]") => {
        if (err) return res([])
        try {
          const tasks = JSON.parse(result)
          res(tasks)
        } catch (err) {
          res([])
        }
      })
    })
  }

  async doLoop() {
    const tasks = await this.getTaskList() || []
    //console.log('redis loop', this.dispatcherNum, tasks.length)
    await this.handleTasks(tasks)
  }

  async handleTask(task){
    const taskId = task._id
    //console.log('handle task', this.dispatcherNum, taskId)
    const { options = {} } = task
    const locks = {}
    let runFlag

    const taskLockHandler = async () => {
      await delay(0)
      const taskLockKey = 'tasks:regular:' + taskId
      try {
        const timeout = Number(env['WORKER_TASK_DEFAULT_TIMEOUT']) + Number(env['WORKER_MONGO_QUERY_TIMEOUT'])
        locks.taskLock = await this.redlock.lock(taskLockKey, timeout)
      } catch (err) {
        //console.log('task lock', this.dispatcherNum, taskId)
        throw new Error('task skip')
      }
    }

    const singletonLockHandler = async () => {
      await delay(0)
      const timeout = Number(env['WORKER_TASK_DEFAULT_TIMEOUT'])
      if (!options.singleton) return
      const taskSingletonLockKey = 'tasks:singleton:' + task.uniqId
      try {
        locks.singletonLock = await this.redlock.lock(taskSingletonLockKey, timeout)
        //console.log('singleton lock', taskSingletonLockKey)
      } catch (err) {
        //console.log('task singleton lock failed', this.dispatcherNum, taskId, taskSingletonLockKey)
        throw new Error('singleton skip')
      }
    }

    const throttleLockHandler = async () => {
      await delay(0)
      const timeout = options.throttleTimeout || Number(env['WORKER_THROTTLE_TIMEOUT'])
      if (!options.throttle) return
      const taskLockKey = 'tasks:throttle:' + task.uniqId
      try {
        locks.throttleLock = await this.redlock.lock(taskLockKey, timeout)
      } catch (err) {

        // Simple throttle - drop everything if locked
        if (!options.trailing) throw new Error('refused')

        // Trailing throttle - if the task is not the
        // last one just - drop it
        if (task.num !== 1) throw new Error('refused')
      }
    }

    try {
      // The order is VERY important
      await taskLockHandler()
      await throttleLockHandler()
      await singletonLockHandler()

      //console.log('run task')
      runFlag = true
      await this.runTask(taskId, 'executing')

    } catch (err) {
      const message = err.message

      if (message === 'refused'){
        runFlag = true
        await this.runTask(taskId, 'refused', 'Throttle')
      }
      // skip the task
    }

    // Need the timeout not to run one task twice
    if (locks.taskLock) {
      let timeout = Number(env['WORKER_MONGO_QUERY_TIMEOUT']) + 100
      if (!runFlag) timeout = 0

      setTimeout(() => {
        locks.taskLock.unlock(function (err) {
          if (err) console.log('Error while unlocking task lock', err)
        })
      }, timeout)
    }

    if (locks.singletonLock) {
      locks.singletonLock.unlock(function(err) {
        if (err) console.log('Error while unlocking singleton lock', err)
      })
    }
  }

  async handleTasks(tasks) {
    tasks = _.shuffle(tasks)
    for (let task of tasks) {
      await this.handleTask(task)
      await delay(0)
    }
  }

}