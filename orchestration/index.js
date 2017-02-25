// Init env vars. This should be very first
require('dm-sharedb-server/nconf')
const conf = require('nconf')
const _ = require('lodash')
const async = require('async')
const ChildWorkerManager = require('./ChildWorkerManager')

// Must have redis to start workers
if (!conf.get('REDIS_URL')) {
  throw new Error('Can not start task-manager without REDIS_URL!')
}

let {backend, redis, redis2, mongo} = require('../lib/init')
setTimeout(() => require('../lib/mongoIndexes')(mongo), 10000)

const TASK_QUERY_KEY = 'tasks:list'

// These timeouts should be long enough to avoid race conditions
// We use them to recover from process crashes

const WAITING_TIME_TIMEOUT = 30000
const EXECUTING_TIME_TIMEOUT = 30000
const THROTTLE_TIMEOUT = 3000
const SINGLETON_TIMEOUT = 30000 // maximum
const MONGO_QUERY_INTERVAL = 500
const MONGO_QUERY_TIMEOUT = 2000
const MONGO_QUERY_LIMIT = 100
const KILL_PROCESS_TIMEOUT = 3000

let childWorkerManager = new ChildWorkerManager()
let stopped = false

console.log('Master Worker is initializing ' + childWorkerManager.childsLength + ' childs...')

let prepareLockTimeout = (timeout) => {
  return Math.floor(timeout / 1000) || 1
}

let passLock = (lockKey, lockTimeout, fail, success) => {
  // set value if it exists
  redis.setnx(lockKey, Date.now() + lockTimeout + 1, (err, res) => {
    if (err) return fail(err)
    // did set
    if (res === 1) {
      // set key lifetime (when it ends, it will be automatically removed)
      redis.expire(lockKey, prepareLockTimeout(lockTimeout))
      return success()
      // didn't set (lock key already exists)
    } else {
      // get the current key value
      redis.get(lockKey, (err, res) => {
        if (err) return fail(err)
        let lockTime = +res // time in ms -- when the key needs to be deleted
        if (lockTime < Date.now()) { // if the server died between `setnx` and `expire` -- set again
          // extend locking on lockedTimeout
          redis.getset(lockKey, Date.now() + lockTimeout + 1, (err, res) => {
            if (err) return fail(err)

            if (lockTime === +res) {
              redis.expire(lockKey, prepareLockTimeout(lockTimeout))
              success()
            } else {
              fail()
            }
          })
        } else {
          fail()
        }
      })
    }
  })
}

let handleTask = (model, task, lastUniqTaskIds, done) => {
  // console.log 'askTask', task.status

  let waitingFn = (next) => {
    if (task.status !== 'waiting') return next()

    // Here we got task that has not been started to execute somehow
    // Maybe process crashed after we changed status to 'waiting'
    // but before we added taskId to redis query. Maybe it crashed after
    // receiving taskId from redis query, but before starting the
    // child process

    // There is no direct way to check if value exists in redis's list
    // So we use lrem to delete all occurancies of value and then
    // push it whenever it was in list or not

    redis.lrem(TASK_QUERY_KEY, 0, task.id, next)
  }

  let executingFn = (next) => {
    if (task.status !== 'executing') return next()

    // Execution of task reached timeout. This can happen if child process
    // has crashed. Anyway there is no information about was task
    // executed or not. So what we gonna do?

    // One of solutions can be to set status to 'waiting' and make task to
    // execute again. God bless idempotence...

    redis.lrem(TASK_QUERY_KEY, 0, task.id, next)
  }

  let throttleFn = (next) => {
    if (!(task.options && task.options.throttle)) return next()

    let alreadyLocked = () => {
      if (task.options && task.options.trailing && lastUniqTaskIds.indexOf(task.id) !== -1) {
        done()
      } else {
        next('Already exists (throttle)!')
      }
    }

    let timeout = ((task && task.options && task.options.timeout) || THROTTLE_TIMEOUT)
    passLock(`game:throttlelock:${ task.uniqId }`, timeout, alreadyLocked, next)
  }

  let singletonFn = (next) => {
    if (!(task.options && task.options.singleton)) return next()
    passLock(`game:singletonlock:${ task.uniqId }`, SINGLETON_TIMEOUT, done, next)
  }

  let setStatusFn = (err) => {
    let taskPath = `tasks.${task.id}`

    if (err) {
      model.setEach(taskPath, {
        status: 'refused',
        error: err,
        refusedTime: Date.now()
      }, done)
    } else {
      model.setEach(taskPath, {
        status: 'waiting',
        waitingTime: Date.now()
      }, (err) => {
        if (err) return done(err)
        redis.rpush(TASK_QUERY_KEY, task.id, done)
      })
    }
  }

  async.series([ waitingFn, executingFn, throttleFn, singletonFn ], setStatusFn)
}

let handleTasks = (done) => {
  let end = (err) => redis.del(lockKey, () => done(err))
  let fail = () => setTimeout(done, MONGO_QUERY_INTERVAL)

  // here we query for tasks and acknowledge then in series
  let lockKey = 'tasks:lockQuery'
  passLock(lockKey, MONGO_QUERY_TIMEOUT, fail, () => {
    let start = Date.now()
    let model = backend.createModel()
    let now = Date.now()

    let query = model.query('tasks', {
      $or: [
        // regular task
        { status: 'new', startTime: { $exists: false } },
        // delayed task
        { status: 'new', startTime: { $lt: now } },
        // waiting timeout
        { status: 'waiting', waitingTime: { $lt: now - WAITING_TIME_TIMEOUT } },
        // execution timeout
        { status: 'executing', executingTime: { $lt: now - EXECUTING_TIME_TIMEOUT } }
      ],
      $orderby: { createdAt: 1 },
      $limit: MONGO_QUERY_LIMIT
    })

    model.fetch(query, (err) => {
      if (err) return end(err)
      let tasks = query.get()

      // TODO: we can increase expire value here if there are a lot of tasks
      // to avoid race condition
      // it can happen when worker was not running for a while
      // if (tasks.length > 50)
      //   redis.expire('tasks:lockQuery', 10)
      // Another approach is to limit number of tasks in query to be sure
      // that we will finish before lock expire

      let uniqTasks = {}
      ;(tasks || [])
        .filter(task => task.uniqId && task.options && task.options.throttle && task.options.trailing)
        .forEach(task => { uniqTasks[ task.uniqId ] = task.id })
      let lastUniqTaskIds = _.values(uniqTasks)

      async.eachSeries(tasks, (task, next) => {
        handleTask(model, task, lastUniqTaskIds, next)
      }, () => {
        model.close()
        setTimeout(end, MONGO_QUERY_INTERVAL)
      })
    })
  })
}

let doneCount = 0

let popAndExecuteNextTask = (done) => {
  // blpop is blocking pop from the list. If list is empty, it does not return
  // anything till value is appear. 0 - means there is no timeout
  // we use different redis client for this op, cause it blocks other ops

  redis2.blpop(TASK_QUERY_KEY, 0, (err, res) => {
    if (err) return done(err)

    let taskId = res && res[ 1 ]
    if (!taskId) return done('Wrong format of res from blpop: ' + res)

    let lockKey = `tasks:lockTask:${ taskId }`
    passLock(lockKey, EXECUTING_TIME_TIMEOUT, done, () => {

      let model = backend.createModel()
      let task = null

      let finish = (err) => {
        model.close()
        done(err)
      }
      let $task = model.at('tasks.' + taskId)

      model.fetch($task, () => {
        task = $task.get()

        if (!task) return finish('No task with taskId: ' + taskId)

        if (task.status !== 'waiting') {
          return finish() // it's ok 'Task status is not waiting: ' + task.status
        }

        model.setEach('tasks.' + taskId, {
          status: 'executing',
          executingTime: Date.now()
        }, (err) => {
          if (err) return finish(err)

          let start = Date.now()

          let method = 'executeTask'
          if (task.separate) method = 'executeTaskInSeparateProcess'

          childWorkerManager[ method ](taskId, (err, workerId) => {
            redis.del(lockKey)

            if (task.options && task.options.singleton && task.uniqId) {
              redis.del(`game:singletonlock:${task.uniqId}`)
            }

            if (err) {
              console.log('Task execution error: ', task.id, task.uniqId, err)
              model.setEach('tasks.' + taskId, {
                status: 'error',
                error: err,
                errorTime: Date.now()
              }, finish)
            } else {
              let duration = Date.now() - start

              // sdc && sdc.timing('tasks.all.execution_time', duration)

              console.log('Task executed', task.id, task.uniqId,
                Date.now() - task.createdAt, duration, `id:${workerId}`)
              doneCount++
              model.setEach('tasks.' + taskId, {
                status: 'done',
                doneTime: Date.now()
              }, finish)
            }
          })
        })
      })
    })
  })
}

childWorkerManager.init = () => {
  console.log('Master Worker is ready')

  setInterval(() => {
    redis.llen(TASK_QUERY_KEY, (err, res) => doneCount = 0)
  }, 1000)

  // The base idea here is: we have two loops, one for queries for new
  // and timeouted tasks from mongo and putting them to redis list, another
  // for poping taskIds from redis list and executing tasks

  let doQueries = () => {
    if (stopped) return
    handleTasks((err) => {
      if (err) console.error('Mongo loop error: ', err)
      setTimeout(doQueries, 0)
    })
  }

  doQueries()

  let doExecutions = () => {
    if (stopped) return
    popAndExecuteNextTask((err) => {
      if (err) console.error('Execution loop error: ' + err)
      setTimeout(doExecutions, 0)
    })
  }

  // execution loops count depends on number of child workers
  _.range(0, childWorkerManager.executionsLength).forEach(() => doExecutions())
}

let onExit = (code) => {
  console.log('Exiting...')

  // Stop loops
  stopped = true

  setTimeout(() => {
    process.exit(code)
  }, KILL_PROCESS_TIMEOUT)
}

process.on('SIGTERM', onExit)
process.on('SIGINT', onExit)
process.on('SIGQUIT', onExit)

process.on('uncaughtException', (err) => {
  console.log('uncaught:', err, err.stack)
  onExit(100)
})
