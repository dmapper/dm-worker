const _ = require('lodash')
const path = require('path')
const EventEmitter = require('events').EventEmitter
const cluster = require('cluster')

const TASK_TIMEOUT = 90000
const WORKER_ENTRY_PATH = path.join(__dirname, '../worker/index.js')

cluster.setupMaster({exec: WORKER_ENTRY_PATH})

module.exports = class ChildWorker extends EventEmitter {

  constructor () {
    super()
    let worker = cluster.fork(process.env)

    this.worker = worker
    this.ready = false
    this.ended = false
    this.executingId = null
    this.executingCallback = null
    this.startTime = Date.now()
    this.tasksDone = 0

    // child worker will end when reach task limit
    // this way we try to avoid memory leaks
    // it should be different for each child worker so we set it in constructor
    this.tasksLimit = _.random(200, 500)

    let onExit = (code) => {
      // FIXME: never reach here, but without this handler signals from master
      // process are passing to child processes.
      // Maybe the fact, that the handler exists prevents signals from propogation
      // that is exactly, what we need
      console.log('Child Worker Exiting...')
      this.end()
    }

    worker.process.on('SIGTERM', onExit)
    worker.process.on('SIGINT', onExit)
    worker.process.on('SIGQUIT', onExit)

    worker.once('message', (data) => {
      if (this.ended) return
      if (data.ready) {
        // child process spawned, connected and initialized
        // we don't use 'online' event, case our initialization takes
        // about 2 sec, so we want to be sure we can work with
        // child process immediately
        this.ready = true
        console.log('Child Worker is ready', (Date.now() - this.startTime), 'id:', worker.id)
        this.emit('ready')
      }
    })

    worker.on('disconnect', () => {
      this.ready = false

      // Usually 'exit' event follows 'disconnect', but just in case
      // it not, we end in some timeout
      setTimeout(() => this.end('disconnected'), 50)
    })

    worker.on('exit', (code, signal) => {
      this.ready = false
      this.end('exit with code ' + code)
    })

    worker.on('error', (err) => {
      this.ready = false

      // The process could not be spawned, or
      // The process could not be killed, or
      // Sending a message to the child process failed for whatever reason.

      console.error('Child Worker error: ' + err)

      // 'exit' event may or may not follow the 'error' event, so
      // we end here
      this.end(err)
    })
  }

  callExecutingCallback (err) {
    if (this.executingId) this.executingId = null
    if (this.executingCallback) {
      this.executingCallback(err, this.worker.id)
      this.executingCallback = null
      this.tasksDone++

      if (this.tasksDone === this.tasksLimit) {
        console.log('Child Worker tasks limit is reached. Respawn')
        this.end('Tasks limit is reached')
      }
    }
  }

  executeTask (taskId, done) {
    if (this.executingId) return done('Already executing task: ' + this.executingId)
    this.executingId = taskId
    this.executingCallback = done

    let finish = (error) => this.callExecutingCallback(error)

    if (!this.worker) return finish('Worker not forked yet')
    if (this.ended) return finish('Worker is ended')
    if (!this.ready) return finish('Worker not ready')
    if (this.worker.isDead()) return finish('Worker is dead')
    if (!this.worker.isConnected()) return finish('Worker not connected')

    let timer = setTimeout(() => {
      let err = 'Child Worker task timeout reached'
      if (this.executingCallback) {
        this.executingCallback(err, this.worker.id)
        this.executingCallback = null
      }
      this.end(err)
    }, TASK_TIMEOUT)

    this.worker.once('message', (data) => {
      let taskId = data && data.taskId
      let err = data && data.err

      if (!taskId) {
        return finish('Child Worker returned message in unknown format: ' + data)
      }
      if (taskId !== this.executingId) {
        return finish('Child Worker returned message for another task: ' + taskId + ' ' + this.executingId)
      }

      clearTimeout(timer)
      finish(err)
    })

    return this.worker.send({ taskId })
  }

  end (err) {
    if (this.ended) return
    this.ended = true
    this.emit('end', err)

    return setTimeout(() => {
      this.callExecutingCallback(err)
      if (this.worker) {
        try { this.worker.kill() } catch (e) {}
        this.worker = null
      }
    }, 3000)
  }

}
