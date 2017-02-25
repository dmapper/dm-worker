const _ = require('lodash')
const ChildWorker = require('./ChildWorker')

const CHILDS_LENGTH = process.env.CHILDS_NUM || 2

module.exports = class ChildWorkerManager {

  constructor () {
    this.childs = []
    this.freeChilds = []
    this.ready = false
    if (CHILDS_LENGTH < 2) {
      throw Error('Child Worker Manager error: childsLength should be at least 2: ' + CHILDS_LENGTH)
    }
    this.childsLength = CHILDS_LENGTH
    // Number of executions should be less then a number of child workers
    // to fill the gaps between respawns
    this.executionsLength = Math.floor(CHILDS_LENGTH / 2)

    this.spawnOne()
  }

  spawnOne () {
    let child = new ChildWorker()

    child.on('ready', () => {
      this.childs.push(child)
      this.freeChilds.push(child)

      // We spawn child workers one after another to distribute the load on CPU
      if (this.childs.length < CHILDS_LENGTH) {
        this.spawnOne()
      } else {
        // As far as all child workers are ready, call init
        if (!this.ready) {
          this.ready = true
          this.init && this.init()
        }
      }
    })

    child.on('end', () => {
      this.childs = _.without(this.childs, child)
      this.freeChilds = _.without(this.freeChilds, child)
      this.spawnOne()
    })
  }

  executeTask (taskId, done) {
    if (!this.freeChilds.length) {
      return setTimeout(() => this.executeTask(taskId, done), 200)
    }
    let child = this.freeChilds[0]
    this.freeChilds = _.without(this.freeChilds, child)

    child.executeTask(taskId, (err, workerId) => {
      // As callback can be called after end, we need to check here
      if (!child.ended) this.freeChilds.push(child)
      done(err, workerId)
    })
  }

  executeTaskInSeparateProcess (taskId, done) {
    let child = new ChildWorker()
    child.on('ready', () => child.executeTask(taskId, done))
  }

}
