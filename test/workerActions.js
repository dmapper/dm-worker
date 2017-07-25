const ACTIONS = global.DM_WORKER_ACTIONS = {}

ACTIONS.test = function (model, task, done) {
  const options = task.options || {}
  const duration = options.duration || 50
  setTimeout(() => {
    //console.log('TASK', task)
    done()
  }, duration)
}