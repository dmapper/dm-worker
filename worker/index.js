// Init env vars. This should be very first
require('dm-sharedb-server/nconf')
let path = require('path')
let {backend} = require('../lib/init')
const BUILD_FOLDER_NAME = 'build'

// get worker actions from the parent project's root folder (if exists)
let actions
try {
  let actionsFilePath = path.join(process.cwd(), BUILD_FOLDER_NAME, 'workerActions.js')
  require.resolve(actionsFilePath)
  actions = require(actionsFilePath)
} catch (e) {
  console.warn('[worker] WARNING! No actions file found. Create a workerActions.js file in ' +
    'your project\'s root with the actions which the worker can execute. This file needs ' +
    'to also be built -- add it to the \'backendApps\' of webpack.config.js')
}

let executeTask = (taskId, done) => {
  // here for some reason we already have all the projects
  // e.g. 'users', 'lobbies', 'clusterOrders'

  let model = backend.createModel()

  let $task = model.at('tasks.' + taskId)
  model.fetch($task, () => {
    let task = $task.get()
    if (!task) return done('No task with such taskId')

    if (task.status !== 'executing') {
      return done('Task status is not executing: ' + task.status)
    }

    let actionType = task.type
    if (!actionType) return done('No action type in task')

    let action = actions[ actionType ]
    if (!action) return done('No action to execute: ' + action)

    action(model, task, () => {
      model.close()
      done()
    })
  })
}

process.on('message', (data) => {
  let taskId = data && data.taskId
  if (!taskId) {
    console.log('[Child Worker] received unknown message format', data)
    return
  }
  executeTask(data.taskId, (err) => {
    process.send({
      taskId: taskId,
      err: err
    })
  })
})

process.send({ready: true})
