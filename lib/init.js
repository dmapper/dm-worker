const createBackend = require('db-sharedb-server/server/backend')
const path = require('path')
const INIT_PATH = path.join(process.cwd(), 'build/workerInit.js')

module.exports = () => {
  let customInit
  // Execute worker init file from the parent project's root folder (if exists)
  try {
    let initFilePath = INIT_PATH
    require.resolve(initFilePath)
    customInit = require(initFilePath)
    if (typeof customInit !== 'function') {
      console.warn('[worker] WARNING! initWorker.js doesn\'t export a function. Ignoring.')
    }
  } catch (e) {
    console.warn('[worker] WARNING! No custom init file found. Create an initWorker.js file in ' +
      'your project\'s root to do the custom initialization of backend (hooks, etc.). ' +
      'This file needs to also be built -- add it to the \'backendApps\' of webpack.config.js')
  }
  let {backend, mongo, redis} = createBackend({
    ee: {emit: () => {}}, // Mock event emitter
    flushRedis: false,
    hooks: customInit && customInit.hooks
  })

  customInit && customInit(backend)
  
  let redisClient = redis.connect()
  let redisClient2 = redis.connect()
  return {backend, redis: redisClient, redis2: redisClient2, mongo}
}
