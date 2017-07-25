const assert = require('assert')
const _ = require('lodash')
const path = require('path')
const {DispatcherRunner} = require('./utils')



describe('async tasks', function() {
  const runner = new DispatcherRunner()
  this.timeout(40000)

  before(async () => {
    process.env['WORKER_TASK_DEFAULT_TIMEOUT'] = '30000' //default
    process.env['WORKER_ACTIONS_PATH'] = path.join(__dirname, './workerActions.js')
    process.env['WORKER_INIT_PATH'] = path.join(__dirname, './initWorker.js')
    await runner.dropMongoDatabase()
    await runner.dropRedisDatabase()
    await runner.start(3)
  })

  after(async () => {
    await runner.stop()
  })

  describe('simple case', () => {
    it('fast tasks - regular mode', async () => {
      const taskNum = 10
      const result = await runner.executeTasks({differentUniqIds: true}, taskNum, 0, {duration: 50, _type: 'testAsync'})
      //console.log('result', result)
      assert.equal(taskNum, result.done)
      assert(!result.series)
    })
  })
})