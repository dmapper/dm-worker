# dm-worker
> Worker

## Usage

1. In project root create `worker.js`:

    ```js
    const conf = require('nconf')
    require('dm-sharedb-server/nconf')

    const path = require('path')

    // full path to workerActions.js and workerInit.js
    process.env['WORKER_ACTIONS_PATH'] = path.join(process.cwd(), './build/workerActions.js')
    process.env['WORKER_INIT_PATH'] = path.join(process.cwd(), './build/workerInit.js')

    const TaskDispatcher = require('dm-worker')
    const dispatcher = new TaskDispatcher()

    dispatcher.start().catch((err) => {
      console.log('Error starting worker', err)
    })
    ```

2. In project root create `workerInit.js`. Do any initializations here (plug in hooks, ORM, etc.).
Since this file may be compiled by webpack, use `global.DM_WORKER_INIT` instead of `module.exports`:

    ```js
    import 'dm-sharedb-server/nconf'
    import ShareDB from 'sharedb'
    import richText from 'rich-text'
    import Racer from 'racer'
    import derbyAr from 'derby-ar'
    import ormEntities from './model'
    import shareDbHooks from 'sharedb-hooks'
    import hooks from './server/hooks'

    let init = global.DM_WORKER_INIT = function (backend) {
      // Register rich-text type in ShareDB
      ShareDB.types.register(richText.type)

      // Init ORM
      Racer.use(derbyAr)
      Racer.use(ormEntities)
      shareDbHooks(backend)
      hooks(backend)
    }
    ```

3. In project root create `workerActions.js`. Put your tasks here (name of functions are the name of tasks).
Since this file may be compiled by webpack, use `global.DM_WORKER_ACTIONS` instead of `module.exports`:

    ```js
    let ACTIONS = global.DM_WORKER_ACTIONS = {}

    ACTIONS.test = function (model, task, done) {
      console.log('>> Start test task', task.id)
      setTimeout(() => {
        console.log('>> Finish test task', task.id)
        done()
      }, 5000)
    }
    ```

4. Add `worker`, `workerInit`, `workerActions` to `webpack.config.js` of `dm-react-webpack`:

    ```js
    module.exports = {   
      // ...

      backendApps: {
        server: path.join(__dirname, 'server'),
        worker: path.join(__dirname, 'worker'),
        workerActions: path.join(__dirname, 'workerActions'),
        workerInit: path.join(__dirname, 'workerInit')     
      }

      // ...   
    }
    ```

## MIT Licence

Copyright (c) 2019 Decision Mapper
