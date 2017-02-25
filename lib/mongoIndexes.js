module.exports = (mongo) => {
  if (!(mongo && mongo.mongo && mongo.mongo.collection)) {
    console.error('Can\'t set up mongo indexes')
    return
  }

  let collection = mongo.mongo.collection('tasks')

  let indexes = [ {
    status: 1,
    startTime: 1,
    executingTime: 1,
    waitingTime: 1,
    createdAt: 1
  }]

  indexes.forEach(index => {
    collection.ensureIndex(index, false, (err, name) => {
      if (err) {
        console.warn('Could not create index for tasks collection:', index, err.stack || err)
      } else {
        console.log('Index ' + JSON.stringify(index) + ' for collection \'tasks\' created')
      }
    })
  })
}
