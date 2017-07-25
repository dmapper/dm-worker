const delay = async (timeout) => {
  return new Promise((resolve)=> {
    setTimeout(resolve, timeout)
  })
}

isPromise = (value) => {
  return typeof value === 'object' && typeof value.then === 'function';
}

module.exports = {
  delay, isPromise
}