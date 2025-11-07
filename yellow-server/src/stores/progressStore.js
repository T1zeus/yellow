// 简单的内存存储，用于跟踪处理进度
let state = { percent: 0, error: '' }

const progressStore = {
  get() {
    return state
  },
  set(percent, error = '') {
    state = { percent, error }
  },
  reset() {
    state = { percent: 0, error: '' }
  }
}

module.exports = { progressStore }