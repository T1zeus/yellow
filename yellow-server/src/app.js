require('dotenv').config()
const express = require('express')
const cors = require('cors')
const morgan = require('morgan')
const path = require('path')

const app = express()

// 中间件配置
app.use(cors())                                    // 跨域支持
app.use(express.json({ limit: '100mb' }))          // JSON 解析（100MB 限制）
app.use(express.urlencoded({ extended: true }))    // URL 编码解析
app.use(morgan('dev'))                              // 请求日志

// 静态文件服务（结果文件可以通过 /files 访问）
const RESULT_DIR = process.env.RESULT_DIR || path.join(__dirname, '..', 'data', 'results')
app.use('/files', express.static(RESULT_DIR))

// TODO: API 路由（下一步创建）
app.use('/api', require('./routes'))

// 健康检查端点
app.get('/health', (req, res) => {
  res.json({ ok: true })
})

// 启动服务器
const host = process.env.SERVER_HOST || '127.0.0.1'
const port = Number(process.env.SERVER_PORT || 8000)

app.listen(port, host, () => {
  console.log(`Server listening on http://${host}:${port}`)
  console.log(`Results directory: ${RESULT_DIR}`)
})