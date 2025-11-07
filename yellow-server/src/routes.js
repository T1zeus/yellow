const multer = require('multer')
const path = require('path')
const fs = require('fs')
const iconv = require('iconv-lite')
const { Router } = require('express')
const { progressStore } = require('./stores/progressStore')
const { runPipelineNode } = require('./services/pipelineService')

const router = Router()


// 配置上传目录
const UPLOAD_DIR = process.env.UPLOAD_DIR || path.join(__dirname, '..', 'data', 'uploads')
const RESULT_DIR = process.env.RESULT_DIR || path.join(__dirname, '..', 'data', 'results')

// 确保目录存在
fs.mkdirSync(UPLOAD_DIR, { recursive: true })
fs.mkdirSync(RESULT_DIR, { recursive: true })

/**
 * 解码文件名，处理中文编码问题
 * @param {string} filename - 原始文件名（可能是乱码）
 * @returns {string} 解码后的文件名
 */
function decodeFileName(filename) {
  if (!filename) return filename
  
  // 如果文件名看起来已经是正常的 UTF-8（不包含明显的乱码字符），直接返回
  // 检查是否包含常见的乱码模式（如连续的不可打印字符）
  const hasGarbledPattern = /[^\x20-\x7E\u4e00-\u9fa5\u3000-\u303f\uff00-\uffef\w\s\-_.()\[\]（）【】]/.test(filename)
  
  if (!hasGarbledPattern) {
    // 文件名看起来正常，直接返回
    return filename
  }
  
  // 尝试从请求头中提取正确的文件名
  // multer 解析后的文件名可能是 latin1 编码的 UTF-8 字节
  try {
    // 方法1：将 latin1 字符串转换为 Buffer，然后按 UTF-8 解码
    // 这是因为 multer 可能将 UTF-8 字节序列当作 latin1 字符串处理
    const buffer = Buffer.from(filename, 'latin1')
    const utf8Decoded = buffer.toString('utf8')
    
    // 检查解码后的结果是否包含正常的中文字符
    const hasValidChinese = /[\u4e00-\u9fa5]/.test(utf8Decoded)
    const hasInvalidChars = /[^\x20-\x7E\u4e00-\u9fa5\u3000-\u303f\uff00-\uffef\w\s\-_.()\[\]（）【】]/.test(utf8Decoded)
    
    if (hasValidChinese && !hasInvalidChars) {
      return utf8Decoded
    }
  } catch (e) {
    // 解码失败，继续尝试其他方法
  }
  
  // 方法2：尝试 GBK 解码（适用于某些 Windows 系统）
  try {
    const buffer = Buffer.from(filename, 'binary')
    const gbkDecoded = iconv.decode(buffer, 'gbk')
    const hasValidChinese = /[\u4e00-\u9fa5]/.test(gbkDecoded)
    const hasInvalidChars = /[^\x20-\x7E\u4e00-\u9fa5\u3000-\u303f\uff00-\uffef\w\s\-_.()\[\]（）【】]/.test(gbkDecoded)
    
    if (hasValidChinese && !hasInvalidChars) {
      return gbkDecoded
    }
  } catch (e) {
    // 解码失败
  }
  
  // 如果所有解码方法都失败，返回原始文件名
  return filename
}

// 自定义存储配置：支持文件夹上传
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const fieldname = file.fieldname
    
    // 交易和酒店文件夹需要保持目录结构
    if (fieldname === 'transaction_files' || fieldname === 'hotel_files') {
      // 从文件路径中提取相对路径（webkitRelativePath 或 originalname）
      // 修复：正确解码文件名
      let decodedName = decodeFileName(file.originalname)
      const relativePath = (decodedName || '').replace(/\\/g, '/')
      const parts = relativePath.split('/')
      
      // 第一个部分是文件夹名，后面的部分是相对路径
      if (parts.length > 1) {
        const folderName = parts[0]
        const targetDir = path.join(UPLOAD_DIR, fieldname === 'transaction_files' ? 'transaction' : 'hotel', folderName)
        fs.mkdirSync(targetDir, { recursive: true })
        cb(null, targetDir)
      } else {
        // 单文件，直接保存到对应文件夹
        const targetDir = path.join(UPLOAD_DIR, fieldname === 'transaction_files' ? 'transaction' : 'hotel')
        fs.mkdirSync(targetDir, { recursive: true })
        cb(null, targetDir)
      }
    } else {
      // 普通文件，保存到 uploads 根目录
      cb(null, UPLOAD_DIR)
    }
  },
  filename: (req, file, cb) => {
    const fieldname = file.fieldname
    
    // 修复：正确解码文件名
    let decodedName = decodeFileName(file.originalname)
    
    // 对于文件夹上传，保持相对路径结构
    if (fieldname === 'transaction_files' || fieldname === 'hotel_files') {
      const relativePath = (decodedName || '').replace(/\\/g, '/')
      const parts = relativePath.split('/')
      
      if (parts.length > 1) {
        // 去掉第一个部分（文件夹名），保留后续路径
        const filePath = parts.slice(1).join('/')
        cb(null, filePath)
      } else {
        cb(null, decodedName)
      }
    } else {
      // 普通文件，使用解码后的文件名
      cb(null, decodedName)
    }
  }
})


const upload = multer({ storage })

// 文件上传接口 - 使用 upload.any() 接受所有字段
router.post('/upload', upload.any(), async (req, res) => {
  try {
    // 重置进度
    progressStore.reset()
    progressStore.set(1, '') // 开始处理

    // 收集文件路径映射（按字段名）
    const fileMap = new Map()
    
    // 处理所有上传的文件
    for (const f of req.files || []) {
      const fieldname = f.fieldname
      
      // 交易和酒店文件需要特殊处理（保存文件夹路径）
      if (fieldname === 'transaction_files' || fieldname === 'hotel_files') {
        // 这些字段存储文件夹路径
        const folderType = fieldname === 'transaction_files' ? 'transaction' : 'hotel'
        const folderPath = path.join(UPLOAD_DIR, folderType)
        if (!fileMap.has(fieldname)) {
          fileMap.set(fieldname, folderPath)
        }
      } else {
        // 普通文件，保存文件路径
        if (!fileMap.has(fieldname)) {
          fileMap.set(fieldname, f.path)
        }
      }
    }

    console.log('上传的文件:', Array.from(fileMap.entries()))

   // 启动后台处理（异步）
    ;(async () => {
      try {
        await runPipelineNode({ RESULT_DIR, fileMap })
      } catch (error) {
        console.error('后台处理失败:', error)
        progressStore.set(-1, error.message)
      }
    })()

    res.json({ 
      message: '上传成功，已开始后台处理',
      files: Object.keys(req.files || {})
    })
  } catch (error) {
    console.error('上传失败:', error)
    progressStore.set(-1, error.message)
    res.status(500).json({ error: '上传失败: ' + error.message })
  }
})

// 进度查询接口
router.get('/progress', (req, res) => {
  const { percent, error } = progressStore.get()
  res.json({ percent, error })
})

// 结果文件列表查询接口
router.get('/results', (req, res) => {
  try {
    if (!fs.existsSync(RESULT_DIR)) {
      return res.json({ files: [] })
    }

    const files = fs.readdirSync(RESULT_DIR)
    const fileList = files
      .filter(file => {
        const filePath = path.join(RESULT_DIR, file)
        return fs.statSync(filePath).isFile() && (file.endsWith('.xlsx') || file.endsWith('.xls'))
      })
      .map(file => {
        const filePath = path.join(RESULT_DIR, file)
        const stats = fs.statSync(filePath)
        return {
          name: file,
          size: stats.size,
          sizeFormatted: formatFileSize(stats.size),
          modifiedTime: stats.mtime.toISOString(),
          modifiedTimeFormatted: stats.mtime.toLocaleString('zh-CN'),
          url: `/files/${file}`,
        }
      })
      .sort((a, b) => b.modifiedTime.localeCompare(a.modifiedTime)) // 按修改时间倒序

    res.json({ 
      files: fileList,
      count: fileList.length,
      resultDir: RESULT_DIR
    })
  } catch (error) {
    console.error('获取结果文件列表失败:', error)
    res.status(500).json({ error: '获取结果文件列表失败: ' + error.message })
  }
})

/**
 * 格式化文件大小
 * @param {number} bytes - 字节数
 * @returns {string} 格式化后的文件大小
 */
function formatFileSize(bytes) {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
}

module.exports = router