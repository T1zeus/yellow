const multer = require('multer')
const path = require('path')
const fs = require('fs')
const iconv = require('iconv-lite')
const { Router } = require('express')
const { exec } = require('child_process')
const { promisify } = require('util')
const { MongoClient } = require('mongodb')
const { progressStore } = require('./stores/progressStore')
const { runPipelineNode } = require('./services/pipelineService')

const execAsync = promisify(exec)

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
    // 注意：路径信息通过元数据传递，这里先保存到临时位置，后续会移动到正确位置
    if (fieldname === 'transaction_files' || fieldname === 'hotel_files') {
      // 先保存到对应文件夹的临时位置
      const targetDir = path.join(UPLOAD_DIR, fieldname === 'transaction_files' ? 'transaction' : 'hotel')
      fs.mkdirSync(targetDir, { recursive: true })
      cb(null, targetDir)
    } else {
      // 普通文件，保存到 uploads 根目录
      cb(null, UPLOAD_DIR)
    }
  },
  filename: (req, file, cb) => {
    const fieldname = file.fieldname
    
    // 修复：正确解码文件名
    let decodedName = decodeFileName(file.originalname)
    
    // 对于文件夹上传，先使用原始文件名保存（后续会根据元数据移动到正确位置）
    // 普通文件，使用解码后的文件名
    cb(null, decodedName)
  }
})


const upload = multer({ storage })

// 文件上传接口 - 使用 upload.any() 接受所有字段
router.post('/upload', upload.any(), async (req, res) => {
  try {
    // 重置进度
    progressStore.reset()
    progressStore.set(1, '') // 开始处理

    // 收集文件路径元数据（从请求体中获取）
    // 注意：multer 会将非文件字段放在 req.body 中
    const pathMetadata = {}
    
    Object.keys(req.body || {}).forEach(key => {
      // 匹配格式：fieldName_metadata_index_filename 或 fieldName_metadata
      if (key.includes('_metadata')) {
        // 提取字段名（去掉 _metadata 及其后面的部分）
        const match = key.match(/^(.+?)_metadata/)
        if (match) {
          const fieldName = match[1]
          if (!pathMetadata[fieldName]) {
            pathMetadata[fieldName] = []
          }
          try {
            const metadata = JSON.parse(req.body[key])
            // 确保 metadata 包含 fieldName（如果前端没有传递）
            if (!metadata.fieldName) {
              metadata.fieldName = fieldName
            }
            pathMetadata[fieldName].push(metadata)
          } catch (e) {
            console.error(`解析路径元数据失败 ${key}:`, e)
          }
        }
      }
    })

    // 收集文件路径映射（按字段名）
    const fileMap = new Map()

    // 处理文件，匹配路径信息并移动到正确位置
    for (const f of req.files || []) {
      const fieldname = f.fieldname
      
      // 交易和酒店文件需要特殊处理（保存文件夹路径）
      if (fieldname === 'transaction_files' || fieldname === 'hotel_files') {
        // 解码文件名（处理编码问题）
        const decodedOriginalName = decodeFileName(f.originalname)
        
        // 查找对应的路径信息（通过文件名匹配）
        const metadataList = pathMetadata[fieldname] || []
        const metadata = metadataList.find(m => {
          // 从路径中提取文件名进行匹配
          const pathParts = (m.path || '').split('/')
          const pathFileName = pathParts[pathParts.length - 1]
          
          // 尝试多种匹配方式（处理编码问题）
          const nameMatch = m.name === f.originalname || 
                           m.name === decodedOriginalName ||
                           pathFileName === f.originalname ||
                           pathFileName === decodedOriginalName
          
          // 如果精确匹配失败，尝试模糊匹配（去掉特殊字符后比较）
          if (!nameMatch) {
            const normalize = (str) => str.replace(/[^\w\u4e00-\u9fa5]/g, '').toLowerCase()
            return normalize(m.name) === normalize(decodedOriginalName) ||
                   normalize(pathFileName) === normalize(decodedOriginalName)
          }
          
          return nameMatch
        })
        
        if (metadata && metadata.path) {
          // 处理路径：去掉第一个部分（最外层文件夹名），保留后续路径
          const relativePath = metadata.path.replace(/\\/g, '/')
          const parts = relativePath.split('/')
          
          if (parts.length > 1) {
            // 对于交易文件，保留子文件夹结构（证件号码-姓名/xxx.xlsx）
            // 对于酒店文件，是直接文件结构（只有文件夹名/文件名），直接保存文件名
            let filePath
            if (fieldname === 'transaction_files' && parts.length > 2) {
              // 交易文件：去掉第一个部分，保留子文件夹结构
              filePath = parts.slice(1).join('/')
            } else {
              // 酒店文件或其他单层结构：只保留文件名
              filePath = parts[parts.length - 1]
            }
            
            const targetDir = path.join(UPLOAD_DIR, fieldname === 'transaction_files' ? 'transaction' : 'hotel')
            const fullPath = path.join(targetDir, filePath)
            const dirPath = path.dirname(fullPath)
            
            // 确保目录存在
            fs.mkdirSync(dirPath, { recursive: true })
            
            // 移动文件到正确位置
            try {
              fs.renameSync(f.path, fullPath)
            } catch (e) {
              console.error(`移动文件失败 ${f.path} -> ${fullPath}:`, e)
              // 如果移动失败，尝试复制
              fs.copyFileSync(f.path, fullPath)
              fs.unlinkSync(f.path)
            }
          } else {
            // 单文件，直接保存到对应文件夹
            const targetDir = path.join(UPLOAD_DIR, fieldname === 'transaction_files' ? 'transaction' : 'hotel')
            const fullPath = path.join(targetDir, decodedOriginalName || f.originalname)
            fs.mkdirSync(targetDir, { recursive: true })
            fs.renameSync(f.path, fullPath)
          }
        } else {
          // 如果没有元数据，保存到默认位置（使用解码后的文件名）
          const targetDir = path.join(UPLOAD_DIR, fieldname === 'transaction_files' ? 'transaction' : 'hotel')
          const fullPath = path.join(targetDir, decodedOriginalName || f.originalname)
          fs.mkdirSync(targetDir, { recursive: true })
          fs.renameSync(f.path, fullPath)
        }
        
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
        await runPipelineNode({ fileMap })
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

// 结果记录列表查询接口（从 MongoDB 读取）
router.get('/records', async (req, res) => {
  try {
    const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017'
    const DB_NAME = process.env.MONGO_DB_NAME || 'yellow_db'
    const client = new MongoClient(MONGO_URI)
    
    try {
      await client.connect()
      const db = client.db(DB_NAME)
      const collection = db.collection('analysis_records')
      
      // 从 MongoDB 读取所有记录
      const records = await collection.find({}).sort({ createdAt: -1 }).toArray()
      
      const recordList = records.map(record => ({
        id: record.recordId,
        name: record.recordId,
        fileCount: record.files?.length || 0,
        files: (record.files || []).map(file => ({
          name: file.fileName,
          size: 0, // MongoDB 中没有文件大小
          sizeFormatted: `${file.rowCount || 0} 条记录`,
        })),
        createdTime: record.createdAt?.toISOString() || new Date().toISOString(),
        createdTimeFormatted: record.createdAt ? new Date(record.createdAt).toLocaleString('zh-CN') : '',
        modifiedTime: record.updatedAt?.toISOString() || new Date().toISOString(),
        modifiedTimeFormatted: record.updatedAt ? new Date(record.updatedAt).toLocaleString('zh-CN') : '',
      }))
      
      res.json({ 
        records: recordList,
        count: recordList.length,
      })
    } catch (mongoError) {
      console.error('[MongoDB] 查询记录列表失败:', mongoError.message)
      res.status(500).json({ error: '查询记录列表失败: ' + mongoError.message })
    } finally {
      await client.close()
    }
  } catch (error) {
    console.error('获取记录列表失败:', error)
    res.status(500).json({ error: '获取记录列表失败: ' + error.message })
  }
})

// 获取指定记录的完整数据（从 MongoDB）
router.get('/records/:recordId/data', async (req, res) => {
  try {
    const { recordId } = req.params
    const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017'
    const DB_NAME = process.env.MONGO_DB_NAME || 'yellow_db'
    const client = new MongoClient(MONGO_URI)
    
    try {
      await client.connect()
      const db = client.db(DB_NAME)
      const collection = db.collection('analysis_records')
      
      const record = await collection.findOne({ recordId })
      
      if (!record) {
        return res.status(404).json({ error: '记录不存在' })
      }
      
      // 返回完整记录数据（包含 tables, statistics, files 等）
      res.json({
        recordId: record.recordId,
        createdAt: record.createdAt,
        updatedAt: record.updatedAt,
        statistics: record.statistics || {},
        files: record.files || [],
        tables: record.tables || {},
      })
    } catch (mongoError) {
      console.error('[MongoDB] 查询记录失败:', mongoError.message)
      res.status(500).json({ error: '查询记录失败: ' + mongoError.message })
    } finally {
      await client.close()
    }
  } catch (error) {
    console.error('获取记录数据失败:', error)
    res.status(500).json({ error: '获取记录数据失败: ' + error.message })
  }
})


// 删除记录接口（移动到系统回收站）
router.delete('/records/:recordId', async (req, res) => {
  try {
    const { recordId } = req.params
    const recordPath = path.join(RESULT_DIR, recordId)
    
    // 安全检查：确保路径在 RESULT_DIR 内
    const resolvedPath = path.resolve(recordPath)
    const resolvedResultDir = path.resolve(RESULT_DIR)
    if (!resolvedPath.startsWith(resolvedResultDir)) {
      return res.status(403).json({ error: '访问被拒绝' })
    }
    
    if (!fs.existsSync(recordPath)) {
      return res.status(404).json({ error: '记录不存在' })
    }
    
    // 检查是否为目录
    if (!fs.statSync(recordPath).isDirectory()) {
      return res.status(400).json({ error: '指定的路径不是目录' })
    }
    
    // 删除 MongoDB 中的记录
    const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017'
    const DB_NAME = process.env.MONGO_DB_NAME || 'yellow_db'
    const client = new MongoClient(MONGO_URI)
    
    let mongoDeleted = false
    try {
      await client.connect()
      const db = client.db(DB_NAME)
      const collection = db.collection('analysis_records')
      
      const result = await collection.deleteOne({ recordId })
      if (result.deletedCount > 0) {
        mongoDeleted = true
        console.log(`[MongoDB] 已删除记录: ${recordId}`)
      } else {
        console.log(`[MongoDB] 记录不存在或已删除: ${recordId}`)
      }
    } catch (mongoError) {
      console.error('[MongoDB] 删除记录失败:', mongoError.message)
      // 不中断流程，继续删除文件系统
    } finally {
      await client.close()
    }
    
    // 使用 PowerShell 将文件夹移动到系统回收站
    // 转义路径中的特殊字符
    const escapedPath = recordPath.replace(/'/g, "''").replace(/"/g, '`"')
    
    // PowerShell 命令：使用 FileSystem.DeleteDirectory 移动到回收站
    const powershellCommand = `powershell -Command "Add-Type -AssemblyName Microsoft.VisualBasic; [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory('${escapedPath}', 'OnlyErrorDialogs', 'SendToRecycleBin')"`
    
    try {
      await execAsync(powershellCommand)
      console.log(`记录已移动到系统回收站: ${recordPath}`)
      
      res.json({ 
        status: 'success',
        message: '记录已移动到回收站' + (mongoDeleted ? '，数据库记录已删除' : '')
      })
    } catch (execError) {
      // 如果 PowerShell 命令失败，尝试直接删除（兼容性处理）
      console.warn('移动到回收站失败，尝试直接删除:', execError.message)
      fs.rmSync(recordPath, { recursive: true, force: true })
      console.log(`记录已删除: ${recordPath}`)
      
      res.json({ 
        status: 'success',
        message: '记录删除成功' + (mongoDeleted ? '，数据库记录已删除' : '')
      })
    }
  } catch (error) {
    console.error('删除记录失败:', error)
    res.status(500).json({ error: '删除记录失败: ' + error.message })
  }
})

// 从 MongoDB 导出 Excel 文件接口
router.get('/export/:recordId/:tableKey', async (req, res) => {
  try {
    const { recordId, tableKey } = req.params
    const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017'
    const DB_NAME = process.env.MONGO_DB_NAME || 'yellow_db'
    const client = new MongoClient(MONGO_URI)
    
    try {
      await client.connect()
      const db = client.db(DB_NAME)
      const collection = db.collection('analysis_records')
      
      const record = await collection.findOne({ recordId })
      
      if (!record) {
        return res.status(404).json({ error: '记录不存在' })
      }
      
      // 映射 tableKey 到文件名
      const FILE_MAP = {
        'result': 'result.xlsx',
        'merge': 'merge.xlsx',
        'transactions': 'transactions.xlsx',
        'abnormal_accounts': '可疑收款账号.xlsx',
        'shopping': 'shopping.xlsx',
        'risk_hotspot': '高风险地点统计.xlsx',
        'risk_population': '实口地址高风险统计.xlsx',
        'risk_shopping': '外卖收货地址高风险统计.xlsx',
      }
      
      const fileName = FILE_MAP[tableKey]
      if (!fileName) {
        return res.status(400).json({ error: '无效的表格类型' })
      }
      
      const data = record.tables?.[tableKey] || []
      
      // 使用 excelService 生成 Excel
      const { writeArrayToExcel } = require('./services/excelService')
      
      // 处理数据，准备导出
      const processedData = data.map(row => ({ ...row }))
      
      // 创建临时文件路径
      const tempDir = path.join(__dirname, '..', 'data', 'temp')
      fs.mkdirSync(tempDir, { recursive: true })
      const tempFilePath = path.join(tempDir, `${recordId}_${tableKey}_${Date.now()}.xlsx`)
      
      // 导出选项
      const exportOptions = {
        excludeFields: ['异常资金', '入住次数', '同住男人数', '前科次数', '前科人员', '前科情况']
      }
      
      // 某些表格不需要排除字段
      if (tableKey === 'result' || tableKey === 'merge') {
        delete exportOptions.excludeFields
      }
      
      // 写入 Excel 文件
      writeArrayToExcel(processedData, tempFilePath, exportOptions)
      
      // 发送文件
      res.download(tempFilePath, fileName, (err) => {
        // 删除临时文件
        if (fs.existsSync(tempFilePath)) {
          fs.unlinkSync(tempFilePath)
        }
        
        if (err) {
          console.error('下载文件失败:', err)
          if (!res.headersSent) {
            res.status(500).json({ error: '下载文件失败' })
          }
        }
      })
    } catch (mongoError) {
      console.error('[MongoDB] 导出文件失败:', mongoError.message)
      res.status(500).json({ error: '导出文件失败: ' + mongoError.message })
    } finally {
      await client.close()
    }
  } catch (error) {
    console.error('导出文件失败:', error)
    res.status(500).json({ error: '导出文件失败: ' + error.message })
  }
})


module.exports = router