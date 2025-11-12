const fs = require('fs')
const path = require('path')
const { MongoClient } = require('mongodb')
const XLSX = require('xlsx')

const DEFAULT_MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017'
const DB_NAME = process.env.MONGO_DB_NAME || 'yellow_db'

const CLI_ARGS = process.argv
const RESULTS_DIR = CLI_ARGS.includes('--results-dir')
  ? path.resolve(CLI_ARGS[CLI_ARGS.indexOf('--results-dir') + 1])
  : path.resolve(__dirname, '..', 'data', 'results')

const FILE_MAP = [
  { fileName: 'result.xlsx', tableKey: 'result' },
  { fileName: 'merge.xlsx', tableKey: 'merge' },
  { fileName: 'transactions.xlsx', tableKey: 'transactions' },
  { fileName: '可疑收款账号.xlsx', tableKey: 'abnormal_accounts' },
  { fileName: 'shopping.xlsx', tableKey: 'shopping' },
  { fileName: '高风险地点统计.xlsx', tableKey: 'risk_hotspot' },
  { fileName: '实口地址高风险统计.xlsx', tableKey: 'risk_population' },
  { fileName: '外卖收货地址高风险统计.xlsx', tableKey: 'risk_shopping' },
]

function readExcel(filePath) {
  const workbook = XLSX.readFile(filePath, { cellDates: true })
  const sheetName = workbook.SheetNames[0]
  const worksheet = workbook.Sheets[sheetName]
  return XLSX.utils.sheet_to_json(worksheet, { defval: null })
}

async function main() {
  if (!fs.existsSync(RESULTS_DIR)) {
    console.error(`[error] results 目录不存在: ${RESULTS_DIR}`)
    process.exit(1)
  }

  const recordFolders = fs
    .readdirSync(RESULTS_DIR, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => dirent.name)
    .filter(name => !name.startsWith('~$'))
    .sort()

  if (recordFolders.length === 0) {
    console.log('[info] 未发现任何记录文件夹，无需导入')
    return
  }

  const client = new MongoClient(DEFAULT_MONGO_URI)
  try {
    await client.connect()
    console.log('[info] 已连接 MongoDB')

    const analysisCollection = client.db(DB_NAME).collection('analysis_records')

    for (const recordId of recordFolders) {
      const folderPath = path.join(RESULTS_DIR, recordId)
      console.log(`\n========== 导入记录: ${recordId} ==========`)

      const tables = {}
      const statistics = {}
      const filesMeta = []

      for (const { fileName, tableKey } of FILE_MAP) {
        const filePath = path.join(folderPath, fileName)
        if (!fs.existsSync(filePath)) {
          console.warn(`[warn] 文件不存在，跳过: ${filePath}`)
          continue
        }

        try {
          const rows = readExcel(filePath).filter(row => row && Object.keys(row).length > 0)
          tables[tableKey] = rows
          statistics[`${tableKey}Count`] = rows.length
          filesMeta.push({
            tableKey,
            fileName,
            rowCount: rows.length,
            lastModified: fs.statSync(filePath).mtime,
          })
          console.log(`[success] 读取 ${fileName} (${rows.length} 条)`)
        } catch (error) {
          console.error(`[error] 读取文件失败: ${filePath}`, error)
        }
      }

      const now = new Date()
      const doc = {
        statistics,
        files: filesMeta,
        tables,
      }

      await analysisCollection.updateOne(
        { recordId },
        {
          $set: {
            ...doc,
            updatedAt: now,
          },
          $setOnInsert: {
            recordId,
            createdAt: now,
          },
        },
        { upsert: true },
      )
    }

    console.log('\n✅ 所有记录导入完成')
  } catch (error) {
    console.error('[error] 导入过程中出现错误', error)
    process.exitCode = 1
  } finally {
    await client.close()
  }
}

main()

