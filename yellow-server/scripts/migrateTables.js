require('dotenv').config()

const { MongoClient } = require('mongodb')
const {
  saveTables,
} = require('../src/services/mongoTableService')

async function migrate() {
  const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017'
  const DB_NAME = process.env.MONGO_DB_NAME || 'yellow_db'

  const client = new MongoClient(MONGO_URI)

  try {
    await client.connect()
    const db = client.db(DB_NAME)
    const collection = db.collection('analysis_records')

    const cursor = collection.find({
      tables: { $exists: true, $ne: null },
    })

    let migrated = 0
    while (await cursor.hasNext()) {
      const record = await cursor.next()
      const recordId = record.recordId
      const tables = record.tables || {}

      if (!recordId || Object.keys(tables).length === 0) {
        continue
      }

      console.log(`开始迁移记录 ${recordId} ...`)
      await saveTables(db, recordId, tables)
      await collection.updateOne(
        { _id: record._id },
        { $unset: { tables: '' } }
      )
      migrated += 1
      console.log(`记录 ${recordId} 迁移完成`)
    }

    console.log(`迁移完成，共处理 ${migrated} 条记录`)
    console.log('如需回收旧字段，可执行: db.analysis_records.updateMany({}, {$unset: {tables: ""}})')

    // 为新集合创建索引
    await db.collection('analysis_tables').createIndex({ recordId: 1, tableKey: 1, chunkIndex: 1 })
  } catch (error) {
    console.error('迁移失败:', error)
    process.exitCode = 1
  } finally {
    await client.close()
  }
}

migrate()

