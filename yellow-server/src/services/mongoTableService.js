const CHUNK_SIZE = parseInt(process.env.MONGO_TABLE_CHUNK_SIZE || '2000', 10)

/**
 * 将表数据分块写入 analysis_tables 集合，避免 16MB 限制
 */
async function saveTables(db, recordId, tables = {}) {
  if (!db || !recordId || !tables) return
  const tablesCollection = db.collection('analysis_tables')

  const tableEntries = Object.entries(tables)
  for (const [tableKey, rows] of tableEntries) {
    await saveSingleTable(db, recordId, tableKey, rows)
  }

  // 为 recordId 添加索引（如果尚未创建），确保查询性能
  await tablesCollection.createIndex({ recordId: 1, tableKey: 1, chunkIndex: 1 })
}

/**
 * 保存单张表数据
 */
async function saveSingleTable(db, recordId, tableKey, rows = []) {
  if (!db || !recordId || !tableKey) return

  const tablesCollection = db.collection('analysis_tables')
  const normalizedRows = Array.isArray(rows) ? rows : []
  const now = new Date()

  // 先删除旧数据
  await tablesCollection.deleteMany({ recordId, tableKey })

  if (normalizedRows.length === 0) {
    await tablesCollection.insertOne({
      recordId,
      tableKey,
      chunkIndex: 0,
      rows: [],
      rowCount: 0,
      createdAt: now,
      updatedAt: now,
    })
    return
  }

  const bulkOps = []
  let chunkIndex = 0
  for (let start = 0; start < normalizedRows.length; start += CHUNK_SIZE) {
    const chunkRows = normalizedRows.slice(start, start + CHUNK_SIZE)
    bulkOps.push({
      insertOne: {
        document: {
          recordId,
          tableKey,
          chunkIndex,
          rows: chunkRows,
          rowCount: chunkRows.length,
          createdAt: now,
          updatedAt: now,
        },
      },
    })
    chunkIndex++

    if (bulkOps.length >= 500) {
      await tablesCollection.bulkWrite(bulkOps)
      bulkOps.length = 0
    }
  }

  if (bulkOps.length) {
    await tablesCollection.bulkWrite(bulkOps)
  }
}

/**
 * 读取某条记录的所有表数据
 */
async function loadAllTables(db, recordId) {
  if (!db || !recordId) return {}
  const tablesCollection = db.collection('analysis_tables')
  const cursor = tablesCollection
    .find({ recordId })
    .sort({ tableKey: 1, chunkIndex: 1 })

  const tables = {}
  await cursor.forEach(doc => {
    if (!tables[doc.tableKey]) {
      tables[doc.tableKey] = []
    }
    if (Array.isArray(doc.rows)) {
      tables[doc.tableKey].push(...doc.rows)
    }
  })

  return tables
}

/**
 * 读取某条记录的指定表
 */
async function loadTable(db, recordId, tableKey) {
  if (!db || !recordId || !tableKey) return []
  const tablesCollection = db.collection('analysis_tables')
  const docs = await tablesCollection
    .find({ recordId, tableKey })
    .sort({ chunkIndex: 1 })
    .toArray()

  if (!docs.length) return []
  const rows = []
  docs.forEach(doc => {
    if (Array.isArray(doc.rows)) {
      rows.push(...doc.rows)
    }
  })

  return rows
}

/**
 * 删除某条记录的所有表（可选 tableKey）
 */
async function deleteTables(db, recordId, tableKey) {
  if (!db || !recordId) return
  const tablesCollection = db.collection('analysis_tables')
  const filter = { recordId }
  if (tableKey) filter.tableKey = tableKey
  await tablesCollection.deleteMany(filter)
}

module.exports = {
  saveTables,
  saveSingleTable,
  loadAllTables,
  loadTable,
  deleteTables,
}

