const { 
        cleanIdNumber,
        readAndCleanData, 
        mergeData, 
        calculateCriminalInfo, 
        mergeCriminalInfo,
        processEmploymentData,
        calculateEmploymentUnit2
    } = require('./dataMergeService')
const {
  mergeTransactionFolder,
  detectAbnormalTransactions,
  flagAbnormalTransaction,
  enrichTransactionWithIdNumber
} = require('./transactionService')
const { processShoppingData } = require('./shoppingService')
const { processingHotel, mergeHotelData } = require('./hotelService')
const { extractAddressesFromData } = require('./addressService')
const { calculateRoommate } = require('./roommateService') 
const { addAlertLevel } = require('./riskService')
const { 
  analyzeHotSpots, 
  analyzePopulationHotSpots, 
  analyzeShoppingSpots 
} = require('./statisticsService')
const { progressStore } = require('../stores/progressStore')
const { MongoClient } = require('mongodb')
const { saveTables } = require('./mongoTableService')

/**
 * 更新进度（分阶段）
 */
function updateProgress(currentStep, totalSteps, basePercent, targetPercent) {
  const percent = basePercent + (targetPercent - basePercent) * (currentStep / totalSteps)
  progressStore.set(Math.round(percent), '')
}

/**
 * 保存数据到 MongoDB
 * @param {Object} options - 配置选项
 * @param {string} options.recordId - 记录ID（时间戳）
 * @param {Object} options.tables - 表格数据对象
 */
async function saveToMongoDB({ recordId, tables }) {
  const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017'
  const DB_NAME = process.env.MONGO_DB_NAME || 'yellow_db'
  
  const client = new MongoClient(MONGO_URI)
  
  try {
    await client.connect()
    console.log('[MongoDB] 已连接数据库')
    
    const db = client.db(DB_NAME)
    const collection = db.collection('analysis_records')
    
    // 准备统计数据
    const statistics = {}
    const filesMeta = []
    
    const FILE_MAP = [
      { tableKey: 'result', fileName: 'result.xlsx' },
      { tableKey: 'merge', fileName: 'merge.xlsx' },
      { tableKey: 'transactions', fileName: 'transactions.xlsx' },
      { tableKey: 'abnormal_accounts', fileName: '可疑收款账号.xlsx' },
      { tableKey: 'shopping', fileName: 'shopping.xlsx' },
      { tableKey: 'risk_hotspot', fileName: '高风险地点统计.xlsx' },
      { tableKey: 'risk_population', fileName: '实口地址高风险统计.xlsx' },
      { tableKey: 'risk_shopping', fileName: '外卖收货地址高风险统计.xlsx' },
    ]
    
    const now = new Date()
    
    for (const { tableKey, fileName } of FILE_MAP) {
      const data = tables[tableKey] || []
      const count = Array.isArray(data) ? data.length : 0
      
      statistics[`${tableKey}Count`] = count
      
      // 不再从文件系统读取，直接使用当前时间
      filesMeta.push({
        tableKey,
        fileName,
        rowCount: count,
        lastModified: now,
      })
    }
    
    // 准备文档
    const doc = {
      recordId,
      statistics,
      files: filesMeta,
      updatedAt: now,
    }
    
    // 使用 upsert 保存
    await collection.updateOne(
      { recordId },
      {
        $set: {
          ...doc,
          updatedAt: now,
        },
        $setOnInsert: {
          createdAt: now,
        },
      },
      { upsert: true }
    )
    
    await saveTables(db, recordId, tables || {})
    
    console.log(`[MongoDB] 记录已保存: ${recordId}`)
    console.log(`[MongoDB] 统计数据:`, statistics)
  } catch (error) {
    console.error('[MongoDB] 保存失败:', error)
    throw error
  } finally {
    await client.close()
    console.log('[MongoDB] 连接已关闭')
  }
}

/**
 * 运行数据处理管道
 * @param {Object} options - 配置选项
 * @param {Map} options.fileMap - 文件路径映射
 */
async function runPipelineNode({ fileMap }) {
  try {
    // 生成时间戳作为记录ID（格式：YYYYMMDD_HHmmss）
    const now = new Date();
    const timestamp = now.getFullYear().toString() +
      String(now.getMonth() + 1).padStart(2, '0') +
      String(now.getDate()).padStart(2, '0') + '_' +
      String(now.getHours()).padStart(2, '0') +
      String(now.getMinutes()).padStart(2, '0') +
      String(now.getSeconds()).padStart(2, '0');
    
    console.log(`生成记录ID: ${timestamp}`);
    
    // 重置进度
    progressStore.reset()
    
    console.log('========== 开始数据处理 ==========')
    // 阶段1: 读取数据 (0-10%)
    updateProgress(0, 1, 0, 10)
    const criminalData = readAndCleanData(fileMap.get('criminal_file'))
    const populationData = readAndCleanData(fileMap.get('population_file'))
    const insuranceData = readAndCleanData(fileMap.get('insurance_file'))
    const roommateData = readAndCleanData(fileMap.get('roommate_file'))

    // 从业数据需要特殊处理
    const employmentRawData = readAndCleanData(fileMap.get('employment_file'))
    const employmentData = processEmploymentData(employmentRawData)

    console.log(`读取完成:`)
    console.log(`- 刑事案件: ${criminalData.length} 条`)
    console.log(`- 实有人口: ${populationData.length} 条`)
    console.log(`- 从业人员: ${employmentData.length} 条`)
    console.log(`- 社保: ${insuranceData.length} 条`)
    console.log(`- 同住人: ${roommateData.length} 条`)

    updateProgress(1, 1, 0, 10)

    // 阶段2: 数据清洗和合并 (10-20%)
    updateProgress(0, 1, 10, 20)
    
    // 调试：统计合并前的从业单位信息
    const employmentWithEmployer = employmentData.filter(r => {
      const emp = String(r['从业单位'] || '').trim()
      return emp !== '' && emp !== 'undefined' && emp !== 'null'
    }).length
    console.log(`合并前从业人员数据: ${employmentData.length} 条，其中 ${employmentWithEmployer} 条有从业单位信息`)
    
    // 合并数据表
    let mergedData = mergeData({
      criminal: criminalData,
      population: populationData,
      employment: employmentData,
      insurance: insuranceData
    })
    
    // 调试：统计合并后的从业单位信息
    const mergedWithEmployer = mergedData.filter(r => {
      const emp = String(r['从业单位'] || '').trim()
      return emp !== '' && emp !== 'undefined' && emp !== 'null'
    }).length
    console.log(`合并后数据: ${mergedData.length} 条，其中 ${mergedWithEmployer} 条有从业单位信息`)
    
    // 计算前科信息
    const criminalInfoMap = calculateCriminalInfo(criminalData)

    // 合并前科信息到主表
    mergedData = mergeCriminalInfo(mergedData, criminalInfoMap)

    console.log(`合并完成: ${mergedData.length} 条记录`)

    // 修复：立即创建从业单位2（与 antiporn 逻辑一致）
    // antiporn: 在数据合并后立即创建 从业单位2 = 从业单位 + (分类)
    // 注意：如果一个人有多条从业记录，从业单位和分类可能包含换行符分隔的多个值
    let employer2Count = 0
    mergedData = mergedData.map(row => {
      const employer = String(row['从业单位'] || '').trim()
      const category = String(row['分类'] || '').trim()
      
      // 处理多条从业记录的情况（用换行符分隔）
      // 如果从业单位或分类包含换行符，需要分别处理每一对
      const employers = employer ? employer.split('\n').filter(e => e.trim()) : []
      const categories = category ? category.split('\n').filter(c => c.trim()) : []
      
      // 创建从业单位2列表
      const employer2List = []
      
      if (employers.length > 0) {
        // 如果有多个从业单位，为每个从业单位创建从业单位2
        for (let i = 0; i < employers.length; i++) {
          const emp = employers[i].trim()
          const cat = categories[i] ? categories[i].trim() : (categories.length > 0 ? categories[0].trim() : '')
          
          if (cat) {
            employer2List.push(`${emp}(${cat})`)
          } else {
            employer2List.push(emp)
          }
        }
      } else if (categories.length > 0) {
        // 如果没有从业单位但有分类，只显示分类
        for (const cat of categories) {
          employer2List.push(`(${cat.trim()})`)
        }
      }
      
      // 用换行符连接多个从业单位2
      const employer2 = employer2List.join('\n')
      
      if (employer2) employer2Count++
      
      const newRow = { ...row }
      // 删除原字段
      delete newRow['从业单位']
      delete newRow['分类']
      // 设置新字段
      newRow['从业单位2'] = employer2
      
      return newRow
    })

    // 统计有多条从业记录的情况
    const multiEmployerCount = mergedData.filter(row => {
      const employer2 = String(row['从业单位2'] || '').trim()
      return employer2.includes('\n')
    }).length
    
    console.log(`从业单位2字段已创建: ${employer2Count} 条记录有从业单位信息`)
    if (multiEmployerCount > 0) {
      console.log(`- 其中 ${multiEmployerCount} 条记录有多条从业单位信息`)
    }

    updateProgress(1, 1, 10, 20)

    // 阶段3: 交易分析 (20-30%)
    updateProgress(0, 1, 20, 30)

    // 合并交易文件夹（只检测异常交易，不立即标记，在案发地点提取后再标记）
    const transactionFolderPath = fileMap.get('transaction_files')
    let transactionData = []
    let abnormalTransactions = []

    if (transactionFolderPath) {
      console.log('开始处理交易数据...')
      transactionData = mergeTransactionFolder(transactionFolderPath)
      
      if (transactionData.length > 0) {
        // ========== 【新增】对交易数据按时间排序（与 antiporn 逻辑一致） ==========
        console.log('开始处理交易时间并排序...')
        transactionData = transactionData.map(row => {
          const timeStr = String(row['交易时间'] || '').trim()
          let timeDt = null
          
          if (timeStr) {
            try {
              // 尝试解析各种时间格式
              // 支持格式：YYYY-MM-DD HH:mm:ss, YYYY/MM/DD, 等
              const parsedDate = new Date(timeStr)
              if (!isNaN(parsedDate.getTime())) {
                timeDt = parsedDate
              }
            } catch (e) {
              // 解析失败，保持为 null
            }
          }
          
          return {
            ...row,
            交易时间_dt: timeDt
          }
        })
        
        // 按时间降序排序（最新的在前，与 antiporn 一致）
        transactionData.sort((a, b) => {
          const timeA = a.交易时间_dt
          const timeB = b.交易时间_dt
          
          // 如果都没有时间，保持原顺序
          if (!timeA && !timeB) return 0
          // 没有时间的排在后面
          if (!timeA) return 1
          if (!timeB) return -1
          // 降序排序（最新的在前）
          return timeB.getTime() - timeA.getTime()
        })
        
        console.log(`交易数据排序完成，共 ${transactionData.length} 条记录`)
        // ========== 【新增结束】 ==========
        
        // ========== 【新增】通过主表数据补充交易数据中的证件号码 ==========
        // 检查是否有缺少证件号码的交易记录
        const missingIdCount = transactionData.filter(row => {
          const id = cleanIdNumber(row['证件号码'] || '')
          return !id || id === ''
        }).length
        
        if (missingIdCount > 0 && mergedData.length > 0) {
          console.log(`发现 ${missingIdCount} 条交易记录缺少证件号码，尝试通过支付账号匹配...`)
          transactionData = enrichTransactionWithIdNumber(transactionData, mergedData)
        }
        // ========== 【新增结束】 ==========
        
        // 检测异常交易（但不立即标记到主表）
        abnormalTransactions = detectAbnormalTransactions(transactionData, {
          minAmount: 500,
          minSenders: 1
        })
      } else {
        console.log('交易数据为空')
      }
      
      // 数据将直接保存到 MongoDB，不再导出文件
      console.log(`交易数据处理完成: ${transactionData.length} 条记录`)
      console.log(`异常交易处理完成: ${abnormalTransactions.length} 个可疑账户`)
    } else {
      console.log('未上传交易文件夹')
      // 数据将直接保存到 MongoDB，不再导出文件
      console.log('交易数据处理完成: 0 条记录')
      console.log('异常交易处理完成: 0 个可疑账户')
    }
    
    updateProgress(1, 1, 20, 30)

    // 阶段4: 购物分析 (30-40%)
    updateProgress(0, 1, 30, 40)

    const shoppingFilePath = fileMap.get('shopping_file')
    let shoppingResult = null
    let groupedSensitiveData = []
    let groupedShoppingData = []

     if (shoppingFilePath) {
      console.log('开始处理购物数据...')
      const shoppingData = readAndCleanData(shoppingFilePath)
      
      if (shoppingData.length > 0) {
        shoppingResult = await processShoppingData(shoppingData)
        
        // 数据将直接保存到 MongoDB，不再导出文件
        console.log(`购物数据处理完成: ${shoppingResult.allShoppingData.length} 条记录`)
        
        groupedSensitiveData = shoppingResult.groupedSensitiveData
        groupedShoppingData = shoppingResult.groupedShoppingData
        
        // 合并敏感商品信息到主表
        const sensitiveMap = new Map()
        for (const item of groupedSensitiveData) {
          sensitiveMap.set(String(item.证件号码 || '').trim(), item)
        }
        
        mergedData = mergedData.map(row => {
          const id = String(row['证件号码'] || '').trim()
          const sensitiveInfo = sensitiveMap.get(id)
          
          if (sensitiveInfo) {
            // 如果异常购物是 '0'，应该转换为空字符串（与 antiporn 的 None 对应）
            const 异常购物 = sensitiveInfo.异常购物 === '0' || sensitiveInfo.异常购物 === 0 ? '' : sensitiveInfo.异常购物
            return {
              ...row,
              异常购物: 异常购物,
              异常购物_等级: sensitiveInfo.异常购物_等级,
              商品名称详细: sensitiveInfo.商品名称详细 || '',
            }
          } else {
            return {
              ...row,
              异常购物: '',
              异常购物_等级: '',
              商品名称详细: '',
            }
          }
        })

        // 合并收货地址信息到主表
        const shoppingMap = new Map()
        for (const item of groupedShoppingData) {
          shoppingMap.set(String(item.证件号码 || '').trim(), item)
        }
        
        mergedData = mergedData.map(row => {
          const id = String(row['证件号码'] || '').trim()
          const shoppingInfo = shoppingMap.get(id)
          
          if (shoppingInfo) {
            return {
              ...row,
              收货地址详细: shoppingInfo.收货地址详细 || '',
              收货地址分类: shoppingInfo.收货地址分类 || '',
            }
          } else {
            return {
              ...row,
              收货地址详细: '',
              收货地址分类: '',
            }
          }
        })
        
      } else {
        console.log('购物数据为空')
        // 添加默认字段
        mergedData = mergedData.map(row => ({
          ...row,
          异常购物: '',
          异常购物_等级: '',
          商品名称详细: '',
          收货地址详细: '',
          收货地址分类: '',
        }))
      }
    } else {
      console.log('未上传购物文件')
      // 添加默认字段
      mergedData = mergedData.map(row => ({
        ...row,
        异常购物: '',
        异常购物_等级: '',
        商品名称详细: '',
        收货地址详细: '',
        收货地址分类: '',
      }))
    }
    
    updateProgress(1, 1, 30, 40)

    // 阶段5: 酒店数据处理 (40-50%)
    updateProgress(0, 1, 40, 50)

    const hotelFolderPath = fileMap.get('hotel_files')
    let hotelData = []

    if (hotelFolderPath) {
      try {
        console.log('开始处理酒店入住数据...')
        hotelData = processingHotel(hotelFolderPath)
        console.log(`酒店数据处理完成: ${hotelData.length} 条记录`)

        // 合并酒店数据到主表
        if (hotelData.length > 0) {
          console.log('合并酒店数据到主表...')
          mergedData = mergeHotelData(mergedData, hotelData)
          
          // 注意：不导出 hotel.xlsx（与 antiporn 一致，antiporn 不导出此文件）
        } else {
          // 修复：即使没有酒店数据，也要添加默认字段
          mergedData = mergeHotelData(mergedData, [])
        }
      } catch (error) {
        console.error('酒店数据处理失败:', error.message)
        // 即使失败也添加默认列
        mergedData = mergeHotelData(mergedData, [])
      }
    } else {
      console.log('未上传酒店文件夹')
      // 修复：即使没有酒店文件夹，也要添加默认字段
      mergedData = mergeHotelData(mergedData, [])
    }

    updateProgress(1, 1, 40, 50)

    // 阶段6：地址提取与分类 （50-60%）
    updateProgress(0, 1, 50, 60)

    if (mergedData.length > 0) {
      console.log('开始提取案发地点...')
      
      // 使用进度回调实时更新进度
      mergedData = await extractAddressesFromData(mergedData, (current, total) => {
        // 进度范围：50-60%
        // 逐行处理，实时更新
        updateProgress(current, total, 50, 60)
      })

      console.log(`地址提取完成: ${mergedData.length} 条记录`)
      
      // 统计提取结果
      const extractedCount = mergedData.filter(row => row['案发地点'] && row['案发地点'].trim() !== '').length
      console.log(`成功提取地址: ${extractedCount} 条`)
    } else {
      console.log('没有数据需要提取地址')
      // 即使没有数据，也添加默认列
      mergedData = mergedData.map(row => ({
        ...row,
        案发地点: '',
        地点分类: '',
      }))
    }

    updateProgress(1, 1, 50, 60)

    // 修复：在案发地点提取之后标记异常资金（与 antiporn 逻辑一致）
    console.log('开始标记异常资金...')
    console.log(`异常交易数据: ${abnormalTransactions.length} 条`)
    mergedData = flagAbnormalTransaction(mergedData, abnormalTransactions)
    
    // 统计异常资金标记结果
    const abnormalFundCount = mergedData.filter(row => {
      const val = row['异常资金']
      return val === 1 || val === '1' || (typeof val === 'number' && val === 1)
    }).length
    console.log(`异常资金标记完成: ${abnormalFundCount} 条记录标记为异常资金`)

    //中间阶段: 数据字段处理 (60-80%)
    updateProgress(0, 1, 60, 80)

    console.log('开始处理数据字段...')

    // 注意：从业单位2已在数据合并后创建，这里不再重复创建

    updateProgress(0.2, 1, 60, 80)

    // 2. 计算前科信息（前科人员、前科情况、前科次数）
    // 前科人员 = 是否有案由（0/1）
    mergedData = mergedData.map(row => {
      const hasCaseReason = row['案由'] && String(row['案由']).trim() !== ''
      return {
        ...row,
        前科人员: hasCaseReason ? 1 : 0,
        前科情况: hasCaseReason ? 1 : 0,
      }
    })

    updateProgress(0.4, 1, 60, 80)

    // 计算前科次数（按证件号码统计）
    const criminalCounts = new Map()
    for (const row of mergedData) {
      if (row['前科情况'] === 1) {
        const id = String(row['证件号码'] || '').trim()
        criminalCounts.set(id, (criminalCounts.get(id) || 0) + 1)
      }
    }

    mergedData = mergedData.map(row => {
      const id = String(row['证件号码'] || '').trim()
      return {
        ...row,
        前科次数: criminalCounts.get(id) || 0,
      }
    })

    updateProgress(0.6, 1, 60, 80)

    // 3. 添加更新时间
    const currentDate = new Date().toISOString().split('T')[0] // YYYY-MM-DD
    mergedData = mergedData.map(row => ({
      ...row,
      更新时间: currentDate,
    }))

    updateProgress(0.8, 1, 60, 80)

    // 4. 处理所属辖区（从实口所属派出所中提取）
    mergedData = mergedData.map(row => {
      const station = String(row['实口所属派出所'] || '').trim()
      const district = station.replace(/.*分局/, '').trim()
      return {
        ...row,
        所属辖区: district,
      }
    })

    updateProgress(1, 1, 60, 80)

    console.log('数据字段处理完成')

    // 阶段7: 字段重命名和同住人分析 (80-85%)
    updateProgress(0, 1, 80, 85)

    console.log('开始字段重命名...')
    
    // 修复：在导出 merge.xlsx 之前进行字段重命名（与 antiporn 一致）
    // antiporn: 在 merge 酒店数据后，立即进行字段重命名，然后计算同住人，最后导出 merge.xlsx
    mergedData = mergedData.map(row => {
      const newRow = { ...row }
      
      // 重命名字段（与 antiporn 的 rename 逻辑一致）
      if (newRow['单位名称'] !== undefined) {
        newRow['社保情况'] = newRow['单位名称']
        delete newRow['单位名称']
      }
      
      if (newRow['实口户籍地址'] !== undefined) {
        newRow['户籍地址'] = newRow['实口户籍地址']
        delete newRow['实口户籍地址']
      }
      
      if (newRow['实口居住地址'] !== undefined && !newRow['居住地址']) {
        newRow['居住地址'] = newRow['实口居住地址']
        delete newRow['实口居住地址']
      }
      
      if (newRow['从业单位2'] !== undefined) {
        newRow['从业单位'] = newRow['从业单位2']
        delete newRow['从业单位2']
      }
      
      return newRow
    })
    
    console.log('字段重命名完成')

    console.log('开始计算同住情况...')
    mergedData = calculateRoommate(roommateData, mergedData)
    console.log('同住人分析完成')

    // 准备 merge 数据（用于保存到 MongoDB）
    // 定义 merge 需要包含的字段（与 antiporn 的 columns_to_keep 一致）
    const mergeColumns = [
      '姓名',
      '证件类型',
      '证件号码',
      '手机号码',
      '户籍地址',
      '居住地址',
      '异常资金',
      '异常购物',
      '案发地点',
      '地点分类',
      '居住情况',
      '居住详细',
      '所属辖区',
      '案由',
      '简要案情',
      '商品名称详细',
      '社保情况',
      '从业单位',
      '前科人员',
      '前科情况',
      '前科次数',
      '入住次数',
      '同住男人数',
      '同住信息',
      '入住信息',
      '收货地址分类',
      '收货地址详细',
      '异常购物_等级',
      '更新时间',
    ]
    
    // 准备 merge 数据：只包含指定字段，确保字段顺序一致
    // 注意：此时字段重命名已完成，mergedData 中的字段名已经是最终名称
    const mergeDataForExport = mergedData.map(row => {
      const mergeRow = {}
      for (const col of mergeColumns) {
        // 确保字段存在，如果不存在则为空字符串
        mergeRow[col] = row[col] !== undefined ? row[col] : ''
      }
      return mergeRow
    })
    
    // 数据将直接保存到 MongoDB，不再导出文件
    console.log(`merge 数据处理完成: ${mergeDataForExport.length} 条记录，包含 ${mergeColumns.length} 个字段`)

    updateProgress(1, 1, 80, 85)

    // 阶段8: 风险等级评估 （85-90%）
    updateProgress(0, 1, 85, 90)

    console.log('开始计算预警等级...')

    // 计算预警等级（需要同住人数据用于检查前科）
    mergedData = addAlertLevel(mergedData, roommateData)
    
    console.log('风险评估完成')

    updateProgress(1, 1, 85, 90)

    //阶段9: 热点统计 (90-95%)
    updateProgress(0, 1, 90, 95)

    console.log('开始热点统计...')

    // 1. 案发地点统计
    console.log('统计案发地点热点...')
    const hotSpotsData = analyzeHotSpots(mergedData, {
      column: '案发地点',
      threshold: 2
    })
    
    // 数据将直接保存到 MongoDB，不再导出文件
    console.log(`案发地点统计完成: ${hotSpotsData.length} 条记录`)

    updateProgress(0.33, 1, 90, 95)

    // 2. 实口居住地址统计
    console.log('统计实口居住地址热点...')
    const populationSpotsData = analyzePopulationHotSpots(mergedData, {
      addressCol: '居住地址',
      threshold: 2
    })
    
    // 数据将直接保存到 MongoDB，不再导出文件
    console.log(`实口居住地址统计完成: ${populationSpotsData.length} 条记录`)

    updateProgress(0.66, 1, 90, 95)

     // 3. 外卖收货地址统计（需要从购物数据中获取）
    console.log('统计外卖收货地址热点...')
    let shoppingSpotsData = []
    
    if (shoppingResult && shoppingResult.allShoppingData && shoppingResult.allShoppingData.length > 0) {
      shoppingSpotsData = analyzeShoppingSpots(shoppingResult.allShoppingData, {
        addressCol: '收货地址',
        threshold: 2
      })
    } else {
      console.log('没有购物数据，使用空数据')
    }
    
    // 数据将直接保存到 MongoDB，不再导出文件
    console.log(`外卖收货地址统计完成: ${shoppingSpotsData.length} 条记录`)

    updateProgress(1, 1, 90, 95)
    
    // 阶段10: 过滤前科人员并聚合 (95-96%)
    updateProgress(0, 1, 95, 96)

    console.log('开始过滤前科人员...')
    
    // 只保留前科人员（前科人员 === 1）
    const filteredCriminalData = mergedData.filter(row => row['前科人员'] === 1)
    console.log(`前科人员过滤完成: ${mergedData.length} -> ${filteredCriminalData.length} 条记录`)

    // 按证件号码聚合（同一证件号码的多条记录合并）
    const criminalMap = new Map()
    for (const row of filteredCriminalData) {
      const id = String(row['证件号码'] || '').trim()
      if (!id) continue
      
      if (!criminalMap.has(id)) {
        // 第一条记录，直接保存（使用深拷贝确保所有字段都被保留）
        criminalMap.set(id, JSON.parse(JSON.stringify(row)))
      } else {
        // 已存在，合并案发地点和简要案情（用换行符连接）
        const existing = criminalMap.get(id)
        
        // 合并案发地点
        const existingLocation = String(existing['案发地点'] || '').trim()
        const newLocation = String(row['案发地点'] || '').trim()
        if (newLocation && newLocation !== existingLocation) {
          if (existingLocation) {
            existing['案发地点'] = `${newLocation}\n${existingLocation}`
          } else {
            existing['案发地点'] = newLocation
          }
        }

        // 合并简要案情
        const existingCase = String(existing['简要案情'] || '').trim()
        const newCase = String(row['简要案情'] || '').trim()
        if (newCase && newCase !== existingCase) {
          if (existingCase) {
            existing['简要案情'] = `${newCase}\n${existingCase}`
          } else {
            existing['简要案情'] = newCase
          }
        }
        
        // 确保其他字段（如异常资金、从业单位等）被保留
        // 注意：字段重命名已在阶段7完成，这里使用最终字段名（从业单位，不是从业单位2）
        // 对于关键字段，优先保留非空值
        for (const key of Object.keys(row)) {
          if (key !== '案发地点' && key !== '简要案情' && key !== '证件号码') {
            const existingVal = existing[key]
            const newVal = row[key]
            
            // 对于数字字段（异常资金、入住次数等），0 是有效值，不应该被替换
            const isNumericField = ['异常资金', '入住次数', '同住男人数', '前科次数', '前科人员', '前科情况'].includes(key)
            
            if (isNumericField) {
              // 数字字段：如果现有值为 undefined/null/空字符串，使用新值（包括0）
              if ((existingVal === undefined || existingVal === null || existingVal === '') && 
                  (newVal !== undefined && newVal !== null && newVal !== '')) {
                existing[key] = newVal
              } else if (typeof existingVal === 'number' && existingVal === 0 && 
                         typeof newVal === 'number' && newVal !== 0) {
                // 如果现有值是0，新值不是0，使用新值
                existing[key] = newVal
              }
            } else {
              // 字符串字段：如果现有值为空，使用新值
              // 对于从业单位等关键字段，优先保留非空值
              if ((existingVal === undefined || existingVal === null || existingVal === '') && 
                  (newVal !== undefined && newVal !== null && newVal !== '')) {
                existing[key] = newVal
              } else if (key === '从业单位' && existingVal && !newVal) {
                // 如果现有值有从业单位，新值没有，保持现有值
                // 不做任何操作
              } else if (key === '从业单位' && !existingVal && newVal) {
                // 如果现有值没有从业单位，新值有，使用新值
                existing[key] = newVal
              }
            }
          }
        }
      }
    } 

    // 转换为数组，用于后续导出
    const summaryData = Array.from(criminalMap.values())
    
    // 统计聚合后的数据质量（注意：字段重命名已在阶段7完成，这里使用最终字段名）
    const hasEmployer = summaryData.filter(row => row['从业单位'] && String(row['从业单位']).trim() !== '').length
    const hasAbnormalFund = summaryData.filter(row => {
      const val = row['异常资金']
      return val === 1 || val === '1' || (typeof val === 'number' && val === 1)
    }).length
    
    console.log(`前科人员聚合完成: ${summaryData.length} 条唯一记录`)
    console.log(`- 有从业单位信息: ${hasEmployer} 条`)
    console.log(`- 有异常资金: ${hasAbnormalFund} 条`)

    updateProgress(1, 1, 95, 96)
    
    // 阶段11: 最终数据整理和导出 (96-100%)
    updateProgress(0, 1, 96, 100)

    console.log('开始最终数据整理...')

    // 注意：字段重命名已在阶段7完成，summaryData 中的字段名已经是最终名称
    // 这里只需要处理数据格式（数字字段类型等）
    const processedSummaryData = summaryData.map(row => {
      const newRow = { ...row }
      // 字段重命名已在阶段7完成，这里不需要再次重命名
      return newRow
    })

    // 统计字段重命名后的数据质量
    const hasEmployerAfterRename = processedSummaryData.filter(row => row['从业单位'] && String(row['从业单位']).trim() !== '').length
    const hasAbnormalFundAfterRename = processedSummaryData.filter(row => {
      const val = row['异常资金']
      return val === 1 || val === '1' || (typeof val === 'number' && val === 1)
    }).length
    console.log(`字段重命名完成:`)
    console.log(`- 有从业单位信息: ${hasEmployerAfterRename} 条`)
    console.log(`- 有异常资金: ${hasAbnormalFundAfterRename} 条`)

    updateProgress(0.5, 1, 96, 100)

    // 2. 导出最终结果文件（包含特定列）
    const resultColumns = [
      '姓名',
      '证件类型',
      '证件号码',
      '手机号码',
      '户籍地址',
      '居住地址',
      '异常资金',
      '异常购物',
      '案发地点',
      '地点分类',
      '居住情况',
      '居住详细',
      '所属辖区',
      '案由',
      '简要案情',
      '商品名称详细',
      '社保情况',
      '从业单位',
      '前科人员',
      '前科情况',
      '前科次数',
      '入住次数',
      '同住男人数',
      '同住信息',
      '入住信息',
      '收货地址分类',
      '收货地址详细',
      '异常购物_等级',
      '预警状态',
      '资金备注',
      '更新时间',
    ]

    // 处理最终数据，确保数字字段类型正确
    const finalData = processedSummaryData.map(row => {
      const resultRow = {}
      for (const col of resultColumns) {
        // 处理数字字段：确保保留为数字类型
        if (col === '异常资金' || col === '入住次数' || col === '同住男人数') {
          let val = row[col]
          // 转换为数字，空值/NaN/空字符串 转为 0
          if (val === null || val === undefined || val === '' || val === 'NaN' || val === 'nan' || val === 'None') {
            resultRow[col] = 0  // 确保是数字 0，不是字符串
          } else if (typeof val === 'number') {
            // 已经是数字，直接使用（包括 0）
            resultRow[col] = isNaN(val) ? 0 : val
          } else {
            // 字符串，尝试转换为数字
            const numVal = parseInt(String(val), 10)
            resultRow[col] = isNaN(numVal) ? 0 : numVal
          }
        } else {
          // 处理字符串字段
          let val = row[col]
          if (val === null || val === undefined) {
            resultRow[col] = ''
          } else if (typeof val === 'number' && isNaN(val)) {
            resultRow[col] = ''
          } else if (String(val).toLowerCase() === 'nan' || String(val).toLowerCase() === 'none') {
            resultRow[col] = ''
          } else {
            resultRow[col] = String(val)
          }
        }
      }
      return resultRow
    })

    // 数据将直接保存到 MongoDB，不再导出文件
    console.log(`最终结果数据处理完成: ${finalData.length} 条记录`)

    updateProgress(1, 1, 96, 100)

    // 注意：merge.xlsx 已在阶段7（同住人分析之后，预警状态之前）导出，这里不再重复导出

    // 阶段12: 写入 MongoDB (100%)
    console.log('开始写入 MongoDB...')
    try {
      await saveToMongoDB({
        recordId: timestamp,
        tables: {
          result: finalData,
          merge: mergeDataForExport,
          transactions: transactionData || [],
          abnormal_accounts: abnormalTransactions || [],
          shopping: shoppingResult?.allShoppingData || [],
          risk_hotspot: hotSpotsData || [],
          risk_population: populationSpotsData || [],
          risk_shopping: shoppingSpotsData || [],
        },
      })
      console.log('MongoDB 写入完成')
    } catch (mongoError) {
      console.error('MongoDB 写入失败:', mongoError.message)
      throw mongoError // 如果完全依赖 MongoDB，这里应该抛出错误
    }

    // 完成
    progressStore.set(100, '')
    console.log('========== 数据处理管道执行完成 ==========')
    console.log(`最终数据（前科人员）: ${finalData.length} 条`)
    console.log(`合并数据（全部）: ${mergedData.length} 条`)
    console.log(`异常交易: ${abnormalTransactions.length} 个`)
    console.log(`敏感商品: ${groupedSensitiveData.length} 个`)
    console.log(`高风险地点: ${hotSpotsData.length} 个`)
    console.log(`实口地址热点: ${populationSpotsData.length} 个`)
    console.log(`外卖地址热点: ${shoppingSpotsData.length} 个`)

  } catch (error) {
    console.error('管道执行错误:', error)
    progressStore.set(-1, String(error?.message || error))
    throw error
  }
}

module.exports = {
  runPipelineNode
}