const { readAndCleanData, 
        mergeData, 
        calculateCriminalInfo, 
        mergeCriminalInfo,
        processEmploymentData,
        calculateEmploymentUnit2
    } = require('./dataMergeService')
const { 
  mergeTransactionFolder, 
  detectAbnormalTransactions, 
  flagAbnormalTransaction 
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
const { writeArrayToExcel } = require('./excelService')
const path = require('path')

/**
 * 更新进度（分阶段）
 */
function updateProgress(currentStep, totalSteps, basePercent, targetPercent) {
  const percent = basePercent + (targetPercent - basePercent) * (currentStep / totalSteps)
  progressStore.set(Math.round(percent), '')
}

/**
 * 运行数据处理管道
 * @param {Object} options - 配置选项
 * @param {string} options.RESULT_DIR - 结果目录
 * @param {Map} options.fileMap - 文件路径映射
 */
// pipelineService.js 的骨架结构
async function runPipelineNode({ RESULT_DIR, fileMap }) {
  try {
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
    // 合并数据表
    let mergedData = mergeData({
      criminal: criminalData,
      population: populationData,
      employment: employmentData,
      insurance: insuranceData
    })
    
    // 计算前科信息
    const criminalInfoMap = calculateCriminalInfo(criminalData)

    // 合并前科信息到主表
    mergedData = mergeCriminalInfo(mergedData, criminalInfoMap)

    console.log(`合并完成: ${mergedData.length} 条记录`)

    updateProgress(1, 1, 10, 20)

    // 阶段3: 交易分析 (20-30%)
    updateProgress(0, 1, 20, 30)

    // 合并交易文件夹
    const transactionFolderPath = fileMap.get('transaction_files')
    let transactionData = []
    let abnormalTransactions = []

    if (transactionFolderPath) {
      console.log('开始处理交易数据...')
      transactionData = mergeTransactionFolder(transactionFolderPath)
      
      if (transactionData.length > 0) {
        // 检测异常交易
        abnormalTransactions = detectAbnormalTransactions(transactionData, {
          minAmount: 500,
          minSenders: 1
        })
        
        // 标记主表的异常资金
        mergedData = flagAbnormalTransaction(mergedData, abnormalTransactions)
        
        // 导出交易文件
        const transactionOutputPath = path.join(RESULT_DIR, 'transactions.xlsx')
        writeArrayToExcel(transactionData, transactionOutputPath)
        console.log(`交易文件已导出: ${transactionData.length} 条记录`)
        
        // 导出异常交易文件
        if (abnormalTransactions.length > 0) {
          const abnormalOutputPath = path.join(RESULT_DIR, '可疑收款账号.xlsx')
          writeArrayToExcel(abnormalTransactions, abnormalOutputPath)
          console.log(`异常交易文件已导出: ${abnormalTransactions.length} 个可疑账户`)
        }
      } else {
        console.log('交易数据为空')
        mergedData = flagAbnormalTransaction(mergedData, [])
      }
    } else {
      console.log('未上传交易文件夹')
      mergedData = flagAbnormalTransaction(mergedData, [])
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
        
        // 导出完整购物数据
        const shoppingOutputPath = path.join(RESULT_DIR, 'shopping.xlsx')
        writeArrayToExcel(shoppingResult.allShoppingData, shoppingOutputPath)
        console.log(`购物文件已导出: ${shoppingResult.allShoppingData.length} 条记录`)
        
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
          
          // 导出酒店文件
          const hotelOutputPath = path.join(RESULT_DIR, 'hotel.xlsx')
          writeArrayToExcel(hotelData, hotelOutputPath)
          console.log(`酒店文件已导出: ${hotelData.length} 条记录`)
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

    //中间阶段: 数据字段处理 (60-80%)
    updateProgress(0, 1, 60, 80)

    console.log('开始处理数据字段...')

    // 1. 计算从业单位2（从业单位 + (分类)）
    mergedData = mergedData.map(row => {
      const employer = String(row['从业单位'] || '').trim()
      const category = String(row['分类'] || '').trim()
      const employer2 = category ? `${employer}(${category})` : employer
      
      // 创建新对象
      const newRow = { ...row }
      delete newRow['从业单位']
      delete newRow['分类']
      newRow['从业单位2'] = employer2
      
      return newRow
    })

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

    // 阶段7: 同住人分析 (80-85%)
    updateProgress(0, 1, 80, 85)

    console.log('开始计算同住情况...')

    mergedData = calculateRoommate(roommateData, mergedData)

    console.log('同住人分析完成')

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
    
    if (hotSpotsData.length > 0) {
      const hotSpotsPath = path.join(RESULT_DIR, '高风险地点统计.xlsx')
      writeArrayToExcel(hotSpotsData, hotSpotsPath)
      console.log(`案发地点统计已导出: ${hotSpotsData.length} 条记录`)
    } else {
      console.log('案发地点统计为空')
    }

    updateProgress(0.33, 1, 90, 95)

    // 2. 实口居住地址统计
    console.log('统计实口居住地址热点...')
    const populationSpotsData = analyzePopulationHotSpots(mergedData, {
      addressCol: '居住地址',
      threshold: 2
    })
    
    if (populationSpotsData.length > 0) {
      const populationSpotsPath = path.join(RESULT_DIR, '实口地址高风险统计.xlsx')
      writeArrayToExcel(populationSpotsData, populationSpotsPath)
      console.log(`实口居住地址统计已导出: ${populationSpotsData.length} 条记录`)
    } else {
      console.log('实口居住地址统计为空')
    }

    updateProgress(0.66, 1, 90, 95)

     // 3. 外卖收货地址统计（需要从购物数据中获取）
    console.log('统计外卖收货地址热点...')
    let shoppingSpotsData = []
    
    if (shoppingResult && shoppingResult.allShoppingData && shoppingResult.allShoppingData.length > 0) {
      shoppingSpotsData = analyzeShoppingSpots(shoppingResult.allShoppingData, {
        addressCol: '收货地址',
        threshold: 2
      })
      
      if (shoppingSpotsData.length > 0) {
        const shoppingSpotsPath = path.join(RESULT_DIR, '外卖收货地址高风险统计.xlsx')
        writeArrayToExcel(shoppingSpotsData, shoppingSpotsPath)
        console.log(`外卖收货地址统计已导出: ${shoppingSpotsData.length} 条记录`)
      } else {
        console.log('外卖收货地址统计为空')
      }
    } else {
      console.log('没有购物数据，跳过外卖收货地址统计')
    }

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
        // 第一条记录，直接保存
        criminalMap.set(id, { ...row })
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
      }
    }

    // 转换为数组，用于后续导出
    const summaryData = Array.from(criminalMap.values())
    console.log(`前科人员聚合完成: ${summaryData.length} 条唯一记录`)

    updateProgress(1, 1, 95, 96)
    
    // 阶段11: 最终数据整理和导出 (96-100%)
    updateProgress(0, 1, 96, 100)

    console.log('开始最终数据整理...')

    // 1. 字段重命名（应用到 summaryData）
    const processedSummaryData = summaryData.map(row => {
      const newRow = { ...row }
      
      // 重命名字段
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

    // 只保留存在的列，并正确处理字段类型
    const finalData = processedSummaryData.map(row => {
      const resultRow = {}
      for (const col of resultColumns) {
        // 处理数字字段：确保保留为数字类型
        if (col === '异常资金' || col === '入住次数' || col === '同住男人数') {
          let val = row[col]
          // 转换为数字，空值/NaN/空字符串 转为 0
          if (val === null || val === undefined || val === '' || val === 'NaN' || val === 'nan' || val === 'None') {
            resultRow[col] = 0
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

    // 导出最终结果文件（只包含前科人员）
    const resultPath = path.join(RESULT_DIR, 'result.xlsx')
    writeArrayToExcel(finalData, resultPath)
    console.log(`最终结果文件已导出: ${resultPath}`)

    updateProgress(1, 1, 96, 100)

    //  导出合并数据（包含所有数据，用于调试）
    const mergeOutputPath = path.join(RESULT_DIR, 'merge.xlsx')
    writeArrayToExcel(mergedData, mergeOutputPath)
    console.log(`合并数据已导出: ${mergeOutputPath}`)

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