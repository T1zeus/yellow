const { readExcelToArray } = require('./excelService')
const path = require('path')
const iconv = require('iconv-lite')

/**
 * 清理证件号码：统一格式
 * 处理 .0 后缀、单引号、空格等
 */
function cleanIdNumber(value) {
  if (value == null || value === '') return ''

  const original = String(value)
  let cleaned = original
    .replace(/\.0+$/, '') // 去除 .0 后缀（如：123.0 → 123）
    .replace(/^'/, '') // 去除开头的单引号（如：'123 → 123）
    .replace(/\*/g, '') // 去除星号（部分数据会用 * 替代）
    .replace(/[\s\n\r\u00A0\u3000]+/g, '') // 去除常见空白字符（含全角空格）
    .trim()
    .toUpperCase()

  if (cleaned === '') {
    return ''
  }

  const isValidId = /^\d{17}[\dX]$/.test(cleaned) || /^\d{15}$/.test(cleaned)

  if (!isValidId) {
    console.warn(`【cleanIdNumber】证件号码格式异常，数据将被丢弃: 原值="${original}", 清理后="${cleaned}"`)
    return ''
  }

  return cleaned
}

/**
 * 判断值是否为空（类似 pandas 的 isna）
 */
function isValueEmpty(value) {
  if (value === null || value === undefined) return true
  if (value === '') return true
  if (typeof value === 'string' && value.trim() === '') return true
  if (typeof value === 'string' && (value.toLowerCase() === 'nan' || value.toLowerCase() === 'none')) return true
  return false
}

/**
 * 合并两个行对象（解决列名冲突）
 * 使用 combine_first 逻辑：如果左值为空，使用右值；否则使用左值
 * 对于特定字段（如从业单位、分类），如果两边都有值且不同，则聚合它们
 */
function mergeTwoRows(row1, row2, onColumn) {
  const merged = { ...row1 }

  // 需要聚合的字段（如果两边都有值且不同，则用换行符连接）
  const aggregateFields = ['从业单位', '分类', '商铺地址']
  
  // 收集所有列名
  const allColumns = new Set([...Object.keys(row1), ...Object.keys(row2)])

  for (const col of allColumns) {
    if (col === onColumn) {
      // 合并键保持不变
      merged[col] = row1[col] || row2[col]
      continue
    }

    const val1 = row1[col]
    const val2 = row2[col]

    // 对于需要聚合的字段，如果两边都有值且不同，则聚合
    if (aggregateFields.includes(col)) {
      const str1 = String(val1 || '').trim()
      const str2 = String(val2 || '').trim()
      
      // 处理多条记录的情况：如果已有换行符分隔的值，需要检查是否已包含新值
      if (str1 && str2) {
        // 两边都有值
        if (str1 === str2) {
          // 值相同，使用任意一个
          merged[col] = str1
        } else {
          // 值不同，需要聚合
          // 如果 str1 已经包含换行符（多条记录），检查是否已包含 str2
          if (str1.includes('\n')) {
            const existingValues = str1.split('\n').map(s => s.trim()).filter(s => s)
            if (!existingValues.includes(str2)) {
              // str2 不在现有值中，添加它
              merged[col] = `${str1}\n${str2}`
            } else {
              // str2 已在现有值中，保持原值
              merged[col] = str1
            }
          } else {
            // str1 是单个值，直接连接
            merged[col] = `${str1}\n${str2}`
          }
        }
      } else if (str1) {
        // 左值有值，使用左值
        merged[col] = str1
      } else if (str2) {
        // 左值空，右值有值，使用右值
        merged[col] = str2
      } else {
        // 两边都空
        merged[col] = ''
      }
    } else {
      // 其他字段：combine_first 逻辑：如果 val1 为空，使用 val2；否则使用 val1
      if (isValueEmpty(val1)) {
        merged[col] = val2 !== undefined ? val2 : ''
      } else {
        merged[col] = val1
      }
    }
  }

  return merged
}

/**
 * 合并两个数据表并解决列名冲突
 * @param {Array<Object>} data1 - 第一个数据表
 * @param {Array<Object>} data2 - 第二个数据表
 * @param {Object} options - 配置选项
 * @param {string} options.onColumn - 合并键（默认 '证件号码'）
 * @param {string} options.how - 合并方式：'left' | 'right' | 'outer' | 'inner'（默认 'outer'）
 * @returns {Array<Object>} 合并后的数据
 */
function mergeAndResolveConflicts(data1, data2, options = {}) {
  const { onColumn = '证件号码', how = 'outer' } = options

  if (!data1 || data1.length === 0) return data2 || []
  if (!data2 || data2.length === 0) return data1 || []

  const cleanData1 = data1.map(row => ({
    ...row,
    [onColumn]: cleanIdNumber(row[onColumn]),
  }))
  const cleanData2 = data2.map(row => ({
    ...row,
    [onColumn]: cleanIdNumber(row[onColumn]),
  }))

  const index1 = new Map()
  for (const row of cleanData1) {
    const key = String(row[onColumn] || '')
    if (key && key !== '') {
      if (!index1.has(key)) {
        index1.set(key, [])
      }
      index1.get(key).push(row)
    }
  }

  const index2 = new Map()
  for (const row of cleanData2) {
    const key = String(row[onColumn] || '')
    if (key && key !== '') {
      if (!index2.has(key)) {
        index2.set(key, [])
      }
      index2.get(key).push(row)
    }
  }

  const allKeys = new Set()
  for (const key of index1.keys()) allKeys.add(key)
  for (const key of index2.keys()) allKeys.add(key)

  const merged = []

  for (const key of allKeys) {
    const rows1 = index1.get(key) || []
    const rows2 = index2.get(key) || []

    if (how === 'inner' && (rows1.length === 0 || rows2.length === 0)) {
      continue
    }

    if (how === 'left' && rows1.length === 0) {
      continue
    }

    if (how === 'right' && rows2.length === 0) {
      continue
    }

    if (rows1.length > 0 && rows2.length > 0) {
      for (const r1 of rows1) {
        for (const r2 of rows2) {
          const mergedRow = mergeTwoRows(r1, r2, onColumn)
          merged.push(mergedRow)
        }
      }
    } else if (rows1.length > 0) {
      merged.push(...rows1)
    } else if (rows2.length > 0) {
      merged.push(...rows2)
    }
  }

  return merged
}

/**
 * 合并多个数据表
 * @param {Object} dataSources - 数据源对象
 * @returns {Array<Object>} 合并后的数据
 */
function mergeData(dataSources) {
  const { 
    criminal = [], 
    population = [], 
    employment = [], 
    insurance = [] 
  } = dataSources

  console.log('开始合并数据表...')
  console.log(`刑事案件: ${criminal.length}, 实有人口: ${population.length}, 从业人员: ${employment.length}, 社保: ${insurance.length}`)

  // 按顺序合并：criminal -> population -> employment -> insurance
  let merged = mergeAndResolveConflicts(criminal, population, { onColumn: '证件号码', how: 'outer' })
  merged = mergeAndResolveConflicts(merged, employment, { onColumn: '证件号码', how: 'outer' })
  merged = mergeAndResolveConflicts(merged, insurance, { onColumn: '证件号码', how: 'outer' })

  console.log(`数据合并完成，共 ${merged.length} 条记录`)
  return merged
}


/**
 * 清理数据：标准化证件号码
 * 这是每个数据表读取后都需要做的清理工作
 */
function cleanData(data) {
  if (!data || data.length === 0) return []
  
  const cleanedRows = []
  let skippedMissingId = 0
  let skippedInvalidId = 0
  
  for (const row of data) {
    const cleaned = { ...row }

    if (cleaned['证件号码'] !== undefined && cleaned['证件号码'] !== null && cleaned['证件号码'] !== '') {
      const cleanedId = cleanIdNumber(cleaned['证件号码'])
      if (!cleanedId) {
        skippedInvalidId++
        console.warn(`【cleanData】证件号码无效，跳过该行: ${JSON.stringify(row)}`)
        continue
      }
      cleaned['证件号码'] = cleanedId
    } else {
      skippedMissingId++
      console.warn(`【cleanData】缺少证件号码，跳过该行: ${JSON.stringify(row)}`)
      continue
    }
    
    for (const key in cleaned) {
      if (typeof cleaned[key] === 'string') {
        cleaned[key] = cleaned[key].trim()
      }
    }

    cleanedRows.push(cleaned)
  }
  
  return cleanedRows
}

/**
 * 处理从业数据（特殊清洗）
 * 从业人员数据有特殊格式：
 * 1. 可能有多级表头（第一行是列名）
 * 2. 需要前向填充：分类、商铺地址、从业单位
 * 3. 证件号码可能包含星号，需要清理
 */
function processEmploymentData(employmentData) {
  if (!employmentData || employmentData.length === 0) {
    return []
  }

  // 1. 设置期望的列名
  const expectedColumns = [
    '分类',
    '商铺地址',
    '商铺简称',
    '从业人员明细_姓名',
    '从业人员明细_个人信息',
    '从业人员明细_联系电话',
    '从业人员明细_居住地址',
  ]

  // 如果第一行是列名，跳过它
  let data = [...employmentData]
  if (data.length > 0) {
    // 检查第一行是否可能是表头（包含列名关键词）
    const firstRow = data[0]
    const firstRowValues = Object.values(firstRow).map(v => String(v).toLowerCase())
    const isHeader = expectedColumns.some(col => {
      const colLower = col.toLowerCase()
      return firstRowValues.some(val => val.includes(colLower))
    })

    if (isHeader) {
      // 跳过第一行（表头）
      data = data.slice(1)
    }
  }

  // 2. 列名映射表（支持多种可能的列名）
  const columnMapping = {
    姓名: ['姓名', '从业人员明细_姓名', '从业人员姓名', 'name'],
    证件号码: ['证件号码', '从业人员明细_个人信息', '个人信息', 'id'],
    手机号码: ['手机号码', '从业人员明细_联系电话', '联系电话', 'phone'],
    居住地址: ['居住地址', '从业人员明细_居住地址', 'address'],
    从业单位: ['从业单位', '商铺简称', '单位名称', 'employer'],
    分类: ['分类', 'category'],
    商铺地址: ['商铺地址', 'shop_address'],
  }

  // 3. 处理数据行：映射到标准列名
  const processed = data.map((row) => {
    const newRow = {}

    // 复制所有原始列（保留原始数据，用于调试和备用）
    for (const key of Object.keys(row)) {
      const trimmedKey = key.trim()
      newRow[trimmedKey] = row[key]
      // 同时保留原始字段名（可能包含空格），用于字段映射
      if (trimmedKey !== key) {
        newRow[key] = row[key]
      }
    }

    // 映射到标准列名
    for (const [targetCol, possibleNames] of Object.entries(columnMapping)) {
      let found = false
      for (const possibleName of possibleNames) {
        // 检查原始字段名（可能包含空格或大小写不同）
        const originalKey = Object.keys(row).find(k => {
          const kTrimmed = k.trim()
          return kTrimmed === possibleName.trim() || k === possibleName
        })
        
        if (originalKey !== undefined && row[originalKey] !== undefined) {
          const val = String(row[originalKey]).trim()
          // 过滤无效值
          if (val !== '' && val !== 'undefined' && val !== 'null' && val !== 'NaN') {
            newRow[targetCol] = row[originalKey]
            found = true
            break
          }
        }
        
        // 也检查直接匹配（字段名完全一致）
        if (row[possibleName] !== undefined) {
          const val = String(row[possibleName]).trim()
          if (val !== '' && val !== 'undefined' && val !== 'null' && val !== 'NaN') {
            newRow[targetCol] = row[possibleName]
            found = true
            break
          }
        }
      }
      
      // 如果没找到，设置为空字符串（而不是 undefined）
      if (!found) {
        newRow[targetCol] = ''
      }
    }

    return newRow
  })

  // 4. 过滤：去除姓名为空的行
  let filtered = processed.filter(row => {
    const name = String(row['姓名'] || '').trim()
    return name !== '' && name !== 'undefined' && name !== 'null' && name !== 'NaN'
  })

  // 5. 清理证件号码（保持为字符串，避免精度丢失）
  filtered = filtered.map(row => {
    const rawId = row['证件号码']
    const cleanedId = cleanIdNumber(rawId)

    if (cleanedId && !/^\d{17}[\dX]$/.test(cleanedId) && !/^\d{15}$/.test(cleanedId)) {
      console.warn(`【从业数据】证件号码格式异常: 姓名="${row['姓名'] || ''}", 原值="${rawId}", 清理后="${cleanedId}"`)
    }

    return {
      ...row,
      证件号码: cleanedId,
    }
  })

  // 过滤掉证件号码为空的行
  filtered = filtered.filter(row => {
    const idNum = String(row['证件号码'] || '').trim()
    return idNum !== '' && idNum !== 'undefined' && idNum !== 'null' && idNum !== 'NaN'
  })

  // 6. 前向填充：分类、商铺地址、从业单位
  // 如果当前行的这些字段为空，使用上一行的值
  // 关键：前向填充时，证件号码不应该被填充，必须保持每行的原始证件号码
  let lastCategory = ''
  let lastShopAddress = ''
  let lastEmployer = ''

  filtered = filtered.map(row => {
    // 保存原始证件号码（前向填充不应该影响证件号码）
    const originalIdNumber = String(row['证件号码'] || '').trim()
    
    let category = String(row['分类'] || '').trim()
    let shopAddress = String(row['商铺地址'] || '').trim()
    let employer = String(row['从业单位'] || '').trim()
    
    // 修复：如果从业单位为空，尝试从原始字段中获取（字段映射可能失败）
    if (!employer || employer === 'undefined' || employer === 'null' || employer === '') {
      // 尝试从原始字段中获取
      employer = String(row['商铺简称'] || row['单位名称'] || '').trim()
      if (employer && employer !== 'undefined' && employer !== 'null') {
        // 如果从原始字段获取到值，更新到从业单位字段
        row['从业单位'] = employer
      }
    }

    // 过滤无效值
    if (category === 'undefined' || category === 'null' || category === 'NaN') category = ''
    if (shopAddress === 'undefined' || shopAddress === 'null' || shopAddress === 'NaN') shopAddress = ''
    if (employer === 'undefined' || employer === 'null' || employer === 'NaN') employer = ''

    // 如果当前行有值，更新last值
    if (category) lastCategory = category
    if (shopAddress) lastShopAddress = shopAddress
    if (employer) lastEmployer = employer

    // 如果当前行为空，使用last值
    // 关键：确保证件号码不被前向填充覆盖，保持原始值
    return {
      ...row,
      证件号码: originalIdNumber, // 确保证件号码不被前向填充影响
      分类: category || lastCategory,
      商铺地址: shopAddress || lastShopAddress,
      从业单位: employer || lastEmployer,
    }
  })
  
  // 统计有从业单位信息的记录
  const withEmployer = filtered.filter(row => {
    const emp = String(row['从业单位'] || '').trim()
    return emp !== '' && emp !== 'undefined' && emp !== 'null'
  }).length

  // 7. 最后清理证件号码（使用cleanIdNumber统一格式）
  filtered = filtered.map(row => ({
    ...row,
    证件号码: cleanIdNumber(row['证件号码']),
  }))

  return filtered
}

/**
 * 计算从业单位2字段
 * 格式：从业单位 + (分类)
 * 例如：某足浴店(足浴)
 */
function calculateEmploymentUnit2(mergedData) {
  return mergedData.map(row => {
    const unit = String(row['从业单位'] || '').trim()
    const category = String(row['分类'] || '').trim()
    
    let unit2 = unit
    if (category) {
      unit2 = unit2 ? `${unit}(${category})` : `(${category})`
    }
    
    return {
      ...row,
      从业单位2: unit2,
    }
  })
}

/**
 * 读取并清理数据文件
 * @param {string} filePath - 文件路径
 * @returns {Array<Object>} 清理后的数据
 */
function readAndCleanData(filePath) {
  if (!filePath) {
    console.log('文件路径为空，跳过读取')
    return []
  }
  
  const data = readExcelToArray(filePath)
  const cleaned = cleanData(data)
  
  // 修复：正确处理中文文件名编码（支持 UTF-8 和 GBK）
  let decodedFileName = path.basename(filePath)
  try {
    const fileName = path.basename(filePath)
    let tentative = fileName

    try {
      const gbkDecoded = iconv.decode(Buffer.from(fileName, 'binary'), 'gbk')
      const hasInvalidChars = /[^\u4e00-\u9fa5\w\s\-_.()\[\]（）【】]/g.test(gbkDecoded)
      if (!hasInvalidChars && gbkDecoded !== fileName) {
        tentative = gbkDecoded
      }
    } catch (e) {}

    decodedFileName = tentative
  } catch (e) {
    decodedFileName = path.basename(filePath)
  }
  
  const idSet = new Set()
  const suspiciousIds = []
  for (const row of cleaned) {
    const id = String(row['证件号码'] || '').trim()
    if (id) {
      idSet.add(id)
      if (!( /^\d{17}[\dX]$/.test(id) || /^\d{15}$/.test(id) )) {
        suspiciousIds.push(id)
      }
    }
  }
  if (suspiciousIds.length > 0) {
    console.warn(`【readAndCleanData】检测到可疑证件号: ${JSON.stringify(suspiciousIds.slice(0, 5))}${suspiciousIds.length > 5 ? ' ...' : ''}`)
  }
  
  return cleaned
}

/**
 * 计算前科信息
 * @param {Array<Object>} criminalData - 刑事案件数据
 * @returns {Object} 包含前科信息的 Map，key 是证件号码，value 是前科信息
 */
function calculateCriminalInfo(criminalData) {
  const criminalMap = new Map()
  
  if (!criminalData || criminalData.length === 0) {
    return criminalMap
  }

  // 按证件号码分组统计
  const idGroups = new Map()
  for (const row of criminalData) {
    const id = cleanIdNumber(row['证件号码'] || '')
    if (id && id !== '') {
      if (!idGroups.has(id)) {
        idGroups.set(id, [])
      }
      idGroups.get(id).push(row)
    }
  }

  // 计算每个证件号码的前科信息
  for (const [id, rows] of idGroups.entries()) {
    // 前科人员：有案由则为 1，否则为 0
    const hasCaseReason = rows.some(row => {
      const caseReason = String(row['案由'] || '').trim()
      return caseReason !== '' && caseReason !== 'undefined' && caseReason !== 'null'
    })
    
    // 前科次数：按证件号码统计案由出现次数
    const caseReasons = rows
      .map(row => String(row['案由'] || '').trim())
      .filter(reason => reason !== '' && reason !== 'undefined' && reason !== 'null')
    
    const uniqueCaseReasons = new Set(caseReasons)
    
    criminalMap.set(id, {
      前科人员: hasCaseReason ? 1 : 0,
      前科次数: uniqueCaseReasons.size,
      前科情况: Array.from(uniqueCaseReasons).join('、'), // 用中文顿号连接
    })
  }

  console.log(`前科信息计算完成：${criminalMap.size} 人有前科记录`)
  return criminalMap
}

/**
 * 将前科信息合并到主数据表
 * @param {Array<Object>} mergedData - 合并后的主数据
 * @param {Map} criminalMap - 前科信息 Map
 * @returns {Array<Object>} 添加了前科信息的数据
 */
function mergeCriminalInfo(mergedData, criminalMap) {
  return mergedData.map(row => {
    const id = cleanIdNumber(row['证件号码'] || '')
    const criminalInfo = criminalMap.get(id) || {
      前科人员: 0,
      前科次数: 0,
      前科情况: '',
    }
    
    return {
      ...row,
      前科人员: criminalInfo.前科人员,
      前科次数: criminalInfo.前科次数,
      前科情况: criminalInfo.前科情况,
    }
  })
}

module.exports = {
  cleanIdNumber,
  cleanData,
  readAndCleanData,
  mergeAndResolveConflicts,
  mergeData,
  calculateCriminalInfo,
  mergeCriminalInfo,
  processEmploymentData,      
  calculateEmploymentUnit2
}