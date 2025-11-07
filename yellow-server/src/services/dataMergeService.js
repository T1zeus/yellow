const { readExcelToArray } = require('./excelService')
const path = require('path')
const iconv = require('iconv-lite')

/**
 * 清理证件号码：统一格式
 * 处理 .0 后缀、单引号、空格等
 */
function cleanIdNumber(value) {
  if (value == null || value === '') return ''
  let cleaned = String(value)
    .replace(/\.0+$/, '')      // 去除 .0 后缀（如：123.0 → 123）
    .replace(/^'/, '')        // 去除开头的单引号（如：'123 → 123）
    .replace(/\s+/g, '')      // 去除所有空格
    .replace(/\n/g, '')       // 去除换行
    .replace(/\r/g, '')       // 去除回车
    .trim()
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
 */
function mergeTwoRows(row1, row2, onColumn) {
  const merged = { ...row1 }

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

    // combine_first 逻辑：如果 val1 为空，使用 val2；否则使用 val1
    if (isValueEmpty(val1)) {
      merged[col] = val2 !== undefined ? val2 : ''
    } else {
      merged[col] = val1
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

  // 清理合并键：统一格式
  const cleanData1 = data1.map(row => ({
    ...row,
    [onColumn]: cleanIdNumber(row[onColumn]),
  }))
  const cleanData2 = data2.map(row => ({
    ...row,
    [onColumn]: cleanIdNumber(row[onColumn]),
  }))

  // 创建索引：以证件号码为键
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

  // 收集所有唯一的键
  const allKeys = new Set()
  for (const key of index1.keys()) allKeys.add(key)
  for (const key of index2.keys()) allKeys.add(key)

  // 合并结果（类似 pandas 的 merge）
  const merged = []

  for (const key of allKeys) {
    const rows1 = index1.get(key) || []
    const rows2 = index2.get(key) || []

    // 根据合并方式决定如何处理
    if (how === 'inner' && (rows1.length === 0 || rows2.length === 0)) {
      continue // inner join：只保留两边都有的
    }

    if (how === 'left' && rows1.length === 0) {
      continue // left join：只保留 data1 中的
    }

    if (how === 'right' && rows2.length === 0) {
      continue // right join：只保留 data2 中的
    }

    // pandas merge 的行为：
    // - 如果两边都有数据，做笛卡尔积（每条左行与每条右行合并）
    // - 如果只有一边有数据，直接保留
    if (rows1.length > 0 && rows2.length > 0) {
      // 笛卡尔积合并
      for (const r1 of rows1) {
        for (const r2 of rows2) {
          merged.push(mergeTwoRows(r1, r2, onColumn))
        }
      }
    } else if (rows1.length > 0) {
      // 只有 data1 有数据，直接保留
      merged.push(...rows1)
    } else if (rows2.length > 0) {
      // 只有 data2 有数据，直接保留
      merged.push(...rows2)
    }
  }

// 去重：如果同一个证件号码有多条记录，合并它们
  const resultMap = new Map()
  for (const row of merged) {
    const key = String(row[onColumn] || '')
    if (key && key !== '') {
      if (!resultMap.has(key)) {
        resultMap.set(key, row)
      } else {
        // 如果已存在，合并相同列（combine_first：左值优先，空值用右值填充）
        const existing = resultMap.get(key)
        const mergedRow = mergeTwoRows(existing, row, onColumn)
        resultMap.set(key, mergedRow)
      }
    }
  }

  const result = Array.from(resultMap.values())
  console.log(`合并完成：${data1.length} + ${data2.length} -> ${result.length} 条记录`)
  return result
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
  
  return data.map(row => {
    const cleaned = { ...row }
    
    // 清理证件号码
    if (cleaned['证件号码']) {
      cleaned['证件号码'] = cleanIdNumber(cleaned['证件号码'])
    }
    
    // 清理所有字符串字段（去除首尾空格）
    for (const key in cleaned) {
      if (typeof cleaned[key] === 'string') {
        cleaned[key] = cleaned[key].trim()
      }
    }
    
    return cleaned
  })
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

    // 复制所有原始列
    for (const key of Object.keys(row)) {
      newRow[key.trim()] = row[key]
    }

    // 映射到标准列名
    for (const [targetCol, possibleNames] of Object.entries(columnMapping)) {
      for (const possibleName of possibleNames) {
        if (row[possibleName] !== undefined) {
          newRow[targetCol] = row[possibleName]
          break
        }
      }
    }

    return newRow
  })

  // 4. 过滤：去除姓名为空的行
  let filtered = processed.filter(row => {
    const name = String(row['姓名'] || '').trim()
    return name !== '' && name !== 'undefined' && name !== 'null' && name !== 'NaN'
  })

  // 5. 清理证件号码（去除星号，转换为数字）
  filtered = filtered.map(row => {
    let idNum = String(row['证件号码'] || '')
      .replace(/\*/g, '') // 去除星号
      .trim()

    // 尝试转换为数字（去除非数字字符）
    const numericPart = idNum.replace(/\D/g, '')
    if (numericPart.length > 0) {
      try {
        const num = parseInt(numericPart, 10)
        if (!isNaN(num) && num > 0) {
          idNum = String(num)
        }
      } catch (e) {
        // 转换失败，保持原值
      }
    }

    return {
      ...row,
      证件号码: idNum,
    }
  })

  // 过滤掉证件号码为空的行
  filtered = filtered.filter(row => {
    const idNum = String(row['证件号码'] || '').trim()
    return idNum !== '' && idNum !== 'undefined' && idNum !== 'null' && idNum !== 'NaN'
  })

  // 6. 前向填充：分类、商铺地址、从业单位
  // 如果当前行的这些字段为空，使用上一行的值
  let lastCategory = ''
  let lastShopAddress = ''
  let lastEmployer = ''

  filtered = filtered.map(row => {
    const category = String(row['分类'] || '').trim()
    const shopAddress = String(row['商铺地址'] || '').trim()
    const employer = String(row['从业单位'] || '').trim()

    // 如果当前行有值，更新last值
    if (category) lastCategory = category
    if (shopAddress) lastShopAddress = shopAddress
    if (employer) lastEmployer = employer

    // 如果当前行为空，使用last值
    return {
      ...row,
      分类: category || lastCategory,
      商铺地址: shopAddress || lastShopAddress,
      从业单位: employer || lastEmployer,
    }
  })

  // 7. 最后清理证件号码（使用cleanIdNumber统一格式）
  filtered = filtered.map(row => ({
    ...row,
    证件号码: cleanIdNumber(row['证件号码']),
  }))

  console.log(`从业数据处理完成：${filtered.length} 条记录`)
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
  try {
    const fileName = path.basename(filePath)
    let decodedFileName = fileName
    
    // 尝试 GBK 解码
    try {
      const gbkDecoded = iconv.decode(Buffer.from(fileName, 'binary'), 'gbk')
      // 检查是否包含乱码字符
      const hasInvalidChars = /[^\u4e00-\u9fa5\w\s\-_.()\[\]（）【】]/g.test(gbkDecoded)
      
      if (!hasInvalidChars && gbkDecoded !== fileName) {
        decodedFileName = gbkDecoded
      }
    } catch (e) {
      // 如果解码失败，使用原始文件名（可能是 UTF-8）
      decodedFileName = fileName
    }
    
    console.log(`读取文件: ${decodedFileName} - ${cleaned.length} 条记录`)
  } catch (e) {
    // 如果转换失败，使用原始文件名
    console.log(`读取文件: ${path.basename(filePath)} - ${cleaned.length} 条记录`)
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