/**
 * 酒店入住分析服务
 * 处理酒店入住数据，统计入住次数、同住男人数等信息
 */

const fs = require('fs')
const path = require('path')
const dayjs = require('dayjs')
const iconv = require('iconv-lite')
const { readAndCleanData, cleanIdNumber } = require('./dataMergeService')

const DEFAULT_FEMALE_PATTERNS = ['女性入住', '女性', '女入住', '入住女性', '女']
const DEFAULT_MALE_PATTERNS = ['入住男性', '男性入住', '男性', '男入住', '男']
const FEMALE_PATTERN_CONFIG = process.env.HOTEL_FEMALE_FILE_PATTERN || process.env.HOTEL_FEMALE_FILE_KEYWORD || ''
const MALE_PATTERN_CONFIG = process.env.HOTEL_MALE_FILE_PATTERN || process.env.HOTEL_MALE_FILE_KEYWORD || ''

function parsePatternConfig(rawValue, fallbacks = []) {
  const patterns = []
  const seen = new Set()

  const addKeyword = keyword => {
    const normalized = keyword.toLowerCase()
    if (!seen.has(`keyword:${normalized}`)) {
      patterns.push({ type: 'keyword', value: normalized })
      seen.add(`keyword:${normalized}`)
    }
  }

  const addRegex = pattern => {
    const key = `regex:${pattern}`
    if (!seen.has(key)) {
      patterns.push({ type: 'regex', value: new RegExp(pattern, 'i') })
      seen.add(key)
    }
  }

  const handleEntry = entry => {
    if (!entry) return
    const trimmed = entry.trim()
    if (!trimmed) return
    if (trimmed.startsWith('re:')) {
      const regexBody = trimmed.slice(3).trim()
      if (regexBody) {
        addRegex(regexBody)
      }
    } else if (trimmed.startsWith('/') && trimmed.endsWith('/') && trimmed.length > 2) {
      const regexBody = trimmed.slice(1, -1)
      if (regexBody) {
        addRegex(regexBody)
      }
    } else {
      addKeyword(trimmed)
    }
  }

  if (rawValue && rawValue.trim()) {
    rawValue.split(',').forEach(handleEntry)
  }

  fallbacks.forEach(handleEntry)

  return patterns
}

const FEMALE_PATTERNS = parsePatternConfig(FEMALE_PATTERN_CONFIG, DEFAULT_FEMALE_PATTERNS)
const MALE_PATTERNS = parsePatternConfig(MALE_PATTERN_CONFIG, DEFAULT_MALE_PATTERNS)

function matchPatterns(filename, patterns) {
  if (!filename) return false
  const lower = filename.toLowerCase()
  return patterns.some(pattern => {
    if (pattern.type === 'regex') {
      return pattern.value.test(filename)
    }
    return lower.includes(pattern.value)
  })
}

/**
 * 安全获取值（处理 null/undefined）
 */
function safeGet(val) {
  if (val == null || val === '') return ' '
  return String(val)
}

/**
 * 提取日期部分的辅助函数（只取日期，不包含时间）
 * @param {string} timeStr - 时间字符串
 * @returns {string} 日期字符串（YYYY-MM-DD格式）
 */
function extractDate(timeStr) {
  if (!timeStr) return ''
  try {
    // 尝试解析时间字符串，提取日期部分（YYYY-MM-DD）
    const parsed = dayjs(timeStr)
    if (parsed.isValid()) {
      return parsed.format('YYYY-MM-DD')
    }
    // 如果解析失败，尝试直接提取日期部分（如果格式是 YYYY-MM-DD HH:mm:ss）
    const dateMatch = String(timeStr).match(/^(\d{4}-\d{2}-\d{2})/)
    if (dateMatch) {
      return dateMatch[1]
    }
  } catch (e) {
    // 解析失败，返回空字符串
  }
  return ''
}

/**
 * 处理酒店入住数据
 * @param {string} hotelFolderPath - 酒店文件夹路径
 * @returns {Array<Object>} 处理后的酒店数据
 */
function processingHotel(hotelFolderPath) {
  if (!hotelFolderPath || !fs.existsSync(hotelFolderPath)) {
    console.warn('酒店文件夹不存在:', hotelFolderPath)
    return []
  }

  // 1. 根据关键词查找文件（参考 antiporn 的简洁实现）
  let ruzhuFile = null
  let tongzhuFile = null

  const files = fs.readdirSync(hotelFolderPath)
  
  for (const file of files) {
    // 跳过非文件项
    const filePath = path.join(hotelFolderPath, file)
    if (!fs.statSync(filePath).isFile()) continue
    
    // 只处理 Excel 文件
    if (!file.endsWith('.xlsx') && !file.endsWith('.xls')) continue
    
    // 过滤掉 Excel 临时文件（以 ~$ 开头的文件）
    if (file.startsWith('~$')) continue
    
    // 简单的关键词匹配（与 antiporn 一致）
    if (!ruzhuFile && matchPatterns(file, FEMALE_PATTERNS)) {
      ruzhuFile = filePath
    }
    if (!tongzhuFile && matchPatterns(file, MALE_PATTERNS)) {
      tongzhuFile = filePath
    }
    
    // 如果两个文件都已找到，提前退出
    if (ruzhuFile && tongzhuFile) {
      break
    }
  }

  if (!ruzhuFile || !tongzhuFile) {
    console.error('❌ 未找到酒店文件:')
    console.error(`  需要包含关键词 "${HOTEL_FEMALE_FILE_KEYWORD}" 的文件`)
    console.error(`  需要包含关键词 "${HOTEL_MALE_FILE_KEYWORD}" 的文件`)
    throw new Error('未找到入住记录和同住人文件')
  }

  console.log(`✅ 找到入住文件: ${path.basename(ruzhuFile)}`)
  console.log(`✅ 找到同住文件: ${path.basename(tongzhuFile)}`)


  // 2. 读取文件
  const dfStay = readAndCleanData(ruzhuFile)
  const dfCohabit = readAndCleanData(tongzhuFile)

  if (dfStay.length === 0 || dfCohabit.length === 0) {
    console.warn('酒店数据为空')
    return []
  }

  // 3. 统计入住次数（按证件号码）
  const stayCounts = new Map()
  for (const row of dfStay) {
    const id = cleanIdNumber(row['证件号码'] || '')
    if (id) {
      stayCounts.set(id, (stayCounts.get(id) || 0) + 1)
    }
  }

  // 4. 统计同住男人数（男性入住表中证件号码是男性，需要通过匹配键关联到女性）
  const cohabitCounts = new Map()
  const cohabitGroups = new Map()
  
  // 构建女性入住记录的索引（按日期、酒店名称、房间号匹配）
  const femaleStayIndex = new Map()
  for (const row of dfStay) {
    const checkInDate = extractDate(row['入住时间'])
    const hotelName = String(row['旅馆业店招名称'] || '').trim()
    const roomNumber = String(row['房间号'] || '').trim()
    const key = `${checkInDate}|${hotelName}|${roomNumber}`
    
    if (key !== '||') {
      const femaleId = cleanIdNumber(row['证件号码'] || '')
      if (femaleId) {
        // 一个键可能对应多个女性（同一日期同一房间），使用数组
        if (!femaleStayIndex.has(key)) {
          femaleStayIndex.set(key, [])
        }
        femaleStayIndex.get(key).push(femaleId)
      }
    }
  }
  
  // 遍历男性入住记录，通过匹配键找到对应的女性
  for (const row of dfCohabit) {
    // 尝试多个可能的列名：证件号码、身份证
    const maleId = cleanIdNumber(row['证件号码'] || row['身份证'] || '')
    
    if (!maleId) continue
    
    const checkInDate = extractDate(row['入住时间'])
    const hotelName = String(row['旅馆业店招名称'] || '').trim()
    const roomNumber = String(row['房间号'] || '').trim()
    const key = `${checkInDate}|${hotelName}|${roomNumber}`
    
    if (key !== '||' && femaleStayIndex.has(key)) {
      // 匹配到对应的女性，为每个女性添加这个男性
      const femaleIds = femaleStayIndex.get(key)
      for (const femaleId of femaleIds) {
        if (!cohabitGroups.has(femaleId)) {
          cohabitGroups.set(femaleId, new Set())
        }
        cohabitGroups.get(femaleId).add(maleId)
      }
    }
  }

  for (const [id, maleIds] of cohabitGroups.entries()) {
    cohabitCounts.set(id, maleIds.size)
  }

  // 5. 生成入住详细信息（按时间倒序）
  const stayDetails = dfStay.map(row => {
    const checkInTime = safeGet(row['入住时间'])
    const checkOutTime = safeGet(row['离店时间'])
    const name = safeGet(row['姓名'])
    const hotelName = safeGet(row['旅馆业店招名称'])
    const roomNumber = safeGet(row['房间号'])
    const address = safeGet(row['旅馆地址'])

    const detail = `- ${checkInTime}-${checkOutTime} ${name} 入住 ${hotelName}${roomNumber}房间-地址：${address}`

    // 解析时间用于排序
    let timeValue = null
    try {
      timeValue = dayjs(checkInTime).valueOf()
      if (isNaN(timeValue)) timeValue = null
    } catch (e) {
      // 忽略解析错误
    }

    return {
      证件号码: cleanIdNumber(row['证件号码'] || ''),
      入住详细: detail,
      时间值: timeValue,
    }
  })

  // 按时间倒序排序
  stayDetails.sort((a, b) => {
    if (a.时间值 == null && b.时间值 == null) return 0
    if (a.时间值 == null) return 1
    if (b.时间值 == null) return -1
    return b.时间值 - a.时间值 // 倒序
  })

  // 按证件号码分组聚合
  const stayDetailGrouped = new Map()
  for (const item of stayDetails) {
    const id = item.证件号码
    if (!stayDetailGrouped.has(id)) {
      stayDetailGrouped.set(id, [])
    }
    stayDetailGrouped.get(id).push(item.入住详细)
  }

  // 6. 生成同住详细信息（按时间倒序，通过匹配键关联女性）
  const cohabitDetails = []
  
  // 构建女性入住记录的索引（包含姓名，用于生成详细信息）
  const femaleStayIndexWithName = new Map()
  for (const row of dfStay) {
    const checkInDate = extractDate(row['入住时间'])
    const hotelName = String(row['旅馆业店招名称'] || '').trim()
    const roomNumber = String(row['房间号'] || '').trim()
    const key = `${checkInDate}|${hotelName}|${roomNumber}`
    
    if (key !== '||') {
      const femaleId = cleanIdNumber(row['证件号码'] || '')
      const femaleName = safeGet(row['姓名'])
      if (femaleId) {
        if (!femaleStayIndexWithName.has(key)) {
          femaleStayIndexWithName.set(key, [])
        }
        femaleStayIndexWithName.get(key).push({ id: femaleId, name: femaleName })
      }
    }
  }
  
  // 遍历男性入住记录，生成同住详细信息
  for (const row of dfCohabit) {
    // 尝试多个可能的列名：证件号码、身份证
    const maleId = cleanIdNumber(row['证件号码'] || row['身份证'] || '')
    if (!maleId) continue
    
    const checkInTime = safeGet(row['入住时间'])
    const checkOutTime = safeGet(row['离店时间'])
    const maleName = safeGet(row['姓名'])
    const hotelName = safeGet(row['旅馆业店招名称'])
    const roomNumber = safeGet(row['房间号'])
    const address = safeGet(row['旅馆地址'])
    
    const checkInDate = extractDate(row['入住时间'])
    const matchKey = `${checkInDate}|${String(row['旅馆业店招名称'] || '').trim()}|${String(row['房间号'] || '').trim()}`
    
    if (matchKey !== '||' && femaleStayIndexWithName.has(matchKey)) {
      // 匹配到对应的女性，为每个女性生成一条同住详细信息
      const femaleList = femaleStayIndexWithName.get(matchKey)
      for (const female of femaleList) {
        const detail = `- ${checkInTime}-${checkOutTime}: ${female.name} 和 ${maleName}(男）证件号：${maleId}在${hotelName}${roomNumber}房间-地址：${address}`
        
        // 解析时间用于排序
        let timeValue = null
        try {
          timeValue = dayjs(checkInTime).valueOf()
          if (isNaN(timeValue)) timeValue = null
        } catch (e) {
          // 忽略解析错误
        }
        
        cohabitDetails.push({
          证件号码: female.id,
          同住详细: detail,
          时间值: timeValue,
        })
      }
    }
  }

  // 按时间倒序排序
  cohabitDetails.sort((a, b) => {
    if (a.时间值 == null && b.时间值 == null) return 0
    if (a.时间值 == null) return 1
    if (b.时间值 == null) return -1
    return b.时间值 - a.时间值 // 倒序
  })

  // 按证件号码分组聚合
  const cohabitDetailGrouped = new Map()
  for (const item of cohabitDetails) {
    const id = item.证件号码
    if (!cohabitDetailGrouped.has(id)) {
      cohabitDetailGrouped.set(id, [])
    }
    cohabitDetailGrouped.get(id).push(item.同住详细)
  }

  // 7. 合并结果（基于入住数据）
  const resultMap = new Map()

  // 先添加入住数据
  for (const row of dfStay) {
    const id = cleanIdNumber(row['证件号码'] || '')
    if (!id) continue

    if (!resultMap.has(id)) {
      resultMap.set(id, { ...row, 证件号码: id })
    }

    const item = resultMap.get(id)
    item.入住次数 = stayCounts.get(id) || 0
    item.同住男人数 = cohabitCounts.get(id) || 0
    item.入住信息 = stayDetailGrouped.get(id)?.join('\n') || ''
    item.同住信息 = cohabitDetailGrouped.get(id)?.join('\n') || ''
  }

  // 转换为数组
  const result = Array.from(resultMap.values())

  const totalStayCount = Array.from(stayCounts.values()).reduce((sum, value) => sum + value, 0)
  const totalCohabitCount = Array.from(cohabitCounts.values()).reduce((sum, value) => sum + value, 0)

  console.log(`酒店数据处理完成: ${result.length} 条记录`)
  console.log(`入住次数统计总计: ${totalStayCount}`)
  console.log(`同住男人数统计总计: ${totalCohabitCount}`)
  return result
}

/**
 * 合并酒店数据到主表
 * @param {Array<Object>} mainData - 主表数据
 * @param {Array<Object>} hotelData - 酒店数据（包含 入住次数、同住男人数、入住信息、同住信息）
 * @returns {Array<Object>} 合并后的数据
 */
function mergeHotelData(mainData, hotelData) {
  if (!mainData || mainData.length === 0) {
    return mainData
  }

  if (!hotelData || hotelData.length === 0) {
    // 修复：即使没有酒店数据，也要添加默认字段
    return mainData.map(row => ({
      ...row,
      入住次数: 0,
      同住男人数: 0,
      入住信息: '',
      同住信息: '',
    }))
  }

  // 创建酒店数据索引（按证件号码）
  const hotelMap = new Map()
  for (const row of hotelData) {
    const id = cleanIdNumber(row['证件号码'] || '')
    if (id) {
      hotelMap.set(id, row)
    }
  }

  // 合并到主表
  let matchedCount = 0
  const mergedResult = mainData.map(row => {
    const id = cleanIdNumber(row['证件号码'] || '')
    const hotelInfo = hotelMap.get(id)

    if (hotelInfo) {
      matchedCount++
      return {
        ...row,
        入住次数: parseInt(hotelInfo['入住次数'] || 0, 10),
        同住男人数: parseInt(hotelInfo['同住男人数'] || 0, 10),
        入住信息: String(hotelInfo['入住信息'] || ''),
        同住信息: String(hotelInfo['同住信息'] || ''),
      }
    } else {
      // 修复：没有酒店数据时，也要添加默认字段
      return {
        ...row,
        入住次数: 0,
        同住男人数: 0,
        入住信息: '',
        同住信息: '',
      }
    }
  })

  console.log(`酒店数据匹配: 主表 ${mainData.length} 条，酒店数据 ${hotelData.length} 条，匹配成功 ${matchedCount} 条`)

  return mergedResult
}

module.exports = {
  processingHotel,
  mergeHotelData
}