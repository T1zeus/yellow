/**
 * 酒店入住分析服务
 * 处理酒店入住数据，统计入住次数、同住男人数等信息
 */

const fs = require('fs')
const path = require('path')
const dayjs = require('dayjs')
const iconv = require('iconv-lite')
const { readAndCleanData } = require('./dataMergeService')

const HOTEL_FEMALE_FILE_KEYWORD = process.env.HOTEL_FEMALE_FILE_KEYWORD || '女性入住'
const HOTEL_MALE_FILE_KEYWORD = process.env.HOTEL_MALE_FILE_KEYWORD || '入住男性'

/**
 * 安全获取值（处理 null/undefined）
 */
function safeGet(val) {
  if (val == null || val === '') return ' '
  return String(val)
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

  // 1. 根据关键词查找文件
  let ruzhuFile = null
  let tongzhuFile = null

  const files = fs.readdirSync(hotelFolderPath)
 

  for (const file of files) {
    const filePath = path.join(hotelFolderPath, file)
    if (!fs.statSync(filePath).isFile()) continue
    if (!file.endsWith('.xlsx') && !file.endsWith('.xls')) continue

    // 修复：尝试多种编码方式解码文件名
    let decodedFileName = file
    try {
      // 方法1：尝试 GBK 解码（适用于老版本 Windows）
      const gbkDecoded = iconv.decode(Buffer.from(file, 'binary'), 'gbk')
      // 检查是否包含乱码字符（非正常中文字符范围）
      const hasInvalidChars = /[^\u4e00-\u9fa5\w\s\-_.()\[\]（）【】]/g.test(gbkDecoded)
      
      if (!hasInvalidChars && gbkDecoded !== file) {
        decodedFileName = gbkDecoded
      } else {
        // 方法2：如果已经是 UTF-8，直接使用
        decodedFileName = file
      }
    } catch (e) {
      // 如果解码失败，使用原始文件名
      decodedFileName = file
    }


    // 使用解码后的文件名进行匹配
    if (decodedFileName.includes(HOTEL_FEMALE_FILE_KEYWORD)) {
      ruzhuFile = filePath
      console.log(`  ✅ 找到入住文件: ${decodedFileName}`)
    }
    if (decodedFileName.includes(HOTEL_MALE_FILE_KEYWORD)) {
      tongzhuFile = filePath
      console.log(`  ✅ 找到同住文件: ${decodedFileName}`)
    }
  }

  if (!ruzhuFile || !tongzhuFile) {
    console.error('❌ 未找到酒店文件:')
    console.error(`  需要包含关键词 "${HOTEL_FEMALE_FILE_KEYWORD}" 的文件`)
    console.error(`  需要包含关键词 "${HOTEL_MALE_FILE_KEYWORD}" 的文件`)
    throw new Error('未找到入住记录和同住人文件')
  }

  // 修复：正确显示文件名
  try {
    let ruzhuName = path.basename(ruzhuFile)
    let tongzhuName = path.basename(tongzhuFile)
    
    // 尝试解码
    try {
      ruzhuName = iconv.decode(Buffer.from(ruzhuName, 'binary'), 'gbk')
    } catch (e) {}
    
    try {
      tongzhuName = iconv.decode(Buffer.from(tongzhuName, 'binary'), 'gbk')
    } catch (e) {}
    
    console.log(`找到入住文件: ${ruzhuName}`)
    console.log(`找到同住文件: ${tongzhuName}`)
  } catch (e) {
    console.log(`找到入住文件: ${path.basename(ruzhuFile)}`)
    console.log(`找到同住文件: ${path.basename(tongzhuFile)}`)
  }


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
    const id = String(row['证件号码'] || '').trim()
    if (id) {
      stayCounts.set(id, (stayCounts.get(id) || 0) + 1)
    }
  }

  // 4. 统计同住男人数（按证件号码，统计不同的证件号码男）
  const cohabitCounts = new Map()
  const cohabitGroups = new Map()

  for (const row of dfCohabit) {
    const id = String(row['证件号码'] || '').trim()
    const maleId = String(row['证件号码男'] || '').trim()

    if (id && maleId) {
      if (!cohabitGroups.has(id)) {
        cohabitGroups.set(id, new Set())
      }
      cohabitGroups.get(id).add(maleId)
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
      证件号码: String(row['证件号码'] || '').trim(),
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

  // 6. 生成同住详细信息（按时间倒序）
  const cohabitDetails = dfCohabit.map(row => {
    const checkInTime = safeGet(row['入住时间'])
    const checkOutTime = safeGet(row['离店时间'])
    const name = safeGet(row['姓名'])
    const maleName = safeGet(row['姓名男'])
    const maleId = safeGet(row['证件号码男'])
    const hotelName = safeGet(row['旅馆业店招名称'])
    const roomNumber = safeGet(row['房间号'])
    const address = safeGet(row['旅馆地址'])

    const detail = `- ${checkInTime}-${checkOutTime}: ${name} 和 ${maleName}(男）证件号：${maleId}在${hotelName}${roomNumber}房间-地址：${address}`

    // 解析时间用于排序
    let timeValue = null
    try {
      timeValue = dayjs(checkInTime).valueOf()
      if (isNaN(timeValue)) timeValue = null
    } catch (e) {
      // 忽略解析错误
    }

    return {
      证件号码: String(row['证件号码'] || '').trim(),
      同住详细: detail,
      时间值: timeValue,
    }
  })

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
    const id = String(row['证件号码'] || '').trim()
    if (!id) continue

    if (!resultMap.has(id)) {
      resultMap.set(id, { ...row })
    }

    const item = resultMap.get(id)
    item.入住次数 = stayCounts.get(id) || 0
    item.同住男人数 = cohabitCounts.get(id) || 0
    item.入住信息 = stayDetailGrouped.get(id)?.join('\n') || ''
    item.同住信息 = cohabitDetailGrouped.get(id)?.join('\n') || ''
  }

  // 转换为数组
  const result = Array.from(resultMap.values())

  console.log(`酒店数据处理完成: ${result.length} 条记录`)
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
    const id = String(row['证件号码'] || '').trim()
    if (id) {
      hotelMap.set(id, row)
    }
  }

  // 合并到主表
  return mainData.map(row => {
    const id = String(row['证件号码'] || '').trim()
    const hotelInfo = hotelMap.get(id)

    if (hotelInfo) {
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
}

module.exports = {
  processingHotel,
  mergeHotelData
}