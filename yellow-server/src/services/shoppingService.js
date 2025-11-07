/**
 * 购物分析服务
 * 处理购物数据，包括敏感商品分类、收货地址分类等
 */

const dayjs = require('dayjs')
const { askDeepSeek } = require('../utils/llmClient')
const LLMCacheManager = require('../utils/llmCache')

// 初始化缓存管理器
const sensitiveCache = new LLMCacheManager('llm_sensitive_cache.json')
const classifyCache = new LLMCacheManager('llm_classify_cache.json')

const ADDRESS_CATEGORIES = new Set(['小区', '商务楼', '场所', '宾馆', '其他'])

/**
 * 安全获取值
 */
function safeGet(val) {
  if (val == null || val === '') return ' '
  return String(val)
}

/**
 * 提取商品数量（用于敏感商品判断）
 * @param {string} desc - 商品描述
 * @returns {number} 总数量
 */
function extractQuantity(desc) {
  if (!desc) return 0

  // 匹配模式：数字+单位×数量，例如 "5只/盒×1" 或 "50只装×1"
  const patterns = [
    /(\d+)\s*只[^×]*×\s*(\d+)/g, // 5只/盒×1
    /(\d+)\s*只装[^×]*×\s*(\d+)/g, // 50只装×1
    /(\d+)\s*包[^×]*×\s*(\d+)/g, // 1包×1
  ]

  let total = 0
  for (const pattern of patterns) {
    let match
    while ((match = pattern.exec(desc)) !== null) {
      const unitCount = parseInt(match[1], 10) || 0
      const multiplier = parseInt(match[2], 10) || 0
      total += unitCount * multiplier
    }
  }

  return total
}

/**
 * 判断敏感商品等级（使用 LLM）
 * @param {string} desc - 商品描述
 * @returns {Promise<string>} 等级：'0'、'低'、'中'、'高'
 */
async function classifySensitiveLevel(desc) {
  if (!desc || desc.trim() === '') {
    return '0'
  }

  // 检查缓存
  if (sensitiveCache.has(desc)) {
    const level = sensitiveCache.get(desc)
    console.log(`[LLM Cache] 敏感商品 ${desc} -> ${level}`)
    return level
  }

  // 提取数量
  const quantity = extractQuantity(desc)

  const prompt = `
请判断商品名称"${desc}"属于以下哪一类风险等级：
0 - 不是敏感商品
低 - 轻度敏感：有一定关联到隐私或特殊行为，例如"验孕棒"、"HCG"、"滋补精华液"、"私密护理"（非食物、补药品）等；
中 - 中度敏感：可能是日常用品但也可用于特殊用途，例如"安全套"、"一次性毛巾"、催情、激情、快感、名流、情趣性用品（非食物）等等。
    注意每个单子购买敏感商品总数量小于10，
    例如："名流 水多多避孕套5只装 玻尿酸超薄免洗男用安全套 5只/盒(包装随机)×1;超大包悬挂式洗脸巾一次性棉柔巾美容院纯棉吸水不掉毛底部抽取式擦脸巾 一次性洗脸面巾 1包×1;超大包悬挂式洗脸巾一次性棉柔巾美容院纯棉吸水不掉毛底部抽取式擦脸巾 一次性洗脸面巾 1包×1;" -> 5只*1+1包*1 = 6
高 - 中度敏感：可能是日常用品但也可用于特殊用途，例如"安全套"、"一次性毛巾"、催情、激情、快感、名流、情趣性用品（非食物）等等。
    注意每个单子（一条记录）购买敏感商品总数量大于10，
    例如："名流 水多多避孕套50只装 玻尿酸超薄免洗男用安全套 50只/盒(包装随机)×1" -> 50只*1 = 50
    例如："[名流]之夜水多多玻尿酸超薄大油量免洗避孕套100只装/装×1" -> 100只*1 = 100
    例如：名流 MO滋养玻尿酸避孕套 10只/盒×2;名流 MO滋养玻尿酸避孕套 10只/盒×1;-> 10只*2+10只*1 = 30
请只返回数字 0、低、中 或 高，不要解释。
`

  try {
    let level = await askDeepSeek(prompt)
    level = level.trim()

    // 验证等级是否有效
    if (!['低', '中', '高'].includes(level)) {
      level = '0'
    }

    // 如果数量 >= 10，强制设为高
    if (quantity >= 10 && level !== '0') {
      level = '高'
    }

    // 保存到缓存
    sensitiveCache.set(desc, level)
    console.log(`[LLM判断] ${desc} → ${level} (数量: ${quantity})`)

    return level
  } catch (error) {
    console.error(`[敏感商品判断错误] ${desc}:`, error.message)
    return '0'
  }
}

/**
 * 分类地址（使用 LLM）
 * @param {string} address - 地址
 * @returns {Promise<string>} 分类：'小区'、'商务楼'、'场所'、'宾馆'、'其他'
 */
async function classifyAddress(address) {
  if (!address || address.trim() === '') {
    return ''
  }

const addressStr = String(address).trim()

  // 检查缓存
  if (classifyCache.has(addressStr)) {
    const result = classifyCache.get(addressStr)
    console.log(`[LLM Cache] 地址分类 ${addressStr} -> ${result}`)
    return result
  }

  // 修复：使用与 antiporn 一致的 prompt
  const prompt = `请将以下地址分类为"小区""商务楼""场所""宾馆""其他"中的一个，
注意：
    - 场所包含的地址可能有酒吧、影院、足疗店、足浴、美容、KTV、按摩、养生、棋牌室、SPA、浴场等娱乐休闲场所：
    - 宾馆包含的地址只要有酒店、旅馆、宾馆等
    - 商务楼包含的地址可能有大夏、商务楼等
    - 小区包含的地址可能有公寓、小区等
    - 不属于以上地址分类定义就判为其他
最终只返回5种分类结果之一:"小区","商务楼","场所","宾馆","其他",不需要括号或者引号
地址：${addressStr}
`

  try {
    let result = await askDeepSeek(prompt)
    result = result.trim()

    // 修复：验证结果是否在有效分类中（与 antiporn 一致）
    const validCategories = ['小区', '商务楼', '场所', '宾馆', '其他']
    if (!validCategories.includes(result)) {
      result = '其他'
    }

    console.log(`[LLM分类] 地址：${addressStr}`)
    console.log(`[LLM分类] 结果：${result}`)

    // 保存到缓存
    classifyCache.set(addressStr, result)

    return result
  } catch (error) {
    console.error(`[地址分类错误] ${addressStr}:`, error.message)
    // 出错时返回 '其他'
    classifyCache.set(addressStr, '其他')
    return '其他'
  }
}

/**
 * 为商品添加颜色标记（HTML格式）
 */
function colorProduct(text, level) {
  if (level === '高') {
    return `<span style='color:red'>${text}</span>`
  }
  if (level === '中') {
    return `<span style='color:darkorange'>${text}</span>`
  }
  if (level === '低') {
    return `<span style='color:darkgreen'>${text}</span>`
  }
  return String(text)
}

/**
 * 构建敏感商品详细信息
 */
function buildSensitiveDetail(shoppingData) {
  return shoppingData.map(row => {
    const checkInTime = safeGet(row['下单时间'])
    const productName = safeGet(row['商品名称'])
    const address = safeGet(row['收货地址'])
    const level = row['异常购物'] || '0'

    const coloredProduct = colorProduct(productName, level)
    const detail = `- ${checkInTime} - 商品: ${coloredProduct} - 收货地址：${address}`

    // 解析时间用于排序
    let timeValue = null
    try {
      timeValue = dayjs(checkInTime).valueOf()
      if (isNaN(timeValue)) timeValue = null
    } catch (e) {
      // 忽略解析错误
    }

    return {
      ...row,
      商品名称详细: detail,
      时间值: timeValue,
    }
  })
}

/**
 * 构建收货地址详细信息
 */
function buildShoppingDetail(shoppingData) {
  return shoppingData.map(row => {
    const checkInTime = safeGet(row['下单时间'])
    const address = safeGet(row['收货地址'])
    const addressClass = safeGet(row['收货地址分类'])
    const productName = safeGet(row['商品名称'])

    const detail = `- ${checkInTime} - 收货地址：${address} (${addressClass}) - 商品: ${productName}`

    // 解析时间用于排序
    let timeValue = null
    try {
      timeValue = dayjs(checkInTime).valueOf()
      if (isNaN(timeValue)) timeValue = null
    } catch (e) {
      // 忽略解析错误
    }

    return {
      ...row,
      收货地址详细: detail,
      时间值: timeValue,
    }
  })
}

/**
 * 处理购物数据（完整流程）
 * @param {Array<Object>} shoppingData - 原始购物数据
 * @returns {Promise<Object>} 处理结果
 */
async function processShoppingData(shoppingData) {
  if (!shoppingData || shoppingData.length === 0) {
    return {
      allShoppingData: [],
      sensitiveShoppingData: [],
      groupedSensitiveData: [],
      groupedShoppingData: [],
    }
  }

  console.log(`开始处理购物数据: ${shoppingData.length} 条记录`)

  // 1. 敏感商品分类（逐条调用 LLM）
  const processedShopping = []
  for (let i = 0; i < shoppingData.length; i++) {
    const row = shoppingData[i]
    const productName = String(row['商品名称'] || '').trim()

    if (productName) {
      const level = await classifySensitiveLevel(productName)
      processedShopping.push({
        ...row,
        异常购物: level,
      })
    } else {
      processedShopping.push({
        ...row,
        异常购物: '0',
      })
    }

    // 每处理10条输出一次进度
    if ((i + 1) % 10 === 0) {
      console.log(`敏感商品分类进度: ${i + 1}/${shoppingData.length}`)
    }
  }

  // 2. 筛选敏感商品
  const sensitiveShopping = processedShopping.filter(row => row['异常购物'] !== '0')

  // 3. 构建敏感商品详细信息
  const sensitiveWithDetail = buildSensitiveDetail(sensitiveShopping)

  // 4. 按时间倒序排序
  sensitiveWithDetail.sort((a, b) => (b.时间值 || 0) - (a.时间值 || 0))

  // 5. 按证件号码聚合敏感商品（取最高等级）
  // 注意：与 antiporn 保持一致，reverseMap 中 0 对应 '0'（字符串）
  const levelMap = { 高: 3, 中: 2, 低: 1, 0: 0, '0': 0, '': 0, None: 0 }
  const reverseMap = { 3: '高', 2: '中', 1: '低', 0: '0' }

  const groupedSensitive = new Map()
  for (const row of sensitiveWithDetail) {
    const id = String(row['证件号码'] || '').trim()
    if (!id) continue

    const currentMaxLevel = groupedSensitive.has(id) ? groupedSensitive.get(id).异常购物_等级_数值 : 0
    const newLevel = levelMap[row['异常购物']] || 0

    if (newLevel > currentMaxLevel) {
      groupedSensitive.set(id, {
        证件号码: id,
        异常购物_等级_数值: newLevel,
        商品名称详细: [row['商品名称详细']],
      })
    } else if (newLevel === currentMaxLevel) {
      if (groupedSensitive.has(id)) {
        groupedSensitive.get(id).商品名称详细.push(row['商品名称详细'])
      }
    }
  }

  const finalGroupedSensitive = Array.from(groupedSensitive.values()).map(item => ({
    证件号码: item.证件号码,
    异常购物: reverseMap[item.异常购物_等级_数值],
    异常购物_等级: item.异常购物_等级_数值 > 0 ? '是' : '否',
    商品名称详细: Array.from(new Set(item.商品名称详细)).join('\n'), // 去重并合并
  }))

  // 6. 收货地址分类（逐条调用 LLM）
  console.log(`开始地址分类，共 ${processedShopping.length} 条记录`)
  const shoppingWithAddressClass = []
  for (let i = 0; i < processedShopping.length; i++) {
    const row = processedShopping[i]
    const address = String(row['收货地址'] || '').trim()
    
    try {
      const addressClass = await classifyAddress(address)
      shoppingWithAddressClass.push({
        ...row,
        收货地址分类: addressClass,
      })

      // 每处理10条输出一次进度
      if ((i + 1) % 10 === 0) {
        console.log(`地址分类进度: ${i + 1}/${processedShopping.length}`)
      }
    } catch (error) {
      console.error(`[地址分类错误] 第 ${i + 1} 条记录:`, error.message)
      shoppingWithAddressClass.push({
        ...row,
        收货地址分类: '其他',
      })
    }
  }
  console.log(`地址分类完成，共处理 ${shoppingWithAddressClass.length} 条记录`)

  // 7. 构建收货地址详细信息
  const shoppingWithAddressDetail = buildShoppingDetail(shoppingWithAddressClass)

  // 8. 按时间倒序排序
  shoppingWithAddressDetail.sort((a, b) => (b.时间值 || 0) - (a.时间值 || 0))

  // 9. 按证件号码聚合收货地址
  const groupedShopping = new Map()
  for (const row of shoppingWithAddressDetail) {
    const id = String(row['证件号码'] || '').trim()
    if (!id) continue

    if (!groupedShopping.has(id)) {
      groupedShopping.set(id, {
        证件号码: id,
        收货地址详细: new Set(),
        收货地址分类: new Set(),
      })
    }
    groupedShopping.get(id).收货地址详细.add(row['收货地址详细'])
    if (row['收货地址分类'] && row['收货地址分类'].trim() !== '') {
      groupedShopping.get(id).收货地址分类.add(row['收货地址分类'])
    }
  }

  

  const finalGroupedShopping = Array.from(groupedShopping.values()).map(item => ({
    证件号码: item.证件号码,
    收货地址详细: Array.from(item.收货地址详细).join('\n'),
    收货地址分类: Array.from(item.收货地址分类).join('\n'),
  }))

  console.log('购物数据处理完成')
  return {
    allShoppingData: processedShopping,
    sensitiveShoppingData: sensitiveShopping,
    groupedSensitiveData: finalGroupedSensitive,
    groupedShoppingData: finalGroupedShopping,
  }
}

module.exports = {
  classifySensitiveLevel,
  classifyAddress,
  processShoppingData
}