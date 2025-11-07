/**
 * 地址提取与分类服务
 * 从简要案情中提取案发地点，并进行分类
 */

const { askDeepSeek } = require('../utils/llmClient')
const LLMCacheManager = require('../utils/llmCache')
const { classifyAddress } = require('./shoppingService')

// 初始化缓存管理器（使用 llm_cache.json）
const addressCache = new LLMCacheManager('llm_cache.json')

const KEYWORDS = [
  '小区',
  '公寓',
  '商务中心',
  '大厦',
  '广场',
  '市场',
  '酒店',
  '宾馆',
  '会所',
  '足浴',
  '足疗',
  'SPA',
  '养生馆',
  '养生店',
  '养生会所',
  'KTV',
  '酒吧',
  '浴场',
  '娱乐城',
  '写字楼',
  '办公楼',
  '工厂',
  '厂区'
]

function trimAddressToLandmark(address) {
  if (!address) return ''
  let trimmed = String(address).trim()
  if (!trimmed) return ''

  // 去掉前后标点
  trimmed = trimmed.replace(/^[，,。；;\s]+/, '').replace(/[，,。；;\s]+$/, '')

  const cutPositions = []
  for (const keyword of KEYWORDS) {
    const idx = trimmed.indexOf(keyword)
    if (idx !== -1) {
      cutPositions.push(idx + keyword.length)
    }
  }

  const roadNumberMatch = trimmed.match(/(.*?(路|街|道|巷|弄)\d+号)/)
  if (roadNumberMatch && roadNumberMatch[1]) {
    cutPositions.push(roadNumberMatch[1].length)
  }

  if (cutPositions.length === 0) {
    return trimmed
  }

  const cutPos = Math.max(...cutPositions)
  let result = trimmed.slice(0, cutPos).trim()

  // 去掉“等人”“等犯罪嫌疑人”等尾巴
  result = result.replace(/等[\u4e00-\u9fa5A-Za-z0-9]*$/, '')
  result = result.replace(/[，,。；;\s]+$/, '')

  return result.trim()
}

function extractAddressByRegex(text) {
  if (!text) return ''
  const source = String(text)

  const patterns = [
    /(上海市[^，。；\s]*?(?:小区|公寓|商务中心|大厦|广场|市场|酒店|宾馆|会所|足浴|足疗|SPA|养生馆|KTV|酒吧|浴场|娱乐城))/,
    /(松江区[^，。；\s]*?(?:小区|公寓|商务中心|大厦|广场|市场|酒店|宾馆|会所|足浴|足疗|SPA|养生馆|KTV|酒吧|浴场|娱乐城))/,
    /([\u4e00-\u9fa5A-Za-z0-9\u3001\-]+?(?:小区|公寓|商务中心|大厦|广场|市场|酒店|宾馆|会所|足浴|足疗|SPA|养生馆|KTV|酒吧|浴场|娱乐城))/,
    /(上海市[^，。；\s]*(?:路|街|道|巷|弄)\d+号[^，。；\s]*?(?:小区|公寓|商务中心|大厦|广场|市场|酒店|宾馆|会所|足浴|足疗|SPA|养生馆|KTV|酒吧|浴场|娱乐城)?)/,
    /([\u4e00-\u9fa5A-Za-z0-9\-]+?(?:路|街|道|巷|弄)\d+号[^，。；\s]*?(?:小区|公寓|商务中心|大厦|广场|市场|酒店|宾馆|会所|足浴|足疗|SPA|养生馆|KTV|酒吧|浴场|娱乐城)?)/
  ]

  for (const pattern of patterns) {
    const match = source.match(pattern)
    if (match && match[0]) {
      const trimmed = trimAddressToLandmark(match[0])
      if (trimmed && trimmed.length >= 4) {
        return trimmed
      }
    }
  }

  return ''
}

function normalizeAddress(rawAddress, originalText) {
  let result = trimAddressToLandmark(rawAddress)
  if (result && result.length >= 4 && !result.includes('无地址')) {
    return result
  }

  const regexResult = extractAddressByRegex(originalText)
  if (regexResult && regexResult.length >= 4) {
    return regexResult
  }

  if (rawAddress && !rawAddress.includes('无地址') && rawAddress.trim().length >= 4) {
    return rawAddress.trim()
  }

  return ''
}

/**
 * 从简要案情中提取地址（使用 LLM）
 * @param {string} text - 简要案情文本
 * @returns {Promise<string>} 提取的地址，如果无法提取则返回空字符串
 */
async function extractAddressByLLM(text) {
  if (!text || String(text).trim() === '') {
    return ''
  }

  const textStr = String(text).trim()

  // 检查缓存
  if (addressCache.has(textStr)) {
    const cached = addressCache.get(textStr)
    const normalizedCached = normalizeAddress(cached, textStr)
    if (normalizedCached) {
      console.log(`[LLM Cache] 地址提取 ${textStr.substring(0, 50)}... -> ${normalizedCached}`)
      return normalizedCached
    }
    console.log(`[LLM Cache] 地址提取 ${textStr.substring(0, 50)}... -> (缓存结果无效，尝试重新提取)`)
  }

  const prompt = `
你是上海公安局的案件信息提取系统。
请严格从原文中提取内容返回地址本身，不要返回标点、时间、人名、行为描述等。

示例1：
text：2025年5月13日，经群众匿名举报，在上海市松江区文城路358弄-6号嘉禾商务中心11楼1103室内，有卖淫嫖娼活动。已接报
answer:文城路358弄-6号嘉禾商务中心11楼1103室内

示例2：

text：2025年4月28日10时许，我所根据特情线索获悉：上海市松江区佘山镇佘北公路2455号飞牛生活广场圣悦养生SPA足道有为客人提供色情服务的情况，此案进一步调查处理。
answer:佘山镇佘北公路2455号飞牛生活广场圣悦养生SPA足道

如果未提取到、无法提取到有效地址，则返回：无地址

现在请处理以下说明：
${textStr}
`

  try {
    let result = await askDeepSeek(prompt)
    result = result.trim()

    console.log(`[LLM提取] 原文：${textStr.substring(0, 50)}...`)
    console.log(`[LLM提取] 结果：${result}`)

    const normalizedResult = normalizeAddress(result, textStr)

    // 保存到缓存（存储最终结果，避免再次处理）
    addressCache.set(textStr, normalizedResult || result)

    if (!normalizedResult) {
      return ''
    }

    return normalizedResult
  } catch (error) {
    console.error(`[地址提取错误] ${textStr.substring(0, 50)}...:`, error.message)
  }

  const fallback = extractAddressByRegex(textStr)
  if (fallback) {
    console.log(`[LLM提取] 结果为空，正则回退获得地址：${fallback}`)
    addressCache.set(textStr, fallback)
    return fallback
  }

  return ''
}

/**
 * 批量提取地址（带进度回调）
 * @param {Array<Object>} data - 数据数组，需要包含 '简要案情' 字段
 * @param {Function} progressCallback - 进度回调函数 (current, total) => void
 * @returns {Promise<Array<Object>>} 添加了 '案发地点' 和 '地点分类' 字段的数据
 */
async function extractAddressesFromData(data, progressCallback) {
  if (!data || data.length === 0) {
    return data.map(row => ({
      ...row,
      案发地点: '',
      地点分类: '',
    }))
  }

  console.log(`开始提取案发地点: ${data.length} 条记录`)

  const results = []
  const total = data.length

  for (let i = 0; i < data.length; i++) {
    const row = data[i]
    const text = String(row['简要案情'] || '').trim()

    // 提取地址
    let address = ''
    if (text) {
      address = await extractAddressByLLM(text)
    }

    // 分类地址（复用 shoppingService 的 classifyAddress）
    let addressCategory = ''
    if (address) {
      addressCategory = await classifyAddress(address)
    }

    results.push({
      ...row,
      案发地点: address,
      地点分类: addressCategory,
    })

    // 更新进度（每处理一条记录）
    if (progressCallback) {
      progressCallback(i + 1, total)
    }

    // 每处理10条记录输出一次进度
    if ((i + 1) % 10 === 0) {
      console.log(`已处理 ${i + 1}/${total} 条记录`)
    }
  }

  console.log(`地址提取完成: ${results.length} 条记录`)
  return results
}

module.exports = {
  extractAddressByLLM,
  extractAddressesFromData
}