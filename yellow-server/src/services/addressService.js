/**
 * 地址提取与分类服务
 * 从简要案情中提取案发地点，并进行分类
 */

const { askDeepSeek } = require('../utils/llmClient')
const LLMCacheManager = require('../utils/llmCache')
const { classifyAddress } = require('./shoppingService')

// 初始化缓存管理器（使用 llm_cache.json）
const addressCache = new LLMCacheManager('llm_cache.json')

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
    const result = addressCache.get(textStr)
    console.log(`[LLM Cache] 地址提取 ${textStr.substring(0, 50)}... -> ${result}`)
    
    // 验证结果
    if (!result || result.includes('无地址') || result.includes('不详') || result.trim().length < 6) {
      return ''
    }
    return result
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

    // 保存到缓存
    addressCache.set(textStr, result)

    // 验证结果：如果包含"无地址"或"不详"，或长度小于6，返回空字符串
    if (!result || result.includes('无地址') || result.includes('不详') || result.trim().length < 6) {
      return ''
    }

    return result
  } catch (error) {
    console.error(`[地址提取错误] ${textStr.substring(0, 50)}...:`, error.message)
    return ''
  }
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