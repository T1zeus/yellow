/**
 * 热点统计服务
 * 分析案发地点、实口居住地址、外卖收货地址的热点统计
 */

const HOT_SPOTS_THRESHOLD = parseInt(process.env.HOT_SPOTS_THRESHOLD || '2', 10)
const POPULATION_SPOTS_THRESHOLD = parseInt(process.env.POPULATION_SPOTS_THRESHOLD || '2', 10)
const SHOPPING_SPOTS_THRESHOLD = parseInt(process.env.SHOPPING_SPOTS_THRESHOLD || '2', 10)

/**
 * 预处理地址：提取到"弄"或"号"
 * @param {string} address - 地址
 * @returns {string} 处理后的地址
 */
function preprocessAddress(address) {
  if (!address || address === '') {
    return ''
  }

  const addr = String(address).trim()

  // 匹配到"弄"
  const matchNong = addr.match(/^(.*?弄)/)
  if (matchNong) {
    return matchNong[1]
  }

  // 匹配到"号"
  const matchHao = addr.match(/^(.*?号)/)
  if (matchHao) {
    return matchHao[1]
  }

  return addr
}

/**
 * 移除最后一个括号及其内容
 * @param {string} addr - 地址
 * @returns {string} 处理后的地址
 */
function removeLastBracket(addr) {
  if (!addr || addr === '') {
    return ''
  }

  const addrStr = String(addr).trim()
  // 移除最后的中文括号或英文括号及其内容
  return addrStr.replace(/(（[^（（）()]*）|\([^（）()]*\))$/, '').trim()
}

/**
 * 分析案发地点热点统计
 * @param {Array<Object>} data - 数据数组（需要包含 '案发地点' 字段）
 * @param {Object} options - 配置选项
 * @param {string} options.column - 列名（默认 '案发地点'）
 * @param {number} options.threshold - 阈值（默认 2）
 * @returns {Array<Object>} 统计结果
 */
function analyzeHotSpots(data, options = {}) {
  const {
    column = '案发地点',
    threshold = HOT_SPOTS_THRESHOLD,
  } = options

  if (!data || data.length === 0) {
    return []
  }

  // 检查列是否存在
  if (!data[0] || !(column in data[0])) {
    return []
  }

  // 过滤和清理数据 - 只保留需要的字段，避免包含主表的所有字段
  const workingData = data
    .map(row => ({
      证件号码: String(row['证件号码'] || '').trim(),
      姓名: String(row['姓名'] || '').trim(),
      [column]: String(row[column] || '').trim(),
      实口所属派出所: row['实口所属派出所'] ? String(row['实口所属派出所'] || '').trim() : undefined,
    }))
    .filter(row => {
      const value = row[column]
      return value !== '' && value !== '无地址'
    })

  if (workingData.length === 0) {
    return []
  }

  // 处理所属辖区（如果存在实口所属派出所）
  if (workingData[0].实口所属派出所 !== undefined && !('所属辖区' in workingData[0])) {
    workingData.forEach(row => {
      if (row.实口所属派出所) {
        row['所属辖区'] = String(row.实口所属派出所).replace(/.*分局/, '').trim()
      } else {
        row['所属辖区'] = ''
      }
    })
  }

  // 统计出现次数
  const locationCounts = new Map()
  const locationData = new Map() // 存储每个地址的所有数据行

  for (const row of workingData) {
    const location = row[column]
    if (!locationCounts.has(location)) {
      locationCounts.set(location, 0)
      locationData.set(location, [])
    }
    locationCounts.set(location, locationCounts.get(location) + 1)
    locationData.get(location).push(row)
  }

  // 转换为数组并过滤阈值
  const result = []
  for (const [location, count] of locationCounts.entries()) {
    if (count >= threshold) {
      const rows = locationData.get(location)

      // 获取所属辖区（取出现最多的）
      let station = ''
      if (rows[0] && '所属辖区' in rows[0]) {
        const stations = rows
          .map(r => String(r['所属辖区'] || '').trim())
          .filter(s => s !== '')
        if (stations.length > 0) {
          const stationCounts = new Map()
          for (const s of stations) {
            stationCounts.set(s, (stationCounts.get(s) || 0) + 1)
          }
          let maxCount = 0
          for (const [s, c] of stationCounts.entries()) {
            if (c > maxCount) {
              maxCount = c
              station = s
            }
          }
        }
      }

      // 获取姓名证件列表（去重）
      let nameIdList = ''
      if (rows[0] && '姓名' in rows[0] && '证件号码' in rows[0]) {
        const unique = new Map()
        for (const r of rows) {
          const name = String(r['姓名'] || '').trim()
          const id = String(r['证件号码'] || '').trim()
          if (name && id) {
            const key = `${name}|${id}`
            if (!unique.has(key)) {
              unique.set(key, { name, id })
            }
          }
        }

        const lines = Array.from(unique.values()).map(
          item => `姓名：${item.name}——证件号码：${item.id}`
        )
        nameIdList = lines.join('\n')
      }

      result.push({
        案发地点: location,
        出现次数: count,
        所属辖区: station,
        姓名证件列表: nameIdList,
      })
    }
  }

  // 按出现次数降序排序
  result.sort((a, b) => b.出现次数 - a.出现次数)

  return result
}

/**
 * 分析实口居住地址热点统计
 * @param {Array<Object>} data - 数据数组（需要包含 '居住地址' 字段）
 * @param {Object} options - 配置选项
 * @param {string} options.addressCol - 地址列名（默认 '居住地址'）
 * @param {number} options.threshold - 阈值（默认 2）
 * @returns {Array<Object>} 统计结果
 */
function analyzePopulationHotSpots(data, options = {}) {
  const {
    addressCol = '居住地址',
    threshold = POPULATION_SPOTS_THRESHOLD,
  } = options

  if (!data || data.length === 0) {
    return []
  }

  // 检查必要字段
  if (!data[0] || !(addressCol in data[0]) || !('姓名' in data[0]) || !('证件号码' in data[0])) {
    return []
  }

  // 预处理数据 - 只保留需要的字段，避免包含主表的所有字段
  const processedData = data.map(row => {
    const originalAddr = String(row[addressCol] || row['实口居住地址'] || '').trim()
    const processedAddr = preprocessAddress(originalAddr)

    return {
      证件号码: String(row['证件号码'] || '').trim(),
      姓名: String(row['姓名'] || '').trim(),
      原始居住地址: originalAddr,
      [addressCol]: processedAddr,
      实口所属派出所: row['实口所属派出所'] ? String(row['实口所属派出所'] || '').trim() : undefined,
    }
  }).filter(row => row[addressCol] !== '')

  // 判断是否有"实口所属派出所"
  const hasStation = processedData.length > 0 && processedData[0].实口所属派出所 !== undefined

  // 去重：同一人同一地址只算一次
  const uniqueMap = new Map()
  for (const row of processedData) {
    const addr = row[addressCol]
    const id = String(row['证件号码'] || '').trim()
    const key = `${addr}|${id}`

    if (!uniqueMap.has(key)) {
      uniqueMap.set(key, {
        地址: addr,
        原始地址: row['原始居住地址'],
        姓名: String(row['姓名'] || '').trim(),
        证件号码: id,
        实口所属派出所: hasStation ? String(row['实口所属派出所'] || '').trim() : '',
      })
    }
  }

  const uniqueData = Array.from(uniqueMap.values())

  // 处理实口所属派出所（去除"分局"）
  if (hasStation) {
    uniqueData.forEach(item => {
      if (item.实口所属派出所) {
        item.实口所属派出所 = item.实口所属派出所.replace(/.*分局/, '').trim()
      }
    })
  }

  // 统计每个地址的不同人数
  const addressCounts = new Map()
  const addressGroups = new Map()

  for (const item of uniqueData) {
    const addr = item.地址
    if (!addressCounts.has(addr)) {
      addressCounts.set(addr, new Set())
      addressGroups.set(addr, [])
    }
    addressCounts.get(addr).add(item.证件号码)
    addressGroups.get(addr).push(item)
  }

  // 转换为结果数组
  const result = []
  for (const [addr, idSet] of addressCounts.entries()) {
    const count = idSet.size
    if (count >= threshold) {
      const items = addressGroups.get(addr)

      // 生成姓名证件列表
      let nameIdList = ''
      if (hasStation) {
        const lines = items.map(
          item => `姓名：${item.姓名}——证件号码：${item.证件号码}——原始地址：${item.原始地址}——实口所属派出所：${item.实口所属派出所}`
        )
        nameIdList = lines.join('\n')
      } else {
        const lines = items.map(
          item => `姓名：${item.姓名}——证件号码：${item.证件号码}——原始地址：${item.原始地址}`
        )
        nameIdList = lines.join('\n')
      }

      // 获取实口所属派出所（取出现最多的）
      let station = ''
      if (hasStation) {
        const stations = items.map(item => item.实口所属派出所).filter(s => s !== '')
        if (stations.length > 0) {
          const stationCounts = new Map()
          for (const s of stations) {
            stationCounts.set(s, (stationCounts.get(s) || 0) + 1)
          }
          let maxCount = 0
          for (const [s, c] of stationCounts.entries()) {
            if (c > maxCount) {
              maxCount = c
              station = s
            }
          }
        }
      }

      const resultItem = {
        居住地址: addr,
        出现次数: count,
        姓名证件列表: nameIdList,
      }

      if (hasStation) {
        resultItem['实口所属派出所'] = station
      }

      result.push(resultItem)
    }
  }

  // 按出现次数降序排序
  result.sort((a, b) => b.出现次数 - a.出现次数)

  return result
}

/**
 * 分析外卖收货地址热点统计
 * @param {Array<Object>} shoppingData - 购物数据数组（需要包含 '收货地址' 字段）
 * @param {Object} options - 配置选项
 * @param {string} options.addressCol - 地址列名（默认 '收货地址'）
 * @param {number} options.threshold - 阈值（默认 2）
 * @returns {Array<Object>} 统计结果
 */
function analyzeShoppingSpots(shoppingData, options = {}) {
  const {
    addressCol = '收货地址',
    threshold = SHOPPING_SPOTS_THRESHOLD,
  } = options

  if (!shoppingData || shoppingData.length === 0) {
    return []
  }

  // 检查必要字段
  if (!shoppingData[0] || !(addressCol in shoppingData[0]) || !('姓名' in shoppingData[0]) || !('证件号码' in shoppingData[0])) {
    return []
  }

  // 预处理数据：移除最后括号，去重 - 只保留需要的字段，避免包含主表的所有字段
  const processedData = shoppingData.map(row => ({
    证件号码: String(row['证件号码'] || '').trim(),
    姓名: String(row['姓名'] || '').trim(),
    原始收货地址: String(row[addressCol] || '').trim(),
    [addressCol]: removeLastBracket(row[addressCol]),
  })).filter(row => row[addressCol] !== '')

  // 去重：同一人同一地址只算一次
  const uniqueMap = new Map()
  for (const row of processedData) {
    const addr = row[addressCol]
    const id = String(row['证件号码'] || '').trim()
    const key = `${addr}|${id}`

    if (!uniqueMap.has(key)) {
      uniqueMap.set(key, {
        地址: addr,
        原始地址: row['原始收货地址'],
        姓名: String(row['姓名'] || '').trim(),
        证件号码: id,
      })
    }
  }

  const uniqueData = Array.from(uniqueMap.values())

  // 统计每个地址的不同人数
  const addressCounts = new Map()
  const addressGroups = new Map()

  for (const item of uniqueData) {
    const addr = item.地址
    if (!addressCounts.has(addr)) {
      addressCounts.set(addr, new Set())
      addressGroups.set(addr, [])
    }
    addressCounts.get(addr).add(item.证件号码)
    addressGroups.get(addr).push(item)
  }

  // 转换为结果数组
  const result = []
  for (const [addr, idSet] of addressCounts.entries()) {
    const count = idSet.size
    if (count >= threshold) {
      const items = addressGroups.get(addr)

      // 生成姓名证件列表
      const lines = items.map(
        item => `姓名：${item.姓名}——证件号码：${item.证件号码}——原始地址：${item.原始地址}`
      )

      result.push({
        外卖收货地址: addr,
        出现次数: count,
        姓名证件列表: lines.join('\n'),
      })
    }
  }

  // 按出现次数降序排序
  result.sort((a, b) => b.出现次数 - a.出现次数)

  return result
}

module.exports = {
  analyzeHotSpots,
  analyzePopulationHotSpots,
  analyzeShoppingSpots
}