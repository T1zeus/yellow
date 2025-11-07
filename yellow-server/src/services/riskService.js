/**
 * 风险评估服务
 * 根据多个维度计算预警等级（高/中/低）
 */

/**
 * 检查同住人中是否有卖淫前科
 * @param {string} address - 居住地址
 * @param {string} idNumber - 证件号码（当前人）
 * @param {Array<Object>} roommateData - 同住人数据
 * @returns {boolean} 是否有卖淫前科
 */
function hasProstitutionRecordInRoommate(address, idNumber, roommateData) {
  if (!address || !roommateData || roommateData.length === 0) {
    return false
  }

  const addressStr = String(address).trim()
  const idStr = String(idNumber || '').trim()

  // 查找同地址的其他人员
  for (const row of roommateData) {
    const rowAddress = String(row['居住地址'] || row['实口居住地址'] || '').trim()
    const rowId = String(row['证件号码'] || '').trim()

    // 跳过自己
    if (rowId === idStr) {
      continue
    }

    // 检查是否同地址
    if (rowAddress === addressStr) {
      const qianke = String(row['前科'] || row['前科情况'] || row['案由'] || '').trim()
      if (qianke && qianke.includes('卖淫')) {
        return true
      }
    }
  }

  return false
}

/**
 * 计算单条记录的预警等级
 * @param {Object} row - 数据行
 * @param {Array<Object>} roommateData - 同住人数据（用于检查前科）
 * @returns {string} 预警等级：'高'、'中'、'低'
 */
function classifyAlert(row, roommateData) {
  const address = String(row['居住地址'] || row['实口居住地址'] || '').trim()
  const idNumber = String(row['证件号码'] || '').trim()

  // ========== 高风险判断 ==========
  // 满足任一条件即为高风险
  const abnormalShopping = String(row['异常购物'] || '').trim()
  const abnormalFund = parseInt(row['异常资金'] || 0, 10)
  const cohabitMaleCount = parseInt(row['同住男人数'] || 0, 10)

  if (abnormalShopping === '高' || abnormalFund === 1 || cohabitMaleCount >= 3) {
    return '高'
  }

  // ========== 中风险判断 ==========
  // 修复：先检查同住人前科（与 antiporn 逻辑一致）
  const hasProstitutionRecord = hasProstitutionRecordInRoommate(address, idNumber, roommateData)
  if (hasProstitutionRecord) {
    return '中'
  }

  // 检查中风险条件（使用 any 逻辑：任一条件满足即中风险）
  const mediumConditions = [
    abnormalShopping === '中' || abnormalShopping === '低',
    Boolean(String(row['从业单位'] || row['从业单位2'] || '').trim()),
    cohabitMaleCount <= 2,
    (String(row['地点分类'] || '').trim().includes('场所') && String(row['收货地址分类'] || '').trim().includes('宾馆')),
    (String(row['地点分类'] || '').trim().includes('宾馆') && String(row['收货地址分类'] || '').trim().includes('宾馆')),
  ]

  // 中风险：任一条件满足即返回中风险
  if (mediumConditions.some(condition => condition)) {
    return '中'
  }

  // ========== 低风险 ==========
  // 其他所有情况
  return '低'
}

/**
 * 为数据添加预警等级
 * @param {Array<Object>} data - 主数据
 * @param {Array<Object>} roommateData - 同住人数据（可选）
 * @returns {Array<Object>} 添加了 '预警状态' 字段的数据
 */
function addAlertLevel(data, roommateData = []) {
  if (!data || data.length === 0) {
    return data.map(row => ({
      ...row,
      预警状态: '低',
    }))
  }

  console.log(`开始计算预警等级: ${data.length} 条记录`)
  console.log(`同住人数据: ${roommateData ? roommateData.length : 0} 条记录`)

  const result = data.map(row => {
    const alertLevel = classifyAlert(row, roommateData)
    return {
      ...row,
      预警状态: alertLevel,
    }
  })

  // 统计预警等级分布
  const levelCounts = {
    高: 0,
    中: 0,
    低: 0,
  }

  for (const row of result) {
    const level = row['预警状态'] || '低'
    if (levelCounts.hasOwnProperty(level)) {
      levelCounts[level]++
    }
  }

  console.log('预警等级统计:')
  console.log(`- 高: ${levelCounts['高']} 条`)
  console.log(`- 中: ${levelCounts['中']} 条`)
  console.log(`- 低: ${levelCounts['低']} 条`)

  return result
}

module.exports = {
  addAlertLevel,
  hasProstitutionRecordInRoommate,
  classifyAlert
}