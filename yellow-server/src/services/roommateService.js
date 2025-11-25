/**
 * 同住人分析服务
 * 计算同住情况，生成居住详细
 */

const { cleanIdNumber } = require('./dataMergeService')

/**
 * 安全获取值（处理 null/undefined/NaN）
 * @param {any} val - 值
 * @returns {string} 处理后的字符串
 */
function safeGet(val) {
  if (val == null || val === '') {
    return '无'
  }
  const s = String(val).trim()
  if (s.toLowerCase() === 'nan' || s === '' || s === 'undefined' || s === 'null') {
    return '无'
  }
  return s
}

/**
 * 计算同住情况
 * @param {Array<Object>} roommateData - 同住人数据（包含居住地址、证件号码等信息）
 * @param {Array<Object>} mergedData - 主数据（需要添加居住情况的数据）
 * @returns {Array<Object>} 添加了 '居住情况' 和 '居住详细' 字段的数据
 */
function calculateRoommate(roommateData, mergedData) {
  if (!mergedData || mergedData.length === 0) {
    return mergedData.map(row => ({
      ...row,
      居住情况: '独居',
      居住详细: '',
    }))
  }

  console.log(`开始计算同住情况: ${mergedData.length} 条记录`)
  console.log(`同住人数据: ${roommateData ? roommateData.length : 0} 条记录`)

  // 如果没有同住人数据，所有记录都标记为独居
  if (!roommateData || roommateData.length === 0) {
    console.log('同住人数据为空，所有记录标记为独居')
    return mergedData.map(row => ({
      ...row,
      居住情况: '独居',
      居住详细: '',
    }))
  }

  // 1. 清理和标准化数据（使用 cleanIdNumber 统一证件号码格式）
  const cleanedRoommateData = roommateData.map(row => ({
    ...row,
    居住地址: String(row['居住地址'] || row['实口居住地址'] || '').trim(),
    证件号码: cleanIdNumber(row['证件号码'] || ''),
  })).filter(row => {
    // 过滤掉居住地址为空或证件号码无效的数据
    const addr = row['居住地址']
    const id = row['证件号码']
    return addr && addr !== '' && addr !== 'undefined' && addr !== 'null' && id && id !== ''
  })

  const cleanedMergedData = mergedData.map(row => ({
    ...row,
    居住地址: String(row['居住地址'] || row['实口居住地址'] || '').trim(),
    证件号码: cleanIdNumber(row['证件号码'] || ''),
  })).filter(row => {
    // 过滤掉证件号码无效的数据
    const id = row['证件号码']
    return id && id !== ''
  })

  // 2. 按居住地址分组（使用 Map 优化查找）
  // 同时去重：同一地址下，同一证件号码只保留一条记录
  const addressGroups = new Map()
  const seenIds = new Map() // 用于记录每个地址下已见过的证件号码
  for (const row of cleanedRoommateData) {
    const addr = row['居住地址']
    const id = row['证件号码']
    if (addr && addr !== '' && id && id !== '') {
      if (!addressGroups.has(addr)) {
        addressGroups.set(addr, [])
        seenIds.set(addr, new Set())
      }
      // 如果该地址下还没有这个证件号码，则添加
      if (!seenIds.get(addr).has(id)) {
        addressGroups.get(addr).push(row)
        seenIds.get(addr).add(id)
      }
    }
  }

  console.log(`按地址分组完成: ${addressGroups.size} 个不同地址`)

  // 3. 为每条主数据计算同住情况
  const result = cleanedMergedData.map(row => {
    const addr = row['居住地址']
    const idc = row['证件号码']

    // 如果没有居住地址，返回独居
    if (!addr || addr === '') {
      return {
        ...row,
        居住情况: '独居',
        居住详细: '',
      }
    }

    // 查找该地址的所有人员
    const group = addressGroups.get(addr) || []

    // 排除自己，查找其他同住人（idc 已经是清理后的证件号码）
    const others = group.filter(r => {
      const otherId = r['证件号码'] // 已经是清理后的证件号码
      return otherId !== '' && otherId !== idc
    })

    // 如果没有其他同住人，返回独居
    if (others.length === 0) {
      return {
        ...row,
        居住情况: '独居',
        居住详细: '',
      }
    }

    // 生成同住人详细信息
    const people = []
    for (const r of others) {
      const name = safeGet(r['姓名'])
      const card = safeGet(r['证件号码'])
      const phone = safeGet(r['手机号码'] || r['联系电话'])
      const sb = safeGet(r['社保'] || r['单位名称'])
      const qk = safeGet(r['前科'] || r['前科情况'])

      people.push(
        `姓名： ${name}, 证件号：${card}, 手机号：${phone}， 社保：${sb}, 前科： ${qk}`
      )
    }

    const detail = ` ${addr} - 跟${others.length}人同住: \n${people.join('\n')}`

    return {
      ...row,
      居住情况: '同住人',
      居住详细: detail,
    }
  })

  // 统计同住情况
  const stats = {
    独居: 0,
    同住人: 0,
  }

  for (const row of result) {
    const status = row['居住情况'] || '独居'
    stats[status] = (stats[status] || 0) + 1
  }

  console.log('同住情况统计:')
  console.log(`- 独居: ${stats['独居']} 条`)
  console.log(`- 同住人: ${stats['同住人']} 条`)

  return result
}

module.exports = {
  calculateRoommate
}