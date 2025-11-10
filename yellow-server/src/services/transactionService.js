const fs = require('fs')
const path = require('path')
const { readExcelToArray, writeArrayToExcel } = require('./excelService')
const { cleanIdNumber } = require('./dataMergeService')

/**
 * 合并交易文件夹，生成汇总数据
 * 支持两种文件夹结构：
 * 1. transaction/证件号码-姓名/xxx.xlsx（子文件夹结构）
 * 2. transaction/xxx.xlsx（直接文件结构，从文件内容中提取证件号码）
 * @param {string} folderPath - 交易文件夹路径
 * @returns {Array<Object>} 合并后的交易数据数组
 */
function mergeTransactionFolder(folderPath) {
  if (!folderPath || !fs.existsSync(folderPath)) {
    console.warn('交易文件夹不存在:', folderPath)
    return []
  }

  const allData = []
  const items = fs.readdirSync(folderPath, { withFileTypes: true })
  
  // 调试：显示文件夹内容
  console.log(`交易文件夹路径: ${folderPath}`)
  console.log(`文件夹内容: ${items.map(item => `${item.isDirectory() ? '[目录]' : '[文件]'} ${item.name}`).join(', ')}`)
  
  // 检查是否有子文件夹
  const hasSubFolders = items.some(item => item.isDirectory())
  
  if (hasSubFolders) {
    // 结构1：子文件夹结构（证件号码-姓名/xxx.xlsx）
    console.log('检测到子文件夹结构，按证件号码-姓名格式处理...')
    for (const item of items) {
      if (!item.isDirectory()) continue

      const personFolder = item.name
      const personPath = path.join(folderPath, personFolder)

      // 解析文件夹名：格式为 "证件号码-姓名"
      let personId, name
      try {
        const parts = personFolder.split('-', 2)
        if (parts.length < 2) {
          console.warn(`跳过文件夹（格式不正确）: ${personFolder}`)
          continue
        }
        personId = parts[0]
        name = parts[1]
      } catch (e) {
        console.warn(`跳过文件夹 ${personFolder}: ${e.message}`)
        continue
      }

      // 读取该文件夹内的所有 Excel 文件
      const files = fs.readdirSync(personPath)
      for (const fileName of files) {
        if (!fileName.endsWith('.xlsx') && !fileName.endsWith('.xls')) continue

        const filePath = path.join(personPath, fileName)
        try {
          const rows = readExcelToArray(filePath)
          // 为每行添加元数据
          const enrichedRows = rows.map(row => ({
            ...row,
            原文件名: fileName,
            证件号码: personId,
            姓名备注: name,
          }))
          allData.push(...enrichedRows)
        } catch (e) {
          console.error(`读取文件失败 ${filePath}:`, e.message)
        }
      }
    }
  } else {
    // 结构2：直接文件结构（transaction/xxx.xlsx）
    console.log('检测到直接文件结构，从文件内容中提取证件号码...')
    let fileCount = 0
    let totalRows = 0
    
    for (const item of items) {
      if (item.isDirectory()) continue
      
      const fileName = item.name
      if (!fileName.endsWith('.xlsx') && !fileName.endsWith('.xls')) continue

      const filePath = path.join(folderPath, fileName)
      try {
        const rows = readExcelToArray(filePath)
        
        // 从文件内容中提取证件号码（如果文件中已有证件号码字段，则使用；否则尝试从文件名或其他字段提取）
        const enrichedRows = rows.map(row => {
          // 如果文件中已有证件号码字段，使用它；否则尝试从其他字段提取
          let personId = row['证件号码'] || row['交易主体证件号码'] || row['收款人证件号码'] || row['证件号'] || ''
          personId = cleanIdNumber(personId || '')
          
          return {
            ...row,
            原文件名: fileName,
            证件号码: personId,
            姓名备注: row['姓名'] || row['交易主体姓名'] || row['收款人姓名'] || '',
          }
        })
        
        allData.push(...enrichedRows)
        fileCount++
        totalRows += enrichedRows.length
      } catch (e) {
        console.error(`读取文件失败 ${filePath}:`, e.message)
      }
    }
    
    console.log(`直接文件结构处理完成: ${fileCount} 个文件，共 ${totalRows} 条记录`)
  }

  console.log(`合并交易文件夹完成，共 ${allData.length} 条记录`)
  return allData
}

/**
 * 通过支付账号匹配补充交易数据中的证件号码
 * @param {Array<Object>} transactionData - 交易数据（可能缺少证件号码）
 * @param {Array<Object>} mainData - 主表数据（包含证件号码和可能的支付账号字段）
 * @returns {Array<Object>} 补充了证件号码的交易数据
 */
function enrichTransactionWithIdNumber(transactionData, mainData) {
  if (!transactionData || transactionData.length === 0) {
    return transactionData
  }
  
  if (!mainData || mainData.length === 0) {
    console.warn('主表数据为空，无法通过支付账号匹配证件号码')
    return transactionData
  }
  
  // 创建主表的支付账号到证件号码的映射
  // 尝试多个可能的字段名
  const accountToIdMap = new Map()
  for (const row of mainData) {
    const id = cleanIdNumber(row['证件号码'] || '')
    if (!id) continue
    
    // 尝试从多个可能的字段中获取支付账号
    const possibleAccountFields = [
      '支付账号', '支付帐号', '收款支付帐号', '付款支付帐号',
      '微信支付账号', '支付宝账号', '手机号码' // 手机号码也可能用于匹配
    ]
    
    for (const field of possibleAccountFields) {
      const account = String(row[field] || '').trim()
      if (account) {
        // 如果该账号还没有映射，或者当前证件号码更有效（优先使用有证件号码的记录）
        if (!accountToIdMap.has(account) || !accountToIdMap.get(account)) {
          accountToIdMap.set(account, id)
        }
      }
    }
  }
  
  console.log(`创建支付账号映射: ${accountToIdMap.size} 个账号`)
  
  // 为交易数据补充证件号码
  let matchedCount = 0
  const enrichedData = transactionData.map(row => {
    // 如果已经有证件号码，直接返回
    const existingId = cleanIdNumber(row['证件号码'] || '')
    if (existingId) {
      return row
    }
    
    // 尝试通过收款支付帐号匹配
    const receiveAccount = String(row['收款支付帐号'] || '').trim()
    if (receiveAccount && accountToIdMap.has(receiveAccount)) {
      matchedCount++
      return {
        ...row,
        证件号码: accountToIdMap.get(receiveAccount)
      }
    }
    
    // 如果收款支付帐号匹配失败，尝试付款支付帐号（虽然不太可能，但尝试一下）
    const payAccount = String(row['付款支付帐号'] || '').trim()
    if (payAccount && accountToIdMap.has(payAccount)) {
      matchedCount++
      return {
        ...row,
        证件号码: accountToIdMap.get(payAccount)
      }
    }
    
    return row
  })
  
  const totalRows = enrichedData.length
  const validIdCount = enrichedData.filter(row => {
    const id = cleanIdNumber(row['证件号码'] || '')
    return id && id !== ''
  }).length
  
  console.log(`支付账号匹配完成: ${totalRows} 条记录，${matchedCount} 条通过支付账号匹配，${validIdCount} 条有有效证件号码`)
  
  return enrichedData
}

/**
 * 检测异常交易
 * 检测规则：
 * 1. 交易主体的出入账标识 = "入账"
 * 2. 交易金额 > minAmount（默认500）
 * 3. 交易金额是 100 的倍数
 * 4. 同一组合（证件号码+交易金额+付款支付帐号）只出现 1 次
 * 5. 同一证件号码+交易金额的组合，不同发送账户数 >= minSenders（默认1）
 * 
 * @param {Array<Object>} transactionData - 交易数据数组
 * @param {Object} options - 配置选项
 * @param {number} options.minAmount - 最小交易金额（默认500）
 * @param {number} options.minSenders - 最小发送账户数（默认1）
 * @returns {Array<Object>} 异常交易列表（按证件号码聚合）
 */
function detectAbnormalTransactions(transactionData, options = {}) {
  const {
    minAmount = parseInt(process.env.MIN_TRANSACTION_AMOUNT || '500', 10),
    minSenders = parseInt(process.env.MIN_SENDERS_COUNT || '1', 10),
  } = options

  if (!transactionData || transactionData.length === 0) {
    return []
  }

  // 1. 清理列名（去除空格）
  const cleaned = transactionData.map(row => {
    const cleanedRow = {}
    for (const [key, value] of Object.entries(row)) {
      cleanedRow[key.trim()] = value
    }
    return cleanedRow
  })

  // 2. 统一列名：付款支付账号 -> 付款支付帐号
  const normalized = cleaned.map(row => {
    if (row['付款支付账号'] && !row['付款支付帐号']) {
      row['付款支付帐号'] = row['付款支付账号']
      delete row['付款支付账号']
    }
    return row
  })

  // 3. 筛选：只保留"入账"记录
  const incoming = normalized.filter(
    row => String(row['交易主体的出入账标识'] || '').trim() === '入账'
  )

  if (incoming.length === 0) {
    console.log('没有入账记录')
    return []
  }

  // 4. 转换交易金额为数字，并筛选
  const withAmount = incoming
    .map(row => {
      // 处理金额中的逗号（如：1,000）
      const amountStr = String(row['交易金额'] || '0').replace(/,/g, '')
      const amount = parseFloat(amountStr)
      return {
        ...row,
        交易金额_数值: isNaN(amount) ? 0 : amount,
      }
    })
    .filter(row => {
      const amount = row.交易金额_数值
      // 金额 > minAmount 且是 100 的倍数
      return amount > minAmount && amount % 100 === 0
    })

  if (withAmount.length === 0) {
    console.log('没有符合条件的交易记录')
    return []
  }

  // 5. 统计每个组合的出现次数（清理证件号码）
  // 组合 = 证件号码 + 交易金额 + 付款支付帐号
  const countMap = new Map()
  for (const row of withAmount) {
    const cleanedId = cleanIdNumber(row['证件号码'] || '')
    if (!cleanedId) continue  // 跳过无效证件号码
    const key = `${cleanedId}|${row.交易金额_数值}|${row['付款支付帐号'] || ''}`
    countMap.set(key, (countMap.get(key) || 0) + 1)
  }

  // 6. 只保留出现次数为1的组合（确保同一组合只出现一次）
  const validRows = withAmount.filter(row => {
    const cleanedId = cleanIdNumber(row['证件号码'] || '')
    if (!cleanedId) return false  // 跳过无效证件号码
    const key = `${cleanedId}|${row.交易金额_数值}|${row['付款支付帐号'] || ''}`
    return countMap.get(key) === 1
  })

  // 7. 按证件号码+交易金额分组（清理证件号码）
  const grouped = new Map()
  for (const row of validRows) {
    const cleanedId = cleanIdNumber(row['证件号码'] || '')
    if (!cleanedId) continue  // 跳过无效证件号码
    const groupKey = `${cleanedId}|${row.交易金额_数值}`
    if (!grouped.has(groupKey)) {
      grouped.set(groupKey, [])
    }
    grouped.get(groupKey).push(row)
  }

  // 8. 检测异常：统计每组的不同发送账户数
  const abnormal = []
  for (const [groupKey, groupRows] of grouped.entries()) {
    const uniqueSenders = new Set()
    for (const row of groupRows) {
      const sender = String(row['付款支付帐号'] || '').trim()
      if (sender) uniqueSenders.add(sender)
    }

    // 如果不同发送账户数 >= minSenders，则判定为异常
    if (uniqueSenders.size >= minSenders) {
      const [personId, amount] = groupKey.split('|')
      const amountNum = parseFloat(amount)

      // 生成资金备注
      const beizhuList = []
      for (const row of groupRows) {
        const sender = String(row['付款支付帐号'] || '').trim()
        // 优先使用交易时间，如果没有则使用交易时间_dt（如果已转换为字符串）
        let time = row['交易时间'] || ''
        if (!time && row['交易时间_dt']) {
          // 如果交易时间_dt 是 Date 对象，格式化为字符串
          if (row['交易时间_dt'] instanceof Date) {
            time = row['交易时间_dt'].toLocaleString('zh-CN', {
              year: 'numeric',
              month: '2-digit',
              day: '2-digit',
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit',
              hour12: false
            })
          } else {
            time = String(row['交易时间_dt'] || '')
          }
        }
        beizhuList.push(`- ${time} 收到来自 ${sender} 的转账`)
      }

      abnormal.push({
        证件号码: personId,  // 已经是清理后的格式
        交易金额: amountNum,
        可疑发送账户数量: uniqueSenders.size,
        交易次数: groupRows.length,
        资金备注: `账户 ${personId} 收：${amountNum}元\n${beizhuList.join('\n')}`,
      })
    }
  }

  // 9. 按证件号码聚合（合并同一人的多条异常记录）
  const finalMap = new Map()
  for (const item of abnormal) {
    const id = item.证件号码
    if (!finalMap.has(id)) {
      finalMap.set(id, {
        证件号码: id,
        交易金额: 0,
        可疑发送账户数量: 0,
        交易次数: 0,
        资金备注: [],
      })
    }

    const existing = finalMap.get(id)
    existing.交易金额 += item.交易金额
    existing.可疑发送账户数量 += item.可疑发送账户数量
    existing.交易次数 += item.交易次数
    existing.资金备注.push(item.资金备注)
  }

  // 转换为数组并合并资金备注（使用换行符连接，与 antiporn 一致）
  const final = Array.from(finalMap.values()).map(item => ({
    ...item,
    资金备注: item.资金备注.join('\n'),  // 使用换行符连接，与 antiporn 的 '\n'.join 一致
  }))

  console.log(`异常交易检测完成，发现 ${final.length} 个可疑账户`)
  return final
}

/**
 * 标记主表中的异常资金
 * @param {Array<Object>} mainData - 主表数据
 * @param {Array<Object>} abnormalData - 异常交易数据
 * @returns {Array<Object>} 标记后的主表数据（添加 异常资金 和 资金备注 字段）
 */
function flagAbnormalTransaction(mainData, abnormalData) {
  if (!mainData || mainData.length === 0) return mainData
  
  if (!abnormalData || abnormalData.length === 0) {
    // 修复：没有异常时，应该设置为 0 而不是空字符串
    return mainData.map(row => ({
      ...row,
      异常资金: 0,
      资金备注: '',
    }))
  }

  // 创建异常数据索引（按证件号码）- 使用 cleanIdNumber 统一格式
  const abnormalMap = new Map()
  for (const item of abnormalData) {
    const id = cleanIdNumber(item.证件号码 || '')
    if (id) {
      abnormalMap.set(id, item)
    }
  }

  // 标记主表 - 使用 cleanIdNumber 统一格式
  return mainData.map(row => {
    const id = cleanIdNumber(row['证件号码'] || '')
    const abnormal = abnormalMap.get(id)

    if (abnormal) {
      return {
        ...row,
        异常资金: 1,  // 修复：明确设置为 1
        资金备注: abnormal.资金备注 || '',
      }
    } else {
      return {
        ...row,
        异常资金: 0,  // 修复：明确设置为 0
        资金备注: '',
      }
    }
  })
}

module.exports = {
  mergeTransactionFolder,
  enrichTransactionWithIdNumber,
  detectAbnormalTransactions,
  flagAbnormalTransaction
}