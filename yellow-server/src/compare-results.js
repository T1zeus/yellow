const { readExcelToArray } = require('./services/excelService')
const path = require('path')
const fs = require('fs')

/**
 * 对比两个 result.xlsx 文件
 * @param {string} file1Path - 第一个文件路径（yellow-server）
 * @param {string} file2Path - 第二个文件路径（antiporn）
 */
function compareResults(file1Path, file2Path) {
  console.log('\n========== 开始对比分析 ==========\n')
  
  // 读取文件
  console.log(`读取文件1: ${file1Path}`)
  const data1 = readExcelToArray(file1Path)
  console.log(`  记录数: ${data1.length}`)
  
  console.log(`\n读取文件2: ${file2Path}`)
  const data2 = readExcelToArray(file2Path)
  console.log(`  记录数: ${data2.length}`)
  
  if (data1.length === 0 || data2.length === 0) {
    console.error('文件为空，无法对比')
    return
  }
  
  // 获取列名
  const columns1 = Object.keys(data1[0])
  const columns2 = Object.keys(data2[0])
  
  console.log(`\n========== 字段对比 ==========`)
  console.log(`\n文件1列数: ${columns1.length}`)
  console.log(`文件2列数: ${columns2.length}`)
  
  // 找出共同字段和独有字段
  const commonColumns = columns1.filter(col => columns2.includes(col))
  const onlyIn1 = columns1.filter(col => !columns2.includes(col))
  const onlyIn2 = columns2.filter(col => !columns1.includes(col))
  
  console.log(`\n共同字段数: ${commonColumns.length}`)
  console.log(`文件1独有字段: ${onlyIn1.length}`)
  console.log(`文件2独有字段: ${onlyIn2.length}`)
  
  if (onlyIn1.length > 0) {
    console.log(`\n文件1独有字段:`)
    onlyIn1.forEach((col, i) => console.log(`  ${i + 1}. ${col}`))
  }
  
  if (onlyIn2.length > 0) {
    console.log(`\n文件2独有字段:`)
    onlyIn2.forEach((col, i) => console.log(`  ${i + 1}. ${col}`))
  }
  
  // 按证件号码建立索引
  const index1 = new Map()
  const index2 = new Map()
  
  data1.forEach(row => {
    const id = String(row['证件号码'] || '').trim()
    if (id) {
      if (!index1.has(id)) {
        index1.set(id, [])
      }
      index1.get(id).push(row)
    }
  })
  
  data2.forEach(row => {
    const id = String(row['证件号码'] || '').trim()
    if (id) {
      if (!index2.has(id)) {
        index2.set(id, [])
      }
      index2.get(id).push(row)
    }
  })
  
  console.log(`\n========== 数据记录对比 ==========`)
  console.log(`\n文件1唯一证件号码数: ${index1.size}`)
  console.log(`文件2唯一证件号码数: ${index2.size}`)
  
  // 找出共同证件号码和独有证件号码
  const commonIds = new Set()
  const onlyIn1Ids = new Set()
  const onlyIn2Ids = new Set()
  
  for (const id of index1.keys()) {
    if (index2.has(id)) {
      commonIds.add(id)
    } else {
      onlyIn1Ids.add(id)
    }
  }
  
  for (const id of index2.keys()) {
    if (!index1.has(id)) {
      onlyIn2Ids.add(id)
    }
  }
  
  console.log(`\n共同证件号码数: ${commonIds.size}`)
  console.log(`文件1独有证件号码数: ${onlyIn1Ids.size}`)
  console.log(`文件2独有证件号码数: ${onlyIn2Ids.size}`)
  
  // 对比共同字段的值差异
  console.log(`\n========== 共同字段值对比 ==========`)
  const differences = []
  
  for (const id of commonIds) {
    const rows1 = index1.get(id)
    const rows2 = index2.get(id)
    
    // 取第一条记录对比
    const row1 = rows1[0]
    const row2 = rows2[0]
    
    for (const col of commonColumns) {
      const val1 = String(row1[col] || '').trim()
      const val2 = String(row2[col] || '').trim()
      
      if (val1 !== val2) {
        differences.push({
          id,
          姓名: row1['姓名'] || row2['姓名'] || '',
          字段: col,
          文件1值: val1,
          文件2值: val2,
        })
      }
    }
  }
  
  console.log(`\n发现 ${differences.length} 处字段值差异`)
  
  if (differences.length > 0) {
    console.log(`\n前20处差异:`)
    differences.slice(0, 20).forEach((diff, i) => {
      console.log(`\n${i + 1}. 证件号码: ${diff.id}, 姓名: ${diff.姓名}`)
      console.log(`   字段: ${diff.字段}`)
      console.log(`   文件1: ${diff.文件1值.substring(0, 100)}`)
      console.log(`   文件2: ${diff.文件2值.substring(0, 100)}`)
    })
    
    // 按字段统计差异
    const fieldDiffCounts = new Map()
    differences.forEach(diff => {
      fieldDiffCounts.set(diff.字段, (fieldDiffCounts.get(diff.字段) || 0) + 1)
    })
    
    console.log(`\n字段差异统计:`)
    Array.from(fieldDiffCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .forEach(([field, count]) => {
        console.log(`  ${field}: ${count} 处差异`)
      })
  }
  
  // 对比关键字段的统计
  console.log(`\n========== 关键字段统计对比 ==========`)
  
  const keyFields = ['预警状态', '异常购物', '异常资金', '前科次数', '入住次数', '同住男人数']
  
  keyFields.forEach(field => {
    if (commonColumns.includes(field)) {
      console.log(`\n字段: ${field}`)
      
      // 文件1统计
      const stats1 = new Map()
      data1.forEach(row => {
        const val = String(row[field] || '').trim()
        stats1.set(val, (stats1.get(val) || 0) + 1)
      })
      
      // 文件2统计
      const stats2 = new Map()
      data2.forEach(row => {
        const val = String(row[field] || '').trim()
        stats2.set(val, (stats2.get(val) || 0) + 1)
      })
      
      // 合并所有值
      const allValues = new Set([...stats1.keys(), ...stats2.keys()])
      
      allValues.forEach(val => {
        const count1 = stats1.get(val) || 0
        const count2 = stats2.get(val) || 0
        if (count1 !== count2) {
          console.log(`  "${val}": 文件1=${count1}, 文件2=${count2}, 差异=${count1 - count2}`)
        } else {
          console.log(`  "${val}": ${count1} (一致)`)
        }
      })
    }
  })
  
  // 生成详细差异报告
  const reportPath = path.join(__dirname, '..', 'data', 'results', 'comparison-report.txt')
  const report = []
  report.push('========== 对比分析报告 ==========')
  report.push(`\n文件1: ${file1Path}`)
  report.push(`文件2: ${file2Path}`)
  report.push(`\n文件1记录数: ${data1.length}`)
  report.push(`文件2记录数: ${data2.length}`)
  report.push(`\n共同证件号码: ${commonIds.size}`)
  report.push(`文件1独有: ${onlyIn1Ids.size}`)
  report.push(`文件2独有: ${onlyIn2Ids.size}`)
  report.push(`\n字段差异总数: ${differences.length}`)
  
  if (onlyIn1Ids.size > 0) {
    report.push(`\n文件1独有证件号码（前10个）:`)
    Array.from(onlyIn1Ids).slice(0, 10).forEach(id => {
      const rows = index1.get(id)
      const name = rows[0]['姓名'] || ''
      report.push(`  ${id} - ${name}`)
    })
  }
  
  if (onlyIn2Ids.size > 0) {
    report.push(`\n文件2独有证件号码（前10个）:`)
    Array.from(onlyIn2Ids).slice(0, 10).forEach(id => {
      const rows = index2.get(id)
      const name = rows[0]['姓名'] || ''
      report.push(`  ${id} - ${name}`)
    })
  }
  
  fs.writeFileSync(reportPath, report.join('\n'), 'utf8')
  console.log(`\n详细报告已保存到: ${reportPath}`)
  
  console.log(`\n========== 对比完成 ==========\n`)
}

// 如果直接运行此脚本
if (require.main === module) {
  const args = process.argv.slice(2)
  
  if (args.length < 2) {
    console.log('用法: node compare-results.js <文件1路径> <文件2路径>')
    console.log('示例: node compare-results.js "data/results/result.xlsx" "D:/workprogram/扫黄/扫黄/代码/antiporn/results/result.xlsx"')
    process.exit(1)
  }
  
  const file1Path = path.resolve(args[0])
  const file2Path = path.resolve(args[1])
  
  if (!fs.existsSync(file1Path)) {
    console.error(`文件1不存在: ${file1Path}`)
    process.exit(1)
  }
  
  if (!fs.existsSync(file2Path)) {
    console.error(`文件2不存在: ${file2Path}`)
    process.exit(1)
  }
  
  compareResults(file1Path, file2Path)
}

module.exports = { compareResults }