const fs = require('fs')
const path = require('path')
const xlsx = require('xlsx')

// 读取 Excel 文件为数组
function readExcelToArray(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    return []
  }
  
  const wb = xlsx.readFile(filePath)
  const sheetName = wb.SheetNames[0]
  const ws = wb.Sheets[sheetName]
  
  // 转换为 JSON 数组，空值默认为空字符串
  return xlsx.utils.sheet_to_json(ws, { defval: '' })
}

// 将数组写入 Excel 文件
function writeArrayToExcel(rows, outPath, options = {}) {
  if (!rows || rows.length === 0) {
    rows = []
  }

  // 定义数字字段列表
  const numericColumns = ['异常资金', '入住次数', '同住男人数', '前科次数', '前科人员', '前科情况']
  const textColumns = ['证件号码']
  
  // 需要排除的字段（主表字段，不应该出现在某些导出文件中）
  const excludeFields = options.excludeFields || []
  
  // 处理数据，确保数字字段保持为数字类型；文本字段保持为字符串
  const processedRows = rows.map(row => {
    const processedRow = {}
    
    // 只保留需要的字段，排除不需要的字段
    for (const key in row) {
      if (!excludeFields.includes(key)) {
        processedRow[key] = row[key]
      }
    }

    // 先处理文本字段，确保不会被 Excel 自动转换
    for (const col of textColumns) {
      if (processedRow[col] !== undefined && processedRow[col] !== null) {
        const val = processedRow[col]
        if (val === '') {
          processedRow[col] = ''
        } else if (typeof val !== 'string') {
          processedRow[col] = String(val)
        }
      }
    }

    // 再处理数字字段（只处理存在的字段，不自动添加）
    for (const col of numericColumns) {
      if (processedRow[col] !== undefined && processedRow[col] !== null) {
        const val = processedRow[col]
        // 如果是字符串 '0' 或空字符串，转换为数字 0
        if (val === '' || val === '0' || val === 'NaN' || val === 'nan' || val === 'None') {
          processedRow[col] = 0
        } else if (typeof val === 'string') {
          // 尝试转换为数字
          const numVal = parseInt(val, 10)
          processedRow[col] = isNaN(numVal) ? 0 : numVal
        } else if (typeof val === 'number') {
          // 已经是数字，确保不是 NaN
          processedRow[col] = isNaN(val) ? 0 : val
        }
      }
      // 注意：不再自动添加不存在的字段
    }
    return processedRow
  })

  const ws = xlsx.utils.json_to_sheet(processedRows || [])
  
  const range = xlsx.utils.decode_range(ws['!ref'] || 'A1')

  // 找出文本列所在的列索引
  const textColumnIndices = new Map()
  for (let C = range.s.c; C <= range.e.c; C++) {
    const headerAddr = xlsx.utils.encode_cell({ r: range.s.r, c: C })
    const headerCell = ws[headerAddr]
    if (!headerCell) continue
    const headerValue = headerCell.v
    if (textColumns.includes(headerValue)) {
      textColumnIndices.set(headerValue, C)
    }
  }
  
  // 设置数字字段的单元格类型为数字
  for (let R = range.s.r + 1; R <= range.e.r; R++) {
    for (let C = range.s.c; C <= range.e.c; C++) {
      const cellAddress = xlsx.utils.encode_cell({ r: R, c: C })
      const cell = ws[cellAddress]
      if (!cell) continue
      
      const headerCell = ws[xlsx.utils.encode_cell({ r: range.s.r, c: C })]
      if (!headerCell) continue
      
      const headerValue = headerCell.v
      if (numericColumns.includes(headerValue)) {
        // 确保数字字段的单元格类型为数字
        if (cell.v === '' || cell.v === null || cell.v === undefined) {
          cell.v = 0
        }
        cell.t = 'n' // 设置为数字类型
      } else if (textColumns.includes(headerValue)) {
        // 文本列强制为字符串，避免 Excel 自动转成科学计数法
        if (cell.v === null || cell.v === undefined) {
          cell.v = ''
        } else if (typeof cell.v !== 'string') {
          cell.v = String(cell.v)
        }
        cell.t = 's'
        cell.z = '@'
      }
    }
  }
  
  const wb = xlsx.utils.book_new()
  xlsx.utils.book_append_sheet(wb, ws, 'Sheet1')
  
  // 确保目录存在
  const dir = path.dirname(outPath)
  fs.mkdirSync(dir, { recursive: true })
  
  xlsx.writeFile(wb, outPath)
}

module.exports = {
  readExcelToArray,
  writeArrayToExcel
}