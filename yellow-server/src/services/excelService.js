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
function writeArrayToExcel(rows, outPath) {
  const ws = xlsx.utils.json_to_sheet(rows || [])
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