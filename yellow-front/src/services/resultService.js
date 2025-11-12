/**
 * 结果相关接口
 */
import { get, del } from './api'

/**
 * 获取结果记录列表（文件夹列表）
 * @returns {Promise<{records: Array, count: number}>}
 */
export const getRecords = async () => {
  return get('/api/records')
}

/**
 * 获取指定记录的完整数据（从 MongoDB）
 * @param {string} recordId - 记录ID（文件夹名）
 * @returns {Promise<{recordId, createdAt, updatedAt, statistics, files, tables}>}
 */
export const getRecordData = async (recordId) => {
  return get(`/api/records/${encodeURIComponent(recordId)}/data`)
}

/**
 * 从 MongoDB 导出 Excel 文件
 * @param {string} recordId - 记录ID
 * @param {string} tableKey - 表格类型（result, merge, transactions, abnormal_accounts, shopping, risk_hotspot, risk_population, risk_shopping）
 * @returns {Promise<Blob>}
 */
export const exportRecordFile = async (recordId, tableKey) => {
  return get(`/api/export/${encodeURIComponent(recordId)}/${tableKey}`)
}

/**
 * 删除记录
 * @param {string} recordId - 记录ID（文件夹名）
 * @returns {Promise<{status: string, message: string}>}
 */
export const deleteRecord = async (recordId) => {
  return del(`/api/records/${encodeURIComponent(recordId)}`)
}