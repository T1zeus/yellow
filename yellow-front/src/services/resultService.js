/**
 * 结果相关接口
 */
import { get } from './api'

/**
 * 获取结果记录列表（文件夹列表）
 * @returns {Promise<{records: Array, count: number}>}
 */
export const getRecords = async () => {
  return get('/api/records')
}

/**
 * 获取结果文件列表（保留兼容性，返回根目录下的文件）
 * @returns {Promise<{files: Array, count: number, resultDir: string}>}
 */
export const getResults = async () => {
  return get('/api/results')
}

/**
 * 下载结果文件（支持从指定记录文件夹下载）
 * @param {string} filename - 文件名
 * @param {string} recordId - 记录ID（文件夹名），可选
 * @returns {Promise<Blob>}
 */
export const downloadResultFile = async (filename, recordId = null) => {
  if (recordId) {
    return get(`/api/files/${recordId}/${filename}`)
  }
  return get(`/files/${filename}`)
}