/**
 * 结果相关接口
 */
import { get } from './api'

/**
 * 获取结果文件列表
 * @returns {Promise<{files: Array, count: number, resultDir: string}>}
 */
export const getResults = async () => {
  return get('/api/results')
}

/**
 * 下载结果文件
 * @param {string} filename - 文件名
 * @returns {Promise<Blob>}
 */
export const downloadResultFile = async (filename) => {
  return get(`/files/${filename}`)
}