/**
 * 上传相关接口
 */
import { post, get } from './api'

/**
 * 上传文件
 * @param {FormData} formData - 文件表单数据
 * @returns {Promise<Object>}
 */
export const uploadFiles = async (formData) => {
  return post('/api/upload', formData)
}

/**
 * 查询处理进度
 * @returns {Promise<{percent: number, error: string}>}
 */
export const getProgress = async () => {
  return get('/api/progress')
}