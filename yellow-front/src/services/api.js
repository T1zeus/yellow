/**
 * 基础 API 工具
 * 封装所有 HTTP 请求的通用逻辑
 */

// 后端服务器地址
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

/**
 * 通用请求方法
 * @param {string} url - 请求地址
 * @param {Object} options - fetch 配置选项
 * @returns {Promise<Response>}
 */
const request = async (url, options = {}) => {
  const response = await fetch(`${API_BASE_URL}${url}`, {
    ...options,
    headers: {
      ...options.headers,
    },
  })

  if (!response.ok) {
    let errorMessage = '请求失败'
    try {
      const errorData = await response.json()
      errorMessage = errorData.error || errorMessage
    } catch {
      errorMessage = `HTTP ${response.status}: ${response.statusText}`
    }
    throw new Error(errorMessage)
  }

  // 根据响应类型返回不同格式
  const contentType = response.headers.get('content-type')
  if (contentType && contentType.includes('application/json')) {
    return response.json()
  } else if (contentType && contentType.includes('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')) {
    return response.blob()
  } else {
    return response.text()
  }
  
}

/**
 * GET 请求
 */
export const get = (url, options = {}) => {
  return request(url, {
    method: 'GET',
    ...options,
  })
}

/**
 * POST 请求
 */
export const post = (url, data, options = {}) => {
  return request(url, {
    method: 'POST',
    body: data instanceof FormData ? data : JSON.stringify(data),
    headers: {
      ...(data instanceof FormData ? {} : { 'Content-Type': 'application/json' }),
      ...options.headers,
    },
    ...options,
  })
}

/**
 * PUT 请求
 */
export const put = (url, data, options = {}) => {
  return request(url, {
    method: 'PUT',
    body: JSON.stringify(data),
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  })
}

/**
 * DELETE 请求
 */
export const del = (url, options = {}) => {
  return request(url, {
    method: 'DELETE',
    ...options,
  })
}

export default {
  get,
  post,
  put,
  delete: del,
}