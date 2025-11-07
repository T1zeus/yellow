/**
 * DeepSeek LLM API 客户端
 * 封装 DeepSeek API 调用
 */

const axios = require('axios')

const DEEPSEEK_BASE_URL = process.env.DEEPSEEK_BASE_URL || 'https://api.deepseek.com/v1/chat/completions'
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || 'deepseek-chat'
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY || 'sk-01dd6b0da46a4e6db47160a3c9c7fb62'
const DEEPSEEK_TIMEOUT = parseInt(process.env.DEEPSEEK_TIMEOUT || '300', 10)
const DEEPSEEK_TEMPERATURE = parseFloat(process.env.DEEPSEEK_TEMPERATURE || '0.3')
const DEEPSEEK_MAX_TOKENS = parseInt(process.env.DEEPSEEK_MAX_TOKENS || '2048', 10)

/**
 * 调用 DeepSeek API
 * @param {string} prompt - 提示词
 * @param {string} context - 上下文（可选）
 * @param {Object} options - 选项
 * @param {boolean} options.removeThinking - 是否移除思考标签（默认 true）
 * @returns {Promise<string>} API 响应内容
 */
async function askDeepSeek(prompt, context = '', options = {}) {
  const { removeThinking = true } = options

  try {
    const fullPrompt = context ? `${context}\n${prompt}` : prompt

    const payload = {
      model: DEEPSEEK_MODEL,
      messages: [{ role: 'system', content: fullPrompt }],
      temperature: DEEPSEEK_TEMPERATURE,
      stream: false,
      max_tokens: DEEPSEEK_MAX_TOKENS,
    }

    const headers = {
      'Content-Type': 'application/json',
    }

    if (DEEPSEEK_API_KEY) {
      headers['Authorization'] = `Bearer ${DEEPSEEK_API_KEY}`
    }

    // 使用 axios 发送请求
    const response = await axios.post(DEEPSEEK_BASE_URL, payload, {
      headers,
      timeout: DEEPSEEK_TIMEOUT * 1000, // axios 使用毫秒
    })

    let content = response.data.choices?.[0]?.message?.content || ''

    // 移除思考标签
    if (removeThinking && content.includes('`</think>`')) {
      content = content.split('`</think>`')[1] || content
    }

    return content.trim()
  } catch (error) {
    // 更详细的错误信息
    if (error.response) {
      // 服务器响应了错误状态码
      console.error('[DeepSeek ERROR] HTTP', error.response.status, ':', error.response.data)
    } else if (error.request) {
      // 请求已发送但没有收到响应
      console.error('[DeepSeek ERROR] 无响应:', error.message)
      console.error('[DeepSeek ERROR] 请求 URL:', DEEPSEEK_BASE_URL)
      console.error('[DeepSeek ERROR] 请检查网络连接和服务器地址')
    } else {
      // 请求配置错误
      console.error('[DeepSeek ERROR] 请求配置错误:', error.message)
    }
    return ''
  }
}

module.exports = {
  askDeepSeek
}