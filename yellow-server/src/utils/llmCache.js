/**
 * LLM 缓存管理
 * 基于 JSON 文件的持久化缓存
 */

const fs = require('fs')
const path = require('path')

class LLMCacheManager {
  constructor(cacheFile = 'llm_cache.json', autoSave = true) {
    // 如果路径是相对路径，放在项目根目录下的 data 目录
    if (!path.isAbsolute(cacheFile)) {
      this.cacheFile = path.join(process.cwd(), 'data', cacheFile)
    } else {
      this.cacheFile = cacheFile
    }
    
    this.autoSave = autoSave
    this.cache = {}
    
    // 如果缓存文件存在，加载它
    if (fs.existsSync(this.cacheFile)) {
      try {
        const content = fs.readFileSync(this.cacheFile, 'utf-8')
        this.cache = JSON.parse(content)
        console.log(`[LLM Cache] 已加载缓存文件: ${this.cacheFile} (${Object.keys(this.cache).length} 条记录)`)
      } catch (error) {
        console.warn(`[LLM Cache] 读取缓存文件失败 ${this.cacheFile}:`, error.message)
        this.cache = {}
      }
    } else {
      // 确保目录存在
      const dir = path.dirname(this.cacheFile)
      if (dir && dir !== '.' && !fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true })
      }
    }
  }

  /**
   * 检查缓存中是否存在某个键
   */
  has(key) {
    return key in this.cache
  }

  /**
   * 获取缓存值
   */
  get(key) {
    return this.cache[key] || ''
  }

  /**
   * 设置缓存值
   */
  set(key, value) {
    this.cache[key] = value
    if (this.autoSave) {
      this.save()
    }
  }

  /**
   * 保存缓存到文件
   */
  save() {
    try {
      const dir = path.dirname(this.cacheFile)
      if (dir && dir !== '.' && !fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true })
      }
      fs.writeFileSync(this.cacheFile, JSON.stringify(this.cache, null, 2), 'utf-8')
    } catch (error) {
      console.error(`[LLM Cache] 保存缓存文件失败 ${this.cacheFile}:`, error.message)
    }
  }

  /**
   * 清空缓存
   */
  clear() {
    this.cache = {}
    if (this.autoSave) {
      this.save()
    }
  }
}

module.exports = LLMCacheManager