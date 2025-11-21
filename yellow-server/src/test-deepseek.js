/**
 * 通义千问 API 测试脚本
 * 用于测试通义千问 API 是否能正常调用
 */

require('dotenv').config()
const { askDeepSeek } = require('./utils/llmClient')

// 测试配置信息
function printConfig() {
  console.log('\n========== 通义千问 API 配置信息 ==========')
  console.log('API URL:', process.env.QWEN_BASE_URL || 'http://38.160.65.151:3000/v1/chat/completions')
  console.log('模型:', process.env.QWEN_MODEL || 'Qwen')
  
  // 检查环境变量中的 API Key
  const envKey = process.env.QWEN_API_KEY || process.env.DASHSCOPE_API_KEY
  if (envKey) {
    console.log('API Key (环境变量):', `${envKey.substring(0, 10)}...`)
  } else {
    console.log('API Key (环境变量): 未设置，将使用代码中的默认值')
  }
  
  console.log('超时时间:', process.env.QWEN_TIMEOUT || '300', '秒')
  console.log('温度:', process.env.QWEN_TEMPERATURE || '0.3')
  console.log('最大 Token:', process.env.QWEN_MAX_TOKENS || '32768')
  console.log('==========================================\n')
}

// 测试用例
const testCases = [
  {
    name: '简单问候测试',
    prompt: '你好，请简单介绍一下你自己。',
    description: '测试基本的对话功能'
  },
  {
    name: '地址提取测试',
    prompt: `你是上海公安局的案件信息提取系统。
请严格从原文中提取内容返回地址本身，不要返回标点、时间、人名、行为描述等。

示例1：
text：2025年5月13日，经群众匿名举报，在上海市松江区文城路358弄-6号嘉禾商务中心11楼1103室内，有卖淫嫖娼活动。已接报
answer:文城路358弄-6号嘉禾商务中心11楼1103室内

如果未提取到、无法提取到有效地址，则返回：无地址

现在请处理以下说明：
2025年5月22日，根据警企协作获得线索，发现上海市松江区车墩镇张三等人卖淫嫖娼的违法行为，后续进行调查。`,
    description: '测试地址提取功能（实际业务场景）'
  },
  {
    name: '地址分类测试',
    prompt: `请对以下地址进行分类，只返回以下类别之一：小区、商务楼、场所、宾馆、其他

地址：松江公寓 (123)

只返回类别名称，不要返回其他内容。`,
    description: '测试地址分类功能（实际业务场景）'
  }
]

// 运行测试
async function runTest() {
  console.log('🚀 开始测试通义千问 API...\n')
  
  // 打印配置信息
  printConfig()
  
  // 运行每个测试用例
  for (let i = 0; i < testCases.length; i++) {
    const testCase = testCases[i]
    console.log(`\n📝 测试 ${i + 1}/${testCases.length}: ${testCase.name}`)
    console.log(`   描述: ${testCase.description}`)
    console.log(`   提示词: ${testCase.prompt.substring(0, 50)}...`)
    console.log('   ⏳ 正在调用 API...')
    
    const startTime = Date.now()
    
    try {
      const result = await askDeepSeek(testCase.prompt)
      const endTime = Date.now()
      const duration = ((endTime - startTime) / 1000).toFixed(2)
      
      if (result && result.trim() !== '') {
        console.log(`   ✅ 测试成功 (耗时: ${duration}秒)`)
        console.log(`   📄 响应内容:`)
        console.log(`   ${'-'.repeat(50)}`)
        console.log(`   ${result}`)
        console.log(`   ${'-'.repeat(50)}`)
      } else {
        console.log(`   ⚠️  测试返回空结果 (耗时: ${duration}秒)`)
        console.log(`   ⚠️  可能的原因:`)
        console.log(`   - API 调用失败但未抛出错误`)
        console.log(`   - 响应内容为空`)
        console.log(`   - 思考标签处理失败`)
      }
    } catch (error) {
      const endTime = Date.now()
      const duration = ((endTime - startTime) / 1000).toFixed(2)
      
      console.log(`   ❌ 测试失败 (耗时: ${duration}秒)`)
      console.log(`   ❌ 错误信息:`, error.message)
      
      if (error.response) {
        console.log(`   ❌ HTTP 状态码:`, error.response.status)
        console.log(`   ❌ 响应数据:`, JSON.stringify(error.response.data, null, 2))
      } else if (error.request) {
        console.log(`   ❌ 请求未收到响应`)
        console.log(`   ❌ 请检查:`)
        console.log(`   - 网络连接是否正常`)
        console.log(`   - API URL 是否正确`)
        console.log(`   - 防火墙是否阻止了请求`)
      }
    }
    
    // 测试间隔，避免请求过快
    if (i < testCases.length - 1) {
      console.log('\n   ⏸️  等待 2 秒后继续下一个测试...')
      await new Promise(resolve => setTimeout(resolve, 2000))
    }
  }
  
  console.log('\n' + '='.repeat(50))
  console.log('✨ 测试完成！')
  console.log('='.repeat(50) + '\n')
}

// 运行测试
runTest().catch(error => {
  console.error('\n❌ 测试运行失败:', error)
  process.exit(1)
})