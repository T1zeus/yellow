import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import legacy from '@vitejs/plugin-legacy'
import selectorParser from 'postcss-selector-parser'
import autoprefixer from 'autoprefixer'
import postcssPresetEnv from 'postcss-preset-env'

const removeWherePseudo = () => {
  const processor = selectorParser(selectors => {
    selectors.walkPseudos(pseudo => {
      if (pseudo.value !== ':where') return
      const [firstSelector] = pseudo.nodes || []
      if (!firstSelector) {
        pseudo.remove()
        return
      }
      const clones = firstSelector.clone().nodes
      if (clones && clones.length) {
        pseudo.replaceWith(...clones)
      } else {
        pseudo.replaceWith(firstSelector.clone())
      }
    })
  })

  return {
    postcssPlugin: 'remove-where-pseudo',
    Once(root) {
      root.walkRules(rule => {
        if (!rule.selector || !rule.selector.includes(':where(')) return
        rule.selector = processor.processSync(rule.selector)
      })
    },
  }
}
removeWherePseudo.postcss = true

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    legacy({
      targets: [
        'Chrome >= 77',
        'Firefox >= 54',
        'Safari >= 10.1',
        'Edge >= 15',
      ],
      additionalLegacyPolyfills: [
        'regenerator-runtime/runtime',
      ],
      renderLegacyChunks: true,
      modernPolyfills: true,
      // 为 Chrome 77 添加必要的 polyfills
      polyfills: [
        'es.symbol',
        'es.array.filter',
        'es.promise',
        'es.promise.finally',
        'es/map',
        'es/set',
        'es.array.for-each',
        'es.object.define-properties',
        'es.object.define-property',
        'es.object.get-own-property-descriptor',
        'es.object.get-own-property-descriptors',
        'es.object.keys',
        'es.object.to-string',
        'web.dom-collections.for-each',
        'esnext.global-this',
        'esnext.string.match-all',
      ],
    }),
  ],
  // 生产环境使用绝对路径，确保资源路径正确
  base: '/',
  css: {
    postcss: {
      plugins: [
        // 先处理 CSS 变量和其他现代特性
        postcssPresetEnv({
          browsers: ['Chrome >= 77'],
          // Chrome 77 支持 CSS 变量，所以不需要降级
          features: {
            'custom-properties': false,
          },
        }),
        autoprefixer({
          overrideBrowserslist: ['Chrome >= 77'],
        }),
        removeWherePseudo(),
      ],
    },
    preprocessorOptions: {
      less: {
        javascriptEnabled: true,
      },
    },
  },
  build: {
    outDir: '../yellow/yellow-front/dist',
    assetsDir: 'assets',
    // 设置构建目标以确保兼容 Chrome 77
    target: 'chrome77',
    cssCodeSplit: true,
    // 确保 CSS 也被正确处理
    cssMinify: 'esbuild',
  },
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        secure: false,
      },
      '/files': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        secure: false,
      },
    },
  },
})