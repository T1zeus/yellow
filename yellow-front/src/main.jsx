import React from 'react'
import ReactDOM from 'react-dom'
import { BrowserRouter } from 'react-router-dom';

import 'antd/dist/antd.css'
import './index.css'
import './styles/index.less'
import App from './App.jsx'

// 使用 React 17 兼容的渲染方式（Chrome 77 支持）
const rootElement = document.getElementById('root')

if (rootElement) {
  ReactDOM.render(
    <BrowserRouter>
      <React.StrictMode>
        <App />
      </React.StrictMode>
    </BrowserRouter>,
    rootElement
  )
}
