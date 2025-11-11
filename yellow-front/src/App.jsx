import { useState } from 'react'
import {
  HistoryOutlined,
  FileOutlined,
} from '@ant-design/icons';
import { Layout, Menu, theme } from 'antd';
import { Routes, Route, Link } from 'react-router-dom';

import './styles/app.less';
import RecordsPage from './pages/RecordsPage';
import UploadPage from './pages/UploadPage';
import ResultsPage from './pages/ResultsPage';
import RecordDetailPage from './pages/RecordDetailPage';

const { Header, Content, Footer, Sider } = Layout;

function getItem(label, key, icon, children) {
  return {
    key,
    icon,
    children,
    label,
  };
}
const items = [
  getItem(<Link to='/'>历史分析记录</Link>, '1', <HistoryOutlined />),
  getItem(<Link to='/upload'>上传分析文件</Link>, '2', <FileOutlined />),
];

const App = () => {
  const [collapsed, setCollapsed] = useState(false);
  const {
    token: { colorBgContainer },
  } = theme.useToken();
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider collapsible collapsed={collapsed} onCollapse={value => setCollapsed(value)}>
        <div className="logo">扫黄分析系统</div>
        <Menu theme="dark" defaultSelectedKeys={['1']} mode="inline" items={items} />
      </Sider>
      <Layout>
        <Header style={{ padding: 0, background: colorBgContainer }} />

        <Content style={{ margin: '16px' }}>
          <Routes>
            <Route path='/' element={<RecordsPage />} />
            <Route path='/upload' element={<UploadPage />} />
            <Route path='/record/:id' element={<RecordDetailPage />} />
            <Route path='/result/:id' element={<ResultsPage />} />
          </Routes>
        </Content>

        <Footer style={{ textAlign: 'center' }}>
          上海谋乐网络科技 ©{new Date().getFullYear()}
        </Footer>
      </Layout>
    </Layout>
  );
};

export default App