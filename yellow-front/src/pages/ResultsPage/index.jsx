import { useState, useEffect, useCallback } from 'react';
import { Button, Card, Table, Tag, Input, Space,
     Select, message, Spin, Modal, Statistic, Row, Col, Tabs } from 'antd';
import { SearchOutlined, SaveOutlined, WarningOutlined } from '@ant-design/icons';
import { useParams } from 'react-router-dom';
import { downloadResultFile } from '../../services/resultService';
import { readExcelFile } from '../../utils/excel';
import './index.less';
const { Search } = Input;
const { TabPane } = Tabs;
// 前科记录表格列定义
const criminalColumns = [
    {
        title: '序号',
        dataIndex: 'index',
        key: 'index',
        width: 80,
    },
    {
        title: '案发地点',
        dataIndex: '案发地点',
        key: '案发地点',
        ellipsis: true,
    },
    {
        title: '地点分类',
        dataIndex: '地点分类',
        key: '地点分类',
        width: 120,
        render: (text) => {
            const typeMap = {
                '小区': { color: 'blue' },
                '宾馆': { color: 'orange' },
                '场所': { color: 'red' },
                '商务楼': { color: 'green' },
                '其他': { color: 'default' },
            };
            const config = typeMap[text] || typeMap['其他'];
            return <Tag color={config.color}>{text || '其他'}</Tag>;
        },
    },
    {
        title: '简要案情',
        dataIndex: '简要案情',
        key: '简要案情',
        width: 400,
        render: (text) => (
            <div style={{ 
                whiteSpace: 'pre-wrap', 
                wordBreak: 'break-word',
                maxHeight: '200px',
                overflowY: 'auto'
            }}>
                {text || '-'}
            </div>
        ),
    },
];

const ResultsPage = () => {
    const { id } = useParams(); // 从 URL 获取文件名
    const [data, setData] = useState([]); // 分析结果数据
    const [filteredData, setFilteredData] = useState([]); // 过滤后的数据
    const [loading, setLoading] = useState(false);
    const [criminalModal, setCriminalModal] = useState({
        visible: false,
        data: [],
        person: null,
    }); // 前科记录弹窗状态
    const [searchText, setSearchText] = useState('');
    const [riskFilter, setRiskFilter] = useState('all'); // 风险级别筛选

     // 用于显示各种详情信息的弹窗（入住、同住、资金、商品、收货地址、居住详情）
    const [detailModal, setDetailModal] = useState({
        visible: false,
        title: '',
        content: '',
    });

    // 用于显示高/中/低风险的统计数量
    const [riskStats, setRiskStats] = useState({
        high: 0,
        medium: 0,
        low: 0,
    });

    // 用于存储高风险地点统计数据（案发地点、实口地址、外卖地址）
    const [highRiskData, setHighRiskData] = useState({
        caseLocations: [],      // 案发地点统计
        populationAddresses: [], // 实口地址统计
        shoppingAddresses: [],   // 外卖地址统计
    });
    const [highRiskLoading, setHighRiskLoading] = useState(false);
    const [showHighRiskTab, setShowHighRiskTab] = useState(false); // 是否显示高风险地点标签页
    const [activeTabKey, setActiveTabKey] = useState('result'); // 当前激活的标签页

    // 使用 useCallback 稳定 loadResultData 的引用，避免 useEffect 报 missing dependency
    const loadResultData = useCallback(async () => {
        setLoading(true);
        try {
            const filename = decodeURIComponent(id);
            const blob = await downloadResultFile(filename);
            const excelData = await readExcelFile(blob);
            
            // 处理数据，添加 key 和索引
            const processedData = excelData.map((row, index) => ({
                ...row,
                key: row['证件号码'] || `row-${index}`,
                index: index + 1,
            }));
            
            setData(processedData);
            message.success('数据加载成功');
        } catch (error) {
            console.error('加载数据失败:', error);
            message.error('加载数据失败: ' + (error?.message || String(error)));
        } finally {
            setLoading(false);
        }
    }, [id]);

     // 这个函数会尝试加载三个统计文件：高风险地点统计.xlsx、实口地址高风险统计.xlsx、外卖收货地址高风险统计.xlsx
    const loadHighRiskData = useCallback(async () => {
        setHighRiskLoading(true);
        try {
            // 并行加载三个统计文件
            const [caseBlob, populationBlob, shoppingBlob] = await Promise.allSettled([
                downloadResultFile('高风险地点统计.xlsx'),
                downloadResultFile('实口地址高风险统计.xlsx'),
                downloadResultFile('外卖收货地址高风险统计.xlsx'),
            ]);

        let hasData = false;  // 添加标志变量
        const newHighRiskData = {
            caseLocations: [],
            populationAddresses: [],
            shoppingAddresses: [],
        };

        // 处理案发地点统计
        if (caseBlob.status === 'fulfilled') {
            try {
                const caseData = await readExcelFile(caseBlob.value);
                newHighRiskData.caseLocations = caseData.map((row, index) => ({
                    ...row,
                    key: `case-${index}`,
                    index: index + 1,
                }));
                if (newHighRiskData.caseLocations.length > 0) {
                    hasData = true;
                }
            } catch (e) {
                console.warn('案发地点统计数据格式错误:', e);
            }
        }
 
        // 处理实口地址统计
        if (populationBlob.status === 'fulfilled') {
            try {
                const popData = await readExcelFile(populationBlob.value);
                newHighRiskData.populationAddresses = popData.map((row, index) => ({
                    ...row,
                    key: `pop-${index}`,
                    index: index + 1,
                }));
                if (newHighRiskData.populationAddresses.length > 0) {
                    hasData = true;
                }
            } catch (e) {
                console.warn('实口地址统计数据格式错误:', e);
            }
        }

        // 处理外卖地址统计
        if (shoppingBlob.status === 'fulfilled') {
            try {
                const shopData = await readExcelFile(shoppingBlob.value);
                newHighRiskData.shoppingAddresses = shopData.map((row, index) => ({
                    ...row,
                    key: `shop-${index}`,
                    index: index + 1,
                }));
                if (newHighRiskData.shoppingAddresses.length > 0) {
                    hasData = true;
                }
            } catch (e) {
                console.warn('外卖地址统计数据格式错误:', e);
            }
        }

        // 统一设置状态
        setHighRiskData(newHighRiskData);

           // 如果有数据，显示标签页
        if (hasData) {
            setShowHighRiskTab(true);
        }
        } catch (error) {
            console.error('加载高风险地点统计失败:', error);
            // 不显示错误提示，因为统计文件可能不存在
        } finally {
            setHighRiskLoading(false);
        }
    }, []);

    useEffect(() => {
        if (id) {
            loadResultData();
            // 同时加载高风险地点统计
            loadHighRiskData();
        }
    }, [id, loadResultData,loadHighRiskData]);

    // 当数据加载完成后，自动计算高/中/低风险的数量
    useEffect(() => {
        if (data.length > 0) {
            const stats = {
                high: data.filter(row => row['预警状态'] === '高').length,
                medium: data.filter(row => row['预警状态'] === '中').length,
                low: data.filter(row => row['预警状态'] === '低').length,
            };
            setRiskStats(stats);
        }
    }, [data]);

    // 当筛选条件变化时，更新过滤后的数据
    useEffect(() => {
        applyFilters();
    }, [data, searchText, riskFilter]);

    // 应用筛选条件
    const applyFilters = () => {
        let filtered = [...data];

        // 搜索筛选（姓名、证件号码、手机号码）
        if (searchText) {
            const searchLower = searchText.toLowerCase();
            filtered = filtered.filter(row => {
                const name = String(row['姓名'] || '').toLowerCase();
                const idNumber = String(row['证件号码'] || '').toLowerCase();
                const phone = String(row['手机号码'] || '').toLowerCase();
                return name.includes(searchLower) || 
                       idNumber.includes(searchLower) || 
                       phone.includes(searchLower);
            });
        }

        // 风险级别筛选
        if (riskFilter !== 'all') {
            filtered = filtered.filter(row => {
                const alertLevel = String(row['预警状态'] || '').trim();
                return alertLevel === riskFilter;
            });
        }

        setFilteredData(filtered);
    };

    // 用于显示各种详情信息（入住、同住、资金、商品、收货地址、居住详情）
    const showDetailModal = (title, content) => {
        // 确保内容是字符串，如果为空则显示默认文本
        const safeContent = content ? String(content) : '暂无相关信息';
        setDetailModal({
            visible: true,
            title,
            content: safeContent,
        });
    };

    // 关闭详情弹窗
        const closeDetailModal = () => {
        setDetailModal({
            visible: false,
            title: '',
            content: '',
        });
    };

    // TODO: 需要后端提供保存接口 /api/save_result
    // 当前只是收集数据，实际保存需要调用 API
    const handleSave = async () => {
        try {
            // 收集可编辑字段的数据
            const saveData = filteredData.map(row => ({
                姓名: row['姓名'],
                证件号码: row['证件号码'],
                手机号码: row['手机号码'],
                预警状态: row['预警状态'],
                更新时间: row['更新时间'],
                户籍地址: row['户籍地址'],
                居住地址: row['居住地址'],
                所属辖区: row['所属辖区'],
                从业单位: row['从业单位'],
                社保情况: row['社保情况'],
            }));

            console.log('要保存的数据:', saveData);
            
            // TODO: 取消注释下面的代码，并确保后端有对应的接口
            // const response = await fetch('/api/save_result', {
            //     method: 'POST',
            //     headers: { 'Content-Type': 'application/json' },
            //     body: JSON.stringify(saveData),
            // });
            // const result = await response.json();
            // if (result.status === 'success') {
            //     message.success('保存成功！');
            // } else {
            //     message.error('保存失败: ' + (result.error || '未知错误'));
            // }
            
            message.success('保存功能待实现（需要后端接口支持）');
        } catch (error) {
            console.error('保存失败:', error);
            message.error('保存失败: ' + error.message);
        }
    };


    // 显示前科详情弹窗
    const showCriminalModal = (record) => {
        // 从当前数据中筛选出该人的前科记录
        // 如果有简要案情，说明有前科
        const criminalRecords = [];
        if (record['简要案情'] || record['案发地点']) {
            criminalRecords.push({
                key: '1',
                index: 1,
                案发地点: record['案发地点'] || '',
                地点分类: record['地点分类'] || '',
                简要案情: record['简要案情'] || '',
            });
        }

        // 如果有多条记录（同一人有多个前科），需要从原始数据中查找
        const allRecords = data.filter(row => 
            String(row['证件号码'] || '').trim() === String(record['证件号码'] || '').trim()
        );
        
        let records = criminalRecords;
        if (allRecords.length > 1) {
            records = allRecords.map((row, idx) => ({
                key: String(idx + 1),
                index: idx + 1,
                案发地点: row['案发地点'] || '',
                地点分类: row['地点分类'] || '',
                简要案情: row['简要案情'] || '',
            }));
        }

        setCriminalModal({
            visible: true,
            data: records,
            person: record,
        });
    };

    // 关闭前科记录弹窗
    const closeCriminalModal = () => {
        setCriminalModal({
            visible: false,
            data: [],
            person: null,
        });
    };

    // 渲染风险级别标签
    const renderRiskLevel = (level) => {
        const levelMap = {
            '高': { color: '#f50' },
            '中': { color: '#faad14' },
            '低': { color: '#87d068' },
        };
        const config = levelMap[level] || levelMap['低'];
        return <Tag color={config.color}>{level || '低'}</Tag>;
    };

    // 表格列定义 - 按用户要求的顺序排列
    const columns = [
        {
            title: '序号',
            dataIndex: 'index',
            key: 'index',
            width: 60,
            fixed: 'left',
        },
        {
            title: '姓名',
            dataIndex: '姓名',
            key: '姓名',
            width: 90,
            fixed: 'left',
        },
        {
            title: '证件号码',
            dataIndex: '证件号码',
            key: '证件号码',
            width: 160,
        },
        {
            title: '手机号码',
            dataIndex: '手机号码',
            key: '手机号码',
            width: 110,
        },
        {
            title: '前科次数',
            dataIndex: '前科次数',
            key: '前科次数',
            width: 75,
            sorter: (a, b) => (a['前科次数'] || 0) - (b['前科次数'] || 0),
            render: (count, record) => {
                const num = parseInt(count || 0, 10);
                return num > 0 ? (
                    <Button 
                        type="link" 
                        onClick={() => showCriminalModal(record)}
                        style={{ padding: 0 }}
                    >
                        {num}
                    </Button>
                ) : (
                    <span>0</span>
                );
            },
        },
        {
            title: '前科案发分类',
            dataIndex: '案由',
            key: '案由',
            width: 95,
            render: (text) => {
                if (!text) return '-';
                // 将逗号、顿号、分号等分隔符拆分，让每个分类单独一行显示
                const categories = String(text)
                    .split(/[,，、;；]/)
                    .map(cat => cat.trim())
                    .filter(cat => cat);
                
                if (categories.length === 0) return '-';
                
                return (
                    <div style={{ 
                        lineHeight: '1.6',
                        wordBreak: 'break-word'
                    }}>
                        {categories.map((cat, index) => (
                            <div key={index} style={{ 
                                marginBottom: index < categories.length - 1 ? '4px' : 0
                            }}>
                                {cat}
                            </div>
                        ))}
                    </div>
                );
            },
        },
        {
            title: '前科案发地点',
            dataIndex: '案发地点',
            key: '案发地点',
            width: 120,
        },
        {
            title: '户籍地址',
            dataIndex: '户籍地址',
            key: '户籍地址',
            width: 120,
        },
        {
            title: '居住地址',
            dataIndex: '居住地址',
            key: '居住地址',
            width: 120,
        },
        {
            title: '居住情况',
            dataIndex: '居住情况',
            key: '居住情况',
            width: 80,
            render: (text, record) => {
                // 如果是"同住人"且有居住详细，点击显示详情
                if (text === '同住人' && record['居住详细']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal('居住详细信息', record['居住详细'])}
                            style={{ padding: 0 }}
                        >
                            <Tag color="orange">同住人</Tag>
                        </Button>
                    );
                }
                return <Tag color="blue">独居</Tag>;
            },
        },
        {
            title: '所属辖区',
            dataIndex: '所属辖区',
            key: '所属辖区',
            width: 90,
        },
        {
            title: '从业单位',
            dataIndex: '从业单位',
            key: '从业单位',
            width: 100,
        },
        {
            title: '社保情况',
            dataIndex: '社保情况',
            key: '社保情况',
            width: 90,
        },
        {
            title: '异常购物',
            dataIndex: '异常购物_等级',
            key: '异常购物_等级',
            width: 75,
            render: (value, record) => {
                // 如果异常购物为"是"且有商品名称详细，点击显示详情
                if (value === '是' && record['商品名称详细']) {
                    return ( 
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal('商品详细信息', record['商品名称详细'])}
                            style={{ padding: 0, color: '#f50' }}
                        >
                            <Tag color="red">是</Tag>
                        </Button>
                    );
                }
                return <Tag color="green">否</Tag>;
            },
        },
        {
            title: '收货地址',
            dataIndex: '收货地址分类',
            key: '收货地址分类',
            width: 95,
            render: (text, record) => {
                // 如果有收货地址分类且有收货地址详细，点击显示详情
                if (text && record['收货地址详细']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal('收货详细信息', record['收货地址详细'])}
                            style={{ padding: 0 }}
                        >
                            {text}
                        </Button>
                    );
                }
                return text || '-';
            },
        },
        {
            title: '异常资金',
            dataIndex: '异常资金',
            key: '异常资金',
            width: 75,
            render: (value, record) => {
                const num = parseInt(value || 0, 10);
                // 如果异常资金为1且有资金备注，点击显示详情
                if (num === 1 && record['资金备注']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal('资金详细信息', record['资金备注'])}
                            style={{ padding: 0, color: '#f50' }}
                        >
                            <Tag color="red">是</Tag>
                        </Button>
                    );
                }
                return <Tag color="green">否</Tag>;
            },
        },
        {
            title: '入住次数',
            dataIndex: '入住次数',
            key: '入住次数',
            width: 70,
            sorter: (a, b) => (a['入住次数'] || 0) - (b['入住次数'] || 0),
            render: (value, record) => {
                const count = parseInt(value || 0, 10);
                // 如果入住次数大于0且有入住信息，点击显示详情
                if (count > 0 && record['入住信息']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal('入住详细信息', record['入住信息'])}
                            style={{ padding: 0 }}
                        >
                            {count}
                        </Button>
                    );
                }
                return <span>{count}</span>;
            },
        },
        {
            title: '同住男人数',
            dataIndex: '同住男人数',
            key: '同住男人数',
            width: 85,
            sorter: (a, b) => (a['同住男人数'] || 0) - (b['同住男人数'] || 0),
            render: (value, record) => {
                const count = parseInt(value || 0, 10);
                // 如果同住男人数大于0且有同住信息，点击显示详情
                if (count > 0 && record['同住信息']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal('同住详细信息', record['同住信息'])}
                            style={{ padding: 0 }}
                        >
                            {count}
                        </Button>
                    );
                }
                return <span>{count}</span>;
            }, 
        },
        {
            title: '预警状态',
            dataIndex: '预警状态',
            key: '预警状态',
            width: 80,
            filters: [
                { text: '高', value: '高' },
                { text: '中', value: '中' },
                { text: '低', value: '低' },
            ],
            onFilter: (value, record) => record['预警状态'] === value,
            render: renderRiskLevel,
        },
        {
            title: '更新时间',
            dataIndex: '更新时间',
            key: '更新时间',
            width: 90,
        },
    ];

    // 案发地点统计表格列
    const caseLocationColumns = [
        {
            title: '序号',
            dataIndex: 'index',
            key: 'index',
            width: 80,
        },
        {
            title: '案发地点',
            dataIndex: '案发地点',
            key: '案发地点',
            ellipsis: true,
        },
        {
            title: '出现次数',
            dataIndex: '出现次数',
            key: '出现次数',
            width: 120,
            sorter: (a, b) => (a['出现次数'] || 0) - (b['出现次数'] || 0),
            render: (value, record) => {
                // 如果有点击次数且有姓名证件列表，点击显示详情
                if (value > 0 && record['姓名证件列表']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal(
                                `${record['案发地点']} - 案发地点人员名单`,
                                record['姓名证件列表']
                            )}
                            style={{ padding: 0 }}
                        >
                            {value}
                        </Button>
                    );
                }
                return value;
            },
        },
    ];

    // 实口地址统计表格列
    const populationAddressColumns = [
        {
            title: '序号',
            dataIndex: 'index',
            key: 'index',
            width: 80,
        },
        {
            title: '实口居住地址',
            dataIndex: '居住地址',
            key: '居住地址',
            ellipsis: true,
        },
        {
            title: '出现次数',
            dataIndex: '出现次数',
            key: '出现次数',
            width: 120,
            sorter: (a, b) => (a['出现次数'] || 0) - (b['出现次数'] || 0),
            render: (value, record) => {
                if (value > 0 && record['姓名证件列表']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal(
                                `${record['居住地址']} - 实口居住地址人员名单`,
                                record['姓名证件列表']
                            )}
                            style={{ padding: 0 }}
                        >
                            {value}
                        </Button>
                    );
                }
                return value;
            },
        },
        {
            title: '是否高风险',
            dataIndex: '实口所属派出所',
            key: '实口所属派出所',
            width: 120,
        },
    ];

     // 外卖地址统计表格列
    const shoppingAddressColumns = [
        {
            title: '序号',
            dataIndex: 'index',
            key: 'index',
            width: 80,
        },
        {
            title: '外卖收货地址',
            dataIndex: '外卖收货地址',
            key: '外卖收货地址',
            ellipsis: true,
        },
        {
            title: '出现次数',
            dataIndex: '出现次数',
            key: '出现次数',
            width: 120,
            sorter: (a, b) => (a['出现次数'] || 0) - (b['出现次数'] || 0),
            render: (value, record) => {
                if (value > 0 && record['姓名证件列表']) {
                    return (
                        <Button 
                            type="link" 
                            onClick={() => showDetailModal(
                                `${record['外卖收货地址']} - 外卖收货地址人员名单`,
                                record['姓名证件列表']
                            )}
                            style={{ padding: 0 }}
                        >
                            {value}
                        </Button>
                    );
                }
                return value;
            },
        },
    ];

    // 主渲染函数
    return (
        <div className='results-page'>
            <Card 
                title={`分析结果 - ${id ? decodeURIComponent(id) : 'result.xlsx'}`}
                extra={
                    <Space size="middle" style={{ display: 'flex', alignItems: 'center' }}>
                        {/* 风险统计 - 移到搜索栏同一行 */}
                        <Space size="small">
                            <Statistic 
                                title="高风险" 
                                value={riskStats.high} 
                                valueStyle={{ color: '#f50', fontSize: '18px' }}
                                style={{ marginRight: 0 }}
                            />
                            <Statistic 
                                title="中风险" 
                                value={riskStats.medium} 
                                valueStyle={{ color: '#faad14', fontSize: '18px' }}
                                style={{ marginRight: 0 }}
                            />
                            <Statistic 
                                title="低风险" 
                                value={riskStats.low} 
                                valueStyle={{ color: '#87d068', fontSize: '18px' }}
                                style={{ marginRight: 0 }}
                            />
                        </Space>
                        {/* 添加查看高风险地点按钮 */}
                        {showHighRiskTab && (
                            <Button 
                                icon={<WarningOutlined />}
                                onClick={() => setActiveTabKey('highRisk')}
                                size="small"
                            >
                                查看高风险地点
                            </Button>
                        )}
                        <Select
                            value={riskFilter}
                            onChange={setRiskFilter}
                            style={{ width: 120 }}
                            size="small"
                        >
                            <Select.Option value="all">全部风险</Select.Option>
                            <Select.Option value="高">高风险</Select.Option>
                            <Select.Option value="中">中风险</Select.Option>
                            <Select.Option value="低">低风险</Select.Option>
                        </Select>
                        <Search
                            placeholder="搜索姓名/证件号/手机号"
                            allowClear
                            style={{ width: 250 }}
                            onSearch={setSearchText}
                            onChange={(e) => setSearchText(e.target.value)}
                            enterButton={<SearchOutlined />}
                            size="small"
                        />
                    </Space>
                }
            >

                {/* 添加标签页切换 - 压缩空间 */}
                <Tabs 
                    defaultActiveKey="result"
                    activeKey={activeTabKey}
                    onChange={(key) => {
                        setActiveTabKey(key);
                    }}
                    style={{ marginTop: 0 }}
                >
                    <TabPane tab="分析结果" key="result">
                        <Spin spinning={loading}>
                            <Table 
                                columns={columns}
                                dataSource={filteredData}
                                scroll={{ x: 1600, y: 'calc(100vh - 280px)' }}
                                pagination={{
                                    pageSize: 20,
                                    showSizeChanger: true,
                                    showTotal: (total) => `共 ${total} 条记录`,
                                    pageSizeOptions: ['10', '20', '50', '100'],
                                    size: 'small',
                                }}
                                size="small"
                            />
                        </Spin>
                    </TabPane>
                    
                    {/* 修改点10：添加高风险地点统计标签页 */}
                    {showHighRiskTab && (
                        <TabPane tab="高风险地点统计" key="highRisk">
                            <Tabs defaultActiveKey="case">
                                <TabPane tab="案发地点统计" key="case">
                                    <Spin spinning={highRiskLoading}>
                                        <Table 
                                            columns={caseLocationColumns}
                                            dataSource={highRiskData.caseLocations}
                                            pagination={{
                                                pageSize: 20,
                                                showSizeChanger: true,
                                                showTotal: (total) => `共 ${total} 条记录`,
                                            }}
                                        />
                                    </Spin>
                                </TabPane>
                                <TabPane tab="实口地址统计" key="population">
                                    <Spin spinning={highRiskLoading}>
                                        <Table 
                                            columns={populationAddressColumns}
                                            dataSource={highRiskData.populationAddresses}
                                            pagination={{
                                                pageSize: 20,
                                                showSizeChanger: true,
                                                showTotal: (total) => `共 ${total} 条记录`,
                                            }}
                                        />
                                    </Spin>
                                </TabPane>
                                <TabPane tab="外卖地址统计" key="shopping">
                                    <Spin spinning={highRiskLoading}>
                                        <Table 
                                            columns={shoppingAddressColumns}
                                            dataSource={highRiskData.shoppingAddresses}
                                            pagination={{
                                                pageSize: 20,
                                                showSizeChanger: true,
                                                showTotal: (total) => `共 ${total} 条记录`,
                                            }}
                                        />
                                    </Spin>
                                </TabPane>
                            </Tabs>
                        </TabPane>
                    )}
                </Tabs>

                {/* 修改点11：添加保存按钮 */}
                <div style={{ 
                    position: 'fixed', 
                    bottom: 20, 
                    left: '50%', 
                    transform: 'translateX(-50%)', 
                    zIndex: 1000 
                }}>
                    <Button 
                        type="primary" 
                        icon={<SaveOutlined />}
                        onClick={handleSave}
                    >
                        保存编辑
                    </Button>
                </div>

                {/* 修改点12：添加详情弹窗 */}
                <Modal
                    title={detailModal.title}
                    open={detailModal.visible}
                    onCancel={closeDetailModal}
                    footer={[
                        <Button key="close" onClick={closeDetailModal}>关闭</Button>
                    ]}
                    width={800}
                >
                    <div 
                        style={{ 
                            whiteSpace: 'pre-wrap', 
                            maxHeight: '60vh', 
                            overflowY: 'auto',
                            lineHeight: 1.5,
                            padding: '10px'
                        }}
                        dangerouslySetInnerHTML={{ __html: detailModal.content }}
                    />
                </Modal>

                {/* 前科记录弹窗 - 改为全屏Modal，类似异常购物和异常资金 */}
                <Modal
                    title={
                        criminalModal.person 
                            ? `前科记录 - ${criminalModal.person['姓名']} (${criminalModal.person['证件号码']})`
                            : '前科记录'
                    }
                    open={criminalModal.visible}
                    onCancel={closeCriminalModal}
                    footer={[
                        <Button key="close" onClick={closeCriminalModal}>关闭</Button>
                    ]}
                    width={1200}
                    style={{ top: 20 }}
                >
                    {criminalModal.data.length > 0 ? (
                        <div>
                            <Table 
                                columns={criminalColumns} 
                                dataSource={criminalModal.data}
                                pagination={false}
                                scroll={{ x: 'max-content' }}
                            />
                            
                            {criminalModal.person && (
                                <div style={{ marginTop: 24, padding: 16, background: '#f5f5f5', borderRadius: 4 }}>
                                    <h4>人员信息</h4>
                                    <p><strong>姓名：</strong>{criminalModal.person['姓名']}</p>
                                    <p><strong>证件号码：</strong>{criminalModal.person['证件号码']}</p>
                                    <p><strong>手机号码：</strong>{criminalModal.person['手机号码']}</p>
                                    <p><strong>居住地址：</strong>{criminalModal.person['居住地址']}</p>
                                    <p><strong>前科次数：</strong>{criminalModal.person['前科次数'] || 0}</p>
                                    <p><strong>预警状态：</strong>{renderRiskLevel(criminalModal.person['预警状态'])}</p>
                                </div>
                            )}
                        </div>
                    ) : (
                        <div style={{ textAlign: 'center', padding: '40px 0' }}>
                            暂无前科记录
                        </div>
                    )}
                </Modal>
            </Card>
        </div>
    );
};

export default ResultsPage;