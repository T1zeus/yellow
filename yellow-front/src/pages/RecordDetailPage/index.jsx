import { useState, useEffect } from 'react';
import { Card, Table, Button, Space, Tag, message } from 'antd';
import { DownloadOutlined, EyeOutlined, ReloadOutlined, ArrowLeftOutlined } from '@ant-design/icons';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { getRecordData, exportRecordFile } from '../../services/resultService';
import { formatFileSize } from '../../utils/format';

const RecordDetailPage = () => {
    const { id } = useParams(); // 记录ID（文件夹名）
    const navigate = useNavigate();
    const [recordInfo, setRecordInfo] = useState(null);
    const [fileList, setFileList] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (id) {
            loadRecordDetail();
        }
    }, [id]);

    const loadRecordDetail = async () => {
        setLoading(true);
        try {
            const recordId = decodeURIComponent(id);
            
            // 从 MongoDB 获取记录数据
            const recordData = await getRecordData(recordId);
            
            if (!recordData) {
                message.error('记录不存在');
                navigate('/');
                return;
            }
            
            // 设置记录信息
            setRecordInfo({
                id: recordData.recordId,
                name: recordData.recordId,
                fileCount: recordData.files?.length || 0,
                files: recordData.files || [],
                createdTime: recordData.createdAt,
                createdTimeFormatted: recordData.createdAt ? new Date(recordData.createdAt).toLocaleString('zh-CN') : '',
                modifiedTime: recordData.updatedAt,
                modifiedTimeFormatted: recordData.updatedAt ? new Date(recordData.updatedAt).toLocaleString('zh-CN') : '',
            });
            
            // 格式化文件列表
            const formattedFiles = (recordData.files || []).map((file, index) => ({
                key: file.fileName,
                index: index + 1,
                name: file.fileName,
                size: `${file.rowCount || 0} 条记录`,
            }));
            
            setFileList(formattedFiles);
        } catch (error) {
            console.error('获取记录详情失败:', error);
            message.error('获取记录详情失败: ' + error.message);
        } finally {
            setLoading(false);
        }
    };

    const handleDownload = async (filename) => {
        try {
            const recordId = decodeURIComponent(id);
            
            // 映射文件名到 tableKey
            const FILE_MAP = {
                'result.xlsx': 'result',
                'merge.xlsx': 'merge',
                'transactions.xlsx': 'transactions',
                '可疑收款账号.xlsx': 'abnormal_accounts',
                'shopping.xlsx': 'shopping',
                '高风险地点统计.xlsx': 'risk_hotspot',
                '实口地址高风险统计.xlsx': 'risk_population',
                '外卖收货地址高风险统计.xlsx': 'risk_shopping',
            };
            
            const tableKey = FILE_MAP[filename];
            if (!tableKey) {
                message.error('不支持下载该文件类型');
                return;
            }
            
            const blob = await exportRecordFile(recordId, tableKey);
            const downloadUrl = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = downloadUrl;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(downloadUrl);
            message.success('下载成功');
        } catch (error) {
            console.error('下载失败:', error);
            message.error('下载失败: ' + error.message);
        }
    };

    const getFileTag = (filename) => {
        if (filename.includes('result')) {
            return <Tag color="blue">结果</Tag>;
        } else if (filename.includes('merge')) {
            return <Tag color="green">合并</Tag>;
        } else if (filename.includes('统计')) {
            return <Tag color="orange">统计</Tag>;
        } else if (filename.includes('可疑')) {
            return <Tag color="red">可疑</Tag>;
        } else if (filename === 'shopping.xlsx' || filename.includes('shopping')) {
            return <Tag color="purple">购物</Tag>;
        } else if (filename === 'transactions.xlsx' || filename.includes('transactions')) {
            return <Tag color="cyan">交易</Tag>;
        }
        return null;
    };

    const columns = [
        {
            title: '序号',
            dataIndex: 'index',
            key: 'index',
            width: 80,
        },
        {
            title: '文件名',
            dataIndex: 'name',
            key: 'name',
            render: (text) => (
                <Space>
                    {getFileTag(text)}
                    {text}
                </Space>
            ),
        },
        {
            title: '文件大小',
            dataIndex: 'size',
            key: 'size',
            width: 120,
        },
        {
            title: '操作',
            key: 'action',
            width: 250,
            render: (_, record) => (
                <Space>
                    {record.name === 'result.xlsx' && (
                        <Link to={`/result/${encodeURIComponent(id)}`}>
                            <Button type="link" icon={<EyeOutlined />}>查看详情</Button>
                        </Link>
                    )}
                    <Button 
                        type="link" 
                        icon={<DownloadOutlined />}
                        onClick={() => handleDownload(record.name)}
                    >
                        下载
                    </Button>
                </Space>
            ),
        },
    ];

    return (
        <div className='record-detail-page'>
            <Card 
                title={
                    <Space>
                        <Button 
                            type="text" 
                            icon={<ArrowLeftOutlined />} 
                            onClick={() => navigate('/')}
                        >
                            返回
                        </Button>
                        <span>记录详情 - {recordInfo?.name || decodeURIComponent(id)}</span>
                    </Space>
                }
                extra={
                    <Button 
                        icon={<ReloadOutlined />} 
                        onClick={loadRecordDetail}
                        loading={loading}
                    >
                        刷新
                    </Button>
                }
            >
                {recordInfo && (
                    <div style={{ marginBottom: 16 }}>
                        <Space>
                            <Tag>创建时间: {recordInfo.createdTimeFormatted}</Tag>
                            <Tag>修改时间: {recordInfo.modifiedTimeFormatted}</Tag>
                            <Tag>文件数量: {recordInfo.fileCount}</Tag>
                        </Space>
                    </div>
                )}
                <Table 
                    columns={columns} 
                    dataSource={fileList}
                    loading={loading}
                    pagination={{
                        pageSize: 10,
                        showSizeChanger: true,
                        showTotal: (total) => `共 ${total} 个文件`,
                    }}
                />
            </Card>
        </div>
    );
};

export default RecordDetailPage;

