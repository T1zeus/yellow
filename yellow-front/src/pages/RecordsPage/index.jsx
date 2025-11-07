import { useState, useEffect } from 'react';
import { Card, Table, Button, Space, Tag, message } from 'antd';
import { DownloadOutlined, EyeOutlined, ReloadOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { getResults } from '../../services/resultService';
import { downloadResultFile } from '../../services/resultService';
import { formatFileSize } from '../../utils/format';

const RecordsPage = () => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        loadRecordList();
    }, []);

    const loadRecordList = async () => {
        setLoading(true);
        try {
            const result = await getResults();
            const files = result.files || [];
            
            const formattedData = files.map((file, index) => ({
                key: file.name,
                index: index + 1,
                name: file.name,
                size: file.sizeFormatted || formatFileSize(file.size),
                modifiedTime: file.modifiedTimeFormatted,
                url: file.url,
            }));
            
            setData(formattedData);
        } catch (error) {
            console.error('获取结果列表失败:', error);
            message.error('获取结果列表失败: ' + error.message);
        } finally {
            setLoading(false);
        }
    };

    const handleDownload = async (filename) => {
        try {
            const blob = await downloadResultFile(filename);
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
            title: '修改时间',
            dataIndex: 'modifiedTime',
            key: 'modifiedTime',
            width: 180,
        },
        {
            title: '操作',
            key: 'action',
            width: 200,
            render: (_, record) => (
                <Space>
                    {record.name === 'result.xlsx' && (
                        <Link to={`/result/${encodeURIComponent(record.name)}`}>
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
        <div className='records-page'>
            <Card 
                title="历史分析记录"
                extra={
                    <Button 
                        icon={<ReloadOutlined />} 
                        onClick={loadRecordList}
                        loading={loading}
                    >
                        刷新
                    </Button>
                }
            >
                <Table 
                    columns={columns} 
                    dataSource={data}
                    loading={loading}
                    pagination={{
                        pageSize: 10,
                        showSizeChanger: true,
                        showTotal: (total) => `共 ${total} 条记录`,
                    }}
                />
            </Card>
        </div>
    );
};

export default RecordsPage;