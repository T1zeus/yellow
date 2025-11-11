import { useState, useEffect } from 'react';
import { Card, Table, Button, Space, Tag, message } from 'antd';
import { EyeOutlined, ReloadOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { getRecords } from '../../services/resultService';

const RecordsPage = () => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(false);
    const navigate = useNavigate();

    useEffect(() => {
        loadRecordList();
    }, []);

    const loadRecordList = async () => {
        setLoading(true);
        try {
            const result = await getRecords();
            const records = result.records || [];
            
            const formattedData = records.map((record, index) => ({
                key: record.id,
                index: index + 1,
                id: record.id,
                name: record.name,
                fileCount: record.fileCount,
                createdTime: record.createdTimeFormatted,
                modifiedTime: record.modifiedTimeFormatted,
            }));
            
            setData(formattedData);
        } catch (error) {
            console.error('获取记录列表失败:', error);
            message.error('获取记录列表失败: ' + error.message);
        } finally {
            setLoading(false);
        }
    };

    const handleViewDetail = (recordId) => {
        navigate(`/record/${encodeURIComponent(recordId)}`);
    };

    const columns = [
        {
            title: '序号',
            dataIndex: 'index',
            key: 'index',
            width: 80,
        },
        {
            title: '记录名称',
            dataIndex: 'name',
            key: 'name',
            render: (text) => (
                <Space>
                    <Tag color="blue" style={{ fontSize: '14px', padding: '4px 12px', lineHeight: '24px' }}>{text}</Tag>
                </Space>
            ),
        },
        {
            title: '文件数量',
            dataIndex: 'fileCount',
            key: 'fileCount',
            width: 100,
        },
        {
            title: '创建时间',
            dataIndex: 'createdTime',
            key: 'createdTime',
            width: 180,
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
            width: 150,
            render: (_, record) => (
                <Space>
                    <Button 
                        type="link" 
                        icon={<EyeOutlined />}
                        onClick={() => handleViewDetail(record.id)}
                    >
                        查看详情
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