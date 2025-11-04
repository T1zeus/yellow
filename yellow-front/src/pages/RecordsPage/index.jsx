import { useState, useEffect } from 'react';
import { Card, Table } from 'antd';
import { Link } from 'react-router-dom';

const columns = [
    {
        title: '序号',
        dataIndex: 'index',
        key: 'index',
    },
    {
        title: '创建时间',
        dataIndex: 'create_at',
        key: 'create_at',
    },
    {
        title: '操作',
        key: 'action',
        render: (_, record) => (
            <Link to={`/result/${record.key}`}>详情</Link>
        ),
    },
];

const RecordsPage = () => {
    const [data, setData] = useState([]);

    useEffect(() => {
        loadRecordList();
    }, []);

    const loadRecordList = async () => {
        const data = [
            {
                key: '1',
                index: '1',
                create_at: '2025-11-04 11:00',
            },
            {
                key: '2',
                index: '2',
                create_at: '2025-11-04 11:00',
            },
            {
                key: '3',
                index: '3',
                create_at: '2025-11-04 11:00',
            },
        ];
        setData(data);
    };

    return (
        <div className='records-page'>
            <Card title="历史分析记录">
                <Table columns={columns} dataSource={data} />
            </Card>
        </div>
    );
};

export default RecordsPage;