import { useState, useEffect } from 'react';
import { Card, Table, Tag } from 'antd';
import { Link } from 'react-router-dom';

const columns = [
    {
        title: '序号',
        dataIndex: 'index',
        key: 'index',
    },
    {
        title: '姓名',
        dataIndex: 'name',
        key: 'name',
    },
    {
        title: '证件号码',
        dataIndex: 'number',
        key: 'number',
    },
    {
        title: '手机号码',
        dataIndex: 'phone',
        key: 'phone',
    },
    {
        title: '前科次数',
        dataIndex: 'criminal_count',
        key: 'criminal_count',
    },
    {
        title: '户籍地址',
        dataIndex: 'residence_addr',
        key: 'residence_addr',
    },
    {
        title: '居住地址',
        dataIndex: 'live_addr',
        key: 'live_addr',
    },
    {
        title: '所属辖区',
        dataIndex: 'belong',
        key: 'belong',
    },
    {
        title: '风险级别',
        dataIndex: 'risk',
        key: 'risk',
        render: (_) => {
            switch (_) {
                case 1:
                    return <Tag color="#87d068">低</Tag>;
                case 2:
                    return <Tag color="#faad14">中</Tag>;
                case 3:
                    return <Tag color="#f50">高</Tag>;
            }
        }
    }
];

const ResultsPage = () => {
    const [data, setData] = useState([]);

    useEffect(() => {
        loadRecordList();
    }, []);

    const loadRecordList = async () => {
        const data = [
            {
                key: '1',
                index: '1',
                name: '小钱',
                number: '310227199002300992',
                phone: '18754878998',
                criminal_count: 2,
                residence_addr: '新桥镇新中街10000号100010室',
                live_addr: '新桥镇新中街10000号100010室',
                belong: '新桥派出所',
                risk: 3
            },
            {
                key: '2',
                index: '2',
                name: '小钱',
                number: '310227199002300992',
                phone: '18754878998',
                criminal_count: 2,
                residence_addr: '新桥镇新中街10000号100010室',
                live_addr: '新桥镇新中街10000号100010室',
                belong: '新桥派出所',
                risk: 2
            },
            {
                key: '3',
                index: '3',
                name: '小钱',
                number: '310227199002300992',
                phone: '18754878998',
                criminal_count: 2,
                residence_addr: '新桥镇新中街10000号100010室',
                live_addr: '新桥镇新中街10000号100010室',
                belong: '新桥派出所',
                risk: 1
            },
        ];
        setData(data);
    };

    return (
        <div className='records-page'>
            <Card title="分析结果">
                <Table columns={columns} dataSource={data} />
            </Card>
        </div>
    );
};

export default ResultsPage;