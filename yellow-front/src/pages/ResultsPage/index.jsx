import { useState, useEffect } from 'react';
import { Button, Card, Table, Tag, Drawer } from 'antd';
import { Link } from 'react-router-dom';

const criminalColumns = [
    {
        title: '序号',
        dataIndex: 'index',
        key: 'index',
    },
    {
        title: '案发地点',
        dataIndex: 'criminal_addr',
        key: 'criminal_addr',
    },
    {
        title: '场所类型',
        dataIndex: 'criminal_type',
        key: 'criminal_type',
        render: (_) => {
            switch (_) {
                case 1:
                    return '小区';
                case 2:
                    return '宾馆';
                case 3:
                    return '场所';
                case 4:
                    return '商务楼';
                default:
                    return '其它';
            }
        }
    },
    {
        title: '案件详情',
        dataIndex: 'criminal_desc',
        key: 'criminal_desc',
    },
];

const ResultsPage = () => {
    const [data, setData] = useState([]); // 分析结果
    const [open, setOpen] = useState(false);
    const [criminalData, setCriminalData] = useState([]); // 前科记录

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
            render: (_, record) => (
                _ > 0 ? (
                    <Button type="link" onClick={() => showDrawer(record.key)}>{_}</Button>
                ) : _
            )
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

    useEffect(() => {
        loadRecordList();
    }, []);

    // 获取分析结果
    const loadRecordList = async () => {
        const data = [
            {
                key: '1',
                index: '1',
                name: '小钱',
                number: '310227199002300992',
                phone: '18754878998',
                criminal_count: 3,
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
                criminal_count: 3,
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
                criminal_count: 3,
                residence_addr: '新桥镇新中街10000号100010室',
                live_addr: '新桥镇新中街10000号100010室',
                belong: '新桥派出所',
                risk: 1
            },
        ];
        setData(data);
    };
    // 获取某人前科列表
    const loadCriminalList = async (id) => {
        const data = [
            {
                key: '1',
                index: '1',
                criminal_addr: '上海市松江区一二三路111弄-1号商务中心11楼1101室',
                criminal_type: 1,
                criminal_desc: '2025年5月13日，经群众匿名举报，在上海市松江区一二三路111弄-1号商务中心11楼1101室内，有卖淫嫖娼活动。已接报',
            },
            {
                key: '2',
                index: '2',
                criminal_addr: '上海市松江区一二三路111弄-1号商务中心11楼1101室',
                criminal_type: 2,
                criminal_desc: '2025年5月13日，经群众匿名举报，在上海市松江区一二三路111弄-1号商务中心11楼1101室内，有卖淫嫖娼活动。已接报',
            },
            {
                key: '3',
                index: '3',
                criminal_addr: '上海市松江区一二三路111弄-1号商务中心11楼1101室',
                criminal_type: 3,
                criminal_desc: '2025年5月13日，经群众匿名举报，在上海市松江区一二三路111弄-1号商务中心11楼1101室内，有卖淫嫖娼活动。已接报',
            },
        ];
        setCriminalData(data);
    };

    const showDrawer = (id) => {
        // 通过id获取这个人的前科记录更新抽屉中的表格
        loadCriminalList(id);
        setOpen(true);
    };
    const onClose = () => {
        setOpen(false);
    };

    return (
        <div className='records-page'>
            <Card title={`分析结果-MLXS-2025-663MBG7MJ09`}>
                <Table columns={columns} dataSource={data} />

                <Drawer
                    title="前科记录"
                    onClose={onClose}
                    open={open}
                    width={1000}
                >
                    <Table columns={criminalColumns} dataSource={criminalData} />
                </Drawer>
            </Card>
        </div>
    );
};

export default ResultsPage;