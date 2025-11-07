import { useState, useEffect } from 'react';
import { Card, Form, Upload, Button } from 'antd';
import { UploadOutlined } from '@ant-design/icons';

import './index.less';

const UploadPage = () => {

    useEffect(() => {
    }, []);

    return (
        <div className='records-page'>
            <Card title="上传分析文件">
                <div className='upload-container'>
                    <Form
                        layout="vertical"
                    //onFinish={onFinish}
                    >
                        <Form.Item
                            name="upload"
                            label="简要案情（Excel）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>
                        <Form.Item
                            name="upload"
                            label="实有人口（Excel）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>
                        <Form.Item
                            name="upload"
                            label="从业人员（Excel）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>
                        <Form.Item
                            name="upload"
                            label="社保（Excel）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>
                        <Form.Item
                            name="upload"
                            label="购物（Excel）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>
                        <Form.Item
                            name="upload"
                            label="同住信息（Excel）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>
                        <Form.Item
                            name="upload"
                            label="资金（文件夹）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>
                        <Form.Item
                            name="upload"
                            label="入住（文件夹）"
                        >
                            <Upload name="logo" action="/upload.do" listType="picture">
                                <Button icon={<UploadOutlined />}>点击上传文件</Button>
                            </Upload>
                        </Form.Item>

                        <Form.Item>
                            <Button type="primary" block htmlType="submit">开始分析</Button>
                        </Form.Item>
                    </Form>
                </div>
            </Card>
        </div>
    );
};

export default UploadPage;