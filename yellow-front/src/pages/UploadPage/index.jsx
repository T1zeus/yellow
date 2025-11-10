import { useState, useEffect, useRef } from 'react';
import { Card, Form, Upload, Button, message, Progress, Space } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { uploadFiles } from '../../services/uploadService';
import { getProgress } from '../../services/uploadService';

import './index.less';

const UploadPage = () => {
  const [form] = Form.useForm();
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState('');
  const [fileList, setFileList] = useState({
    criminal_file: [],
    population_file: [],
    employment_file: [],
    insurance_file: [],
    shopping_file: [],
    roommate_file: [],
    transaction_files: [],
    hotel_files: [],
  });
  const progressTimerRef = useRef(null);
  const navigate = useNavigate();

  useEffect(() => {
    return () => {
      if (progressTimerRef.current) {
        clearInterval(progressTimerRef.current);
      }
    };
  }, []);

 const startProgressPolling = () => {
  if (progressTimerRef.current) {
    clearInterval(progressTimerRef.current);
  }

  progressTimerRef.current = setInterval(async () => {
    try {
      const data = await getProgress();
      setProgress(data.percent || 0);
      setError(data.error || '');

      if (data.percent >= 100 || data.percent === -1) {
        clearInterval(progressTimerRef.current);
        progressTimerRef.current = null; // 添加这行
        setUploading(false);
        
        if (data.percent === -1) {
          message.error(`处理失败: ${data.error || '未知错误'}`);
        } else {
          message.success('处理完成！');
          setTimeout(() => {
            navigate('/');
          }, 1500);
        }
      }
    } catch (error) {
      console.error('获取进度失败:', error);
      // 如果连接失败，停止轮询（可能是服务器未启动）
      if (error.message.includes('Failed to fetch') || error.message.includes('ERR_CONNECTION_REFUSED')) {
        clearInterval(progressTimerRef.current);
        progressTimerRef.current = null;
        setUploading(false);
        message.error('无法连接到服务器，请确保后端服务已启动');
      }
    }
  }, 1000);
};

  const onFinish = async () => {
    const hasFiles = Object.values(fileList).some(list => list.length > 0);
    if (!hasFiles) {
      message.warning('请至少上传一个文件');
      return;
    }

    setUploading(true);
    setProgress(0);
    setError('');

    try {
      const formData = new FormData();
      
      Object.entries(fileList).forEach(([fieldName, files]) => {
        files.forEach((file, index) => {
          if (file.originFileObj) {
            const fileName = file.name || file.originFileObj.name || '';
            // 对于文件夹上传，过滤掉临时文件（以 ~$ 开头的文件）
            if (fieldName === 'transaction_files' || fieldName === 'hotel_files') {
              if (fileName.startsWith('~$')) {
                // 跳过临时文件，不上传
                return;
              }
              const relativePath = file.originFileObj.webkitRelativePath || file.name;
              
              // 将路径信息作为元数据传递（使用唯一字段名，避免覆盖）
              // 使用索引和文件名的哈希值作为唯一标识，避免特殊字符问题
              const safeFileName = fileName.replace(/[^a-zA-Z0-9._-]/g, '_').substring(0, 50);
              const metadataKey = `${fieldName}_metadata_${index}_${safeFileName}`;
              formData.append(metadataKey, JSON.stringify({
                index: index,
                path: relativePath,
                name: fileName,
                fieldName: fieldName
              }));
              
              // 上传文件
              formData.append(fieldName, file.originFileObj);
            } else {
              formData.append(fieldName, file.originFileObj);
            }
          }
        });
      });

      await uploadFiles(formData);
      message.success('文件上传成功，开始处理...');
      startProgressPolling();
    } catch (error) {
      console.error('上传失败:', error);
      message.error(error.message || '上传失败');
      setUploading(false);
      setError(error.message || '上传失败');
    }
  };

  const handleFileChange = (fieldName) => (info) => {
    let newFileList = [...info.fileList];
    
    // 对于文件夹上传，过滤掉 Excel 临时文件（以 ~$ 开头的文件）
    if (fieldName === 'transaction_files' || fieldName === 'hotel_files') {
      newFileList = newFileList.filter(file => {
        const fileName = file.name || file.originFileObj?.name || '';
        // 过滤掉临时文件（以 ~$ 开头的文件）
        if (fileName.startsWith('~$')) {
          return false;
        }
        return true;
      });
    } else {
      // 普通文件，只保留最后一个
      newFileList = newFileList.slice(-1);
    }

    setFileList(prev => ({
      ...prev,
      [fieldName]: newFileList,
    }));
  };

  const handleRemove = (fieldName) => (file) => {
    setFileList(prev => ({
      ...prev,
      [fieldName]: prev[fieldName].filter(item => item.uid !== file.uid),
    }));
  };

  const beforeUpload = (file, fieldName) => {
    // 对于文件夹上传，过滤掉临时文件
    if (fieldName === 'transaction_files' || fieldName === 'hotel_files') {
      if (file.name.startsWith('~$')) {
        return Upload.LIST_IGNORE; // 忽略临时文件
      }
    }
    
    const isExcel = file.name.endsWith('.xlsx') || file.name.endsWith('.xls');
    if (!isExcel && fieldName !== 'transaction_files' && fieldName !== 'hotel_files') {
      message.error('只能上传 Excel 文件！');
      return Upload.LIST_IGNORE;
    }
    return false;
  };

  return (
    <div className='upload-page'>
      <Card title="上传分析文件">
        <div className='upload-container'>
          <Form form={form} layout="vertical" onFinish={onFinish}>
            <Form.Item label="简要案情（Excel）" name="criminal_file">
              <Upload
                accept=".xlsx,.xls"
                fileList={fileList.criminal_file}
                onChange={handleFileChange('criminal_file')}
                onRemove={handleRemove('criminal_file')}
                beforeUpload={(file) => beforeUpload(file, 'criminal_file')}
                maxCount={1}
              >
                <Button icon={<UploadOutlined />}>点击上传文件</Button>
              </Upload>
            </Form.Item>

            <Form.Item label="实有人口（Excel）" name="population_file">
              <Upload
                accept=".xlsx,.xls"
                fileList={fileList.population_file}
                onChange={handleFileChange('population_file')}
                onRemove={handleRemove('population_file')}
                beforeUpload={(file) => beforeUpload(file, 'population_file')}
                maxCount={1}
              >
                <Button icon={<UploadOutlined />}>点击上传文件</Button>
              </Upload>
            </Form.Item>

            <Form.Item label="从业人员（Excel）" name="employment_file">
              <Upload
                accept=".xlsx,.xls"
                fileList={fileList.employment_file}
                onChange={handleFileChange('employment_file')}
                onRemove={handleRemove('employment_file')}
                beforeUpload={(file) => beforeUpload(file, 'employment_file')}
                maxCount={1}
              >
                <Button icon={<UploadOutlined />}>点击上传文件</Button>
              </Upload>
            </Form.Item>

            <Form.Item label="社保（Excel）" name="insurance_file">
              <Upload
                accept=".xlsx,.xls"
                fileList={fileList.insurance_file}
                onChange={handleFileChange('insurance_file')}
                onRemove={handleRemove('insurance_file')}
                beforeUpload={(file) => beforeUpload(file, 'insurance_file')}
                maxCount={1}
              >
                <Button icon={<UploadOutlined />}>点击上传文件</Button>
              </Upload>
            </Form.Item>

            <Form.Item label="购物（Excel）" name="shopping_file">
              <Upload
                accept=".xlsx,.xls"
                fileList={fileList.shopping_file}
                onChange={handleFileChange('shopping_file')}
                onRemove={handleRemove('shopping_file')}
                beforeUpload={(file) => beforeUpload(file, 'shopping_file')}
                maxCount={1}
              >
                <Button icon={<UploadOutlined />}>点击上传文件</Button>
              </Upload>
            </Form.Item>

            <Form.Item label="同住信息（Excel）" name="roommate_file">
              <Upload
                accept=".xlsx,.xls"
                fileList={fileList.roommate_file}
                onChange={handleFileChange('roommate_file')}
                onRemove={handleRemove('roommate_file')}
                beforeUpload={(file) => beforeUpload(file, 'roommate_file')}
                maxCount={1}
              >
                <Button icon={<UploadOutlined />}>点击上传文件</Button>
              </Upload>
            </Form.Item>

            <Form.Item 
              label="资金交易（文件夹）" 
              name="transaction_files"
              help="支持上传整个文件夹，保持目录结构"
            >
              <Upload
                directory
                fileList={fileList.transaction_files}
                onChange={handleFileChange('transaction_files')}
                onRemove={handleRemove('transaction_files')}
                beforeUpload={(file) => beforeUpload(file, 'transaction_files')}
              >
                <Button icon={<UploadOutlined />}>点击上传文件夹</Button>
              </Upload>
            </Form.Item>

            <Form.Item 
              label="酒店入住（文件夹）" 
              name="hotel_files"
              help="支持上传整个文件夹，包含女性入住和入住男性文件"
            >
              <Upload
                directory
                fileList={fileList.hotel_files}
                onChange={handleFileChange('hotel_files')}
                onRemove={handleRemove('hotel_files')}
                beforeUpload={(file) => beforeUpload(file, 'hotel_files')}
              >
                <Button icon={<UploadOutlined />}>点击上传文件夹</Button>
              </Upload>
            </Form.Item>

            {uploading && (
              <Form.Item>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Progress 
                    percent={progress} 
                    status={progress === -1 ? 'exception' : 'active'}
                    strokeColor={{
                      '0%': '#108ee9',
                      '100%': '#87d068',
                    }}
                  />
                  {error && (
                    <div style={{ color: '#f50', fontSize: '12px' }}>{error}</div>
                  )}
                </Space>
              </Form.Item>
            )}

            <Form.Item>
              <Button 
                type="primary" 
                block 
                htmlType="submit"
                loading={uploading}
                disabled={uploading}
              >
                {uploading ? '处理中...' : '开始分析'}
              </Button>
            </Form.Item>
          </Form>
        </div>
      </Card>
    </div>
  );
};

export default UploadPage;