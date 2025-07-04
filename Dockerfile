# 使用一个轻量级的官方Python 3.10镜像作为基础
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件到工作目录
COPY requirements.txt .

# 安装项目依赖，--no-cache-dir减小镜像体积
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目所有文件到工作目录
COPY . .

# 声明容器对外暴露的端口
EXPOSE 8000

# 容器启动时运行的命令 (已修正为最终版)
# 直接启动api_server.py文件中的app实例
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]