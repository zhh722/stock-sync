FROM python:3.10-slim

WORKDIR /app

# 设置中国时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 彻底清理 APT 源：删除原有 sources.list 和 sources.list.d/
RUN rm -f /etc/apt/sources.list && \
    rm -rf /etc/apt/sources.list.d/*

RUN echo "deb https://mirrors.aliyun.com/debian/ bookworm main non-free non-free-firmware" > /etc/apt/sources.list && \
    echo "deb https://mirrors.aliyun.com/debian-security/ bookworm-security main non-free non-free-firmware" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.aliyun.com/debian/ bookworm-updates main non-free non-free-firmware" >> /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive
# 安装 cron
RUN apt-get update && apt-get install -y --no-install-recommends cron && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制文件
COPY sync_to_mysql.py .
COPY sync_daily.py .
COPY sync_weekly.py .
COPY crontab.conf /etc/cron.d/stock-sync
RUN chmod 0644 /etc/cron.d/stock-sync
RUN crontab /etc/cron.d/stock-sync

# 创建日志目录
RUN mkdir -p /app/logs

# 启动 cron（前台运行）
CMD ["cron", "-f"]