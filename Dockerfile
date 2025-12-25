FROM python:3.10-slim

WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 设置阿里云源（保持现状）
RUN rm -f /etc/apt/sources.list && rm -rf /etc/apt/sources.list.d/* \
    && echo "deb https://mirrors.aliyun.com/debian/ bookworm main non-free non-free-firmware" > /etc/apt/sources.list \
    && echo "deb https://mirrors.aliyun.com/debian-security/ bookworm-security main non-free non-free-firmware" >> /etc/apt/sources.list \
    && echo "deb https://mirrors.aliyun.com/debian/ bookworm-updates main non-free non-free-firmware" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y --no-install-recommends procps vim && rm -rf /var/lib/apt/lists/*
# 只安装 Python 依赖，不再安装 cron
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY . .

# 容器不需要保持运行，它只要“被叫醒”执行脚本即可
# 这里的 CMD 只是一个默认值，会被 docker exec 覆盖或配合运行
CMD ["tail", "/dev/null"]