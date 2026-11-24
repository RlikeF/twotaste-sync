# 双人食光 · 同步服务镜像
# 构建：docker build -t twotaste-sync server
# 运行：docker run -d --name twotaste-sync -p 8787:8787 -e TOKEN=你们的暗号 -v twotaste-data:/data twotaste-sync
FROM node:22-alpine

WORKDIR /app
COPY sync-server.mjs ./

# 数据落在卷里，容器重建也不丢
ENV DATA_FILE=/data/state.json
RUN mkdir -p /data

EXPOSE 8787
CMD ["node", "sync-server.mjs"]