# 传输路径分组服务

面向大 CSV 的在线路径拆分与传输清单生成服务。

## 能力

- 可同时上传多个 CSV；每个文件以原始文件名显示为独立输入和独立路径树。
- 浏览器按 5MB 分片上传。网络中断后重新选择同一文件，会从服务端已有分片继续上传。
- 服务端逐行解析 CSV，不将整个文件读入内存；目录树只保存目录统计和每个目录最多 20 个文件名样本。
- 每个输入可先选择路径列，再在树中选择来源前缀、填写目的地，最后导出 `src_path,dst_path,rowkey` CSV。
- Docker 数据卷保存上传、任务、树摘要和导出文件；服务重启后处理中任务会自动重新排队。

## 本地开发

```bash
npm install
npm run build
npm run start
```

另开一个终端启动前端开发界面：

```bash
npm run dev
```

生产环境使用 Docker，并将 `/data` 挂载到持久化磁盘。GitHub Pages 不能运行本服务的上传和处理后端；请使用支持 Docker 和持久磁盘的服务部署。

## Render 部署

仓库已经包含 `render.yaml`。在 Render 中选择 New Blueprint，连接 `Isaac-some/Trie` 仓库和 `codex/persistent-csv-service` 分支，Render 会读取 Docker 配置、健康检查和 `/data` 持久磁盘设置。部署完成后，服务地址可以直接分享给使用者。
