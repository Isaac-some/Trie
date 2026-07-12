---
title: 传输路径分组
sdk: docker
app_port: 7860
---

# 传输路径分组服务

面向大 CSV 的在线路径拆分与传输清单生成服务。

## 能力

- 可同时上传多个 CSV；每个文件以原始文件名显示为独立输入和独立路径树。
- 浏览器按 5MB 分片上传。网络中断后重新选择同一文件，会从服务端已有分片继续上传。
- 服务端逐行解析 CSV，不将整个文件读入内存；目录树只保存目录统计和每个目录最多 20 个文件名样本。
- 每个输入可先选择路径列，再在树中选择来源前缀、填写目的地，最后导出 `src_path,dst_path,rowkey` CSV。
- 服务只在当前页面会话期间保存临时文件；刷新或关闭页面会清理该会话，服务重启也不会保留数据。

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

生产环境使用 Docker 和临时磁盘。GitHub Pages 不能运行本服务的上传和处理后端；请使用支持 Docker 和至少数百 MB 临时磁盘的服务部署。

## Render 部署

仓库已经包含 `render.yaml`。在 Render 中选择 New Blueprint，连接 `Isaac-some/Trie` 仓库和 `codex/persistent-csv-service` 分支，Render 会读取 Docker 配置和健康检查。应用按阅后即焚模式运行，不需要绑定持久磁盘；每个页面会话最多保留 1 小时的临时数据。

## Hugging Face Spaces 部署

也可以创建一个 Docker Space，把本仓库内容导入 Space。Space 配置已经声明端口 `7860`，不需要持久磁盘；页面刷新或关闭后当前会话会被清理。免费实例可能休眠或在长任务期间被回收，因此适合临时处理，不适合作为有 SLA 的生产服务。
