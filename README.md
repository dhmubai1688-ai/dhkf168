# TG 打卡 — GitHub 网页一次上传版

本目录只有 8 个文件。源码完整保存在三个 tar.gz 文件中，功能与独立项目相同；Render 构建时自动解压。

## 上传步骤

1. 解压外层 `tg-attendance-github-upload.zip`。
2. 打开解压后的 `github-upload` 文件夹。
3. 将其中 **8 个文件全部选中**，拖进 GitHub 网页上传区并提交。
4. **不要解压** `backend.tar.gz`、`frontend.tar.gz`、`shared.tar.gz`，也不要只把外层 ZIP 上传到仓库。
5. 仓库根目录应直接看到 Dockerfile、render.yaml 和三个 tar.gz 文件。
6. Render 新建 Docker Web Service，选择 Free，Root Directory 留空，Dockerfile Path 为 `./Dockerfile`。
7. 按 [部署指南](DEPLOY.md) 配置 Aiven、管理员密码、加密密钥和 CA 证书。

此包不包含真实密码、证书、node_modules 或前端构建产物，无需隐藏文件即可部署。不要把真实密码和证书添加到 GitHub。

## 修改源码

此包主要方便首次网页上传。以后修改源码时，应解压对应 tar.gz、修改后重新打包（压缩包内直接是该目录内容），或者改用完整源码项目配合 Git/GitHub Desktop。不要混用完整版 Dockerfile 与本包的压缩源码布局。

原始业务配置、员工和历史记录不会随源码迁移。免费服务休眠期间无法保证定时提醒准点执行。

## 验证范围

压缩包内每个文件均与已验证的独立项目逐字节核对。原项目已通过前端构建、Linux 后端编译及相关测试；本上传版未实际执行 Docker 镜像构建或 Render 公网部署，仍需部署后验收。
