# Render 免费版 + Aiven 免费 PostgreSQL 部署

核对日期：2026-11-07。费用及限额最终以服务商控制台为准。

## 1. 准备独立数据库

在 Aiven 创建 PostgreSQL Free 服务。官方当前标注 1 CPU、1 GB RAM、1 GB 磁盘、20 个连接，不提供连接池；没有固定到期日，但不持续使用可能被暂停。

- 在 Aiven 控制台复制 Service URI，密码含特殊字符时使用控制台提供的 URI，避免手工错误编码。
- 使用专用的空数据库，例如 `tg_attendance`；也可以使用新服务中尚未存放业务表的 `defaultdb`。
- 下载 **CA certificate**，命名 `aiven-ca.pem`。
- 不导入 monitor 的完整数据库；本项目首次启动自动建立兼容的考勤表。
- 推荐 Render 与 Aiven 选尽可能接近的区域。

生产连接会强制 `sslmode=verify-full` 并使用 `PGSSLROOTCERT` 指向的证书，校验 CA 和服务器主机名。不要为解决连接错误关闭校验。

## 2. 准备代码仓库

此为 GitHub 网页上传精简包。把 `github-upload` 文件夹中的全部文件上传到独立仓库根目录：根目录应直接看见 `Dockerfile`、`render.yaml` 和三个 `.tar.gz` 文件。不要解压这三个源码压缩文件，Render 会在构建时自动解压。

不要上传 `.env`、CA、密码、Bot Token 或 `node_modules`。本项目已附 `.gitignore` / `.dockerignore`。

若选择继续放在大仓库中，则 Render 的 Root Directory 填 `tg-attendance-standalone`；Dockerfile 和 Docker Context 都相对于这个目录。Blueprint 建议采用独立仓库，避免根目录解析差异。

## 3. 创建 Render 服务

在 Render 选择 New → Web Service，连接仓库：

- Language / Runtime：Docker。
- Instance Type：Free。
- Dockerfile Path：`./Dockerfile`。
- Docker Context：`.`。
- Health Check Path：`/health`。
- 不创建 Render Postgres、不创建 Redis、不创建磁盘或额外 Worker。

也可从仓库中的 Blueprint (`render.yaml`) 创建。Blueprint 不包含真实 CA 文件；第一次部署若提示缺少 CA，需要先添加下节 Secret File，再重新部署。

设置环境变量：

| 变量 | 值 |
|---|---|
| `ENV` | `production` |
| `TZ` | `Asia/Shanghai`，也可按团队所在地设置 |
| `DATABASE_URL` | Aiven 的完整 PostgreSQL URI，数据库为专用空库 |
| `PGSSLROOTCERT` | `/etc/secrets/aiven-ca.pem` |
| `ADMIN_USERNAME` | `admin` 或自定管理员名 |
| `ADMIN_PASSWORD` | 自己生成的强密码，至少 12 字符 |
| `TELEGRAM_TOKEN_SECRET` | 独立随机字符串，至少 32 字符；Blueprint 会生成 |
| `DB_MAX_CONNECTIONS` | `5`，允许配置 3–10 |
| `GOMEMLIMIT` | `256MiB` |
| `GOGC` | `70` |

在 Render → Environment → Secret Files 添加：

- Filename：`aiven-ca.pem`
- Contents：Aiven 下载证书的完整 PEM 内容。

Render 将其挂载到 `/etc/secrets/aiven-ca.pem`。CA 不是数据库密码，但应使用 Secret File 管理，避免随源代码替换错误。

`PORT` 由 Render 注入；程序使用它监听。`RENDER_EXTERNAL_URL` 用于生成 Webhook URL；自定义域名时可另外设置 `PUBLIC_BASE_URL=https://你的域名`，不要加路径或尾部 `/telegram/webhook`。

**保存好 `TELEGRAM_TOKEN_SECRET`，后续部署不要随意更换。** 它用于解密数据库里保存的 Bot Token 和 Webhook Secret。管理员密码只在首次创建账号时生效；改变部署变量不会覆盖已有账号密码。

## 4. 配置机器人和员工

1. 建议在 BotFather 创建一个专用于新项目的 Bot。一个 Bot 只能使用一个 Webhook；复用线上旧 Bot 会把旧系统的消息转移走。
2. 打开 Render 提供的 HTTPS 地址，使用上面配置的账号密码登录。
3. 在“员工与绑定”建立员工工号、姓名、职位等。
4. 在“打卡规则 → 基础配置”填写 Token、启用 Telegram、保存。Token 在数据库中加密保存，不回显。
5. 在“运行概览”点击“注册 / 更新 Webhook”。它不会清空尚待投递的 Telegram 消息。
6. 把 Bot 加入工作群。使用 `/chatid` 获取群 ID，在“群组绑定”添加；在“员工绑定”关联员工与 TG 用户 ID。
7. 选择允许的群组及语言、班次、活动时限/次数/并发人数、罚款档位、审批阈值。
8. 若要接收普通文本/底部键盘消息，按 Telegram 群隐私设置给机器人相应权限，或先使用 `/checkin_day`、`/wc1`、`/at` 等命令验证。
9. 换 Token、Webhook Secret 或域名后，再点击“注册 / 更新 Webhook”。一般修改班次/活动规则无需重启服务。

## 5. 验收

- 登录、刷新页面和退出正常；退出后的后台接口返回未登录。
- 发送 `/help`，运行概览新增消息并完成处理。
- TG 上班 → 开始活动 → 回座 → 下班，后台对应出现记录。
- 重复上班不会新增第二条开放班次；重复 Webhook 不会重新处理。
- 中文/越南语/英文回复、夜班跨午夜归属、罚款和审批结果符合预期。
- 后台导出 Excel 可以打开；确认群组过滤和日期范围正确。
- Render 休眠后发送一条命令，确认唤醒后到账；普通消息打卡时间应为 Telegram 发送时间。
- 临时数据库故障后，未成功入库的请求返回非 200，Telegram 可重试；不要手动删除队列。

## 6. 容量与维护

- Aiven 免费 1 GB 主要用于考勤记录，勿混入截图、视频等媒体。
- 已完成收件箱元数据保留 30 天；正文处理成功后立即移除。待核对消息保留供排查。
- 管理会话过期清理与队列清理在服务醒着且 Bot 启用时进行。
- 日重置备份在 Aiven 保存 7 天，可在运行概览下载；临时磁盘文件仅用于发送。
- 原业务归档保留；历史考勤不要未经备份直接清空。定期导出与数据库备份应独立保存。
- 若发现“待核对”，先按收到时间检查考勤、活动及罚款；必要时后台修正，然后确认核对。确认不会重放消息。
- 没有使用外部保活服务。需要全天准点提醒时，应升级为常驻计算资源；增加虚假的保活请求不能解决所有免费配额限制。

## 7. 当前验证范围

已在本地独立 PostgreSQL 实例验证初始化、管理员登录、员工管理及自动化业务链路。未连接用户真实 Aiven、Render 账号或真实 Bot，因而不能把本地通过视为公网部署验收通过。

参考官方文档：
- [Render Free](https://render.com/docs/free)
- [Render environment variables and secrets](https://render.com/docs/configure-environment-variables)
- [Aiven PostgreSQL free tier](https://aiven.io/docs/products/postgresql/concepts/pg-free-tier)
- [Aiven TLS certificates](https://aiven.io/docs/platform/concepts/tls-ssl-certificates)
- [Telegram Bot API](https://core.telegram.org/bots/api)

日重置报表先持久保存在 Aiven 的 `standalone_exports` 表，再进入重置流程；后台运行概览可下载最近 7 天的备份。临时文件仅用于发送，发送流程结束即删除。此备份占用 Aiven 磁盘配额，大量群组应关注数据库容量。
