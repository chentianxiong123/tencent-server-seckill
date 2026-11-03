# CDP + jienigui 抢购（本仓库实战版）

fork 自 djs-91/tencent-server-seckill 后的实战改造，走 CDP 连**真实浏览器 profile**（非 storage_state 隔离），核心差异：

- **CDP 连复制真实 profile 的 Chrome**（`--remote-debugging-port=9222`），`navigator.webdriver=False`、书签/历史/cookie 真实，指纹检测 bot.sannysoft 全绿、CreepJS 38% like headless（腾讯云不做深度指纹）。
- **登录态**：页面扫码登录后 cookie 存进复制 profile；因 session cookie 不持久，用备份恢复并**设 expires=+1 年强制持久**（`cdp-cookie-backup-*.json`），重启不丢。
- **CSRF**：从 skey cookie 按腾讯算法算 `X-CSRF-TOKEN`，`page.route()` 注入（非 window.__latestCsrf hook）。
- **发送策略 = 上游 djs-91 口径**：提前 2 秒探针（`ADVANCE_MS=-2000`）→ 开窗前 0.6 秒全速铺开（`PRE_OPEN_FIRE_MS=600`）→ 同时最多 30 条回来补一（`INFLIGHT=30`）；`408/1100205/1100110/1101001` 当排队信号**继续**，`1100136` 售罄即停、`1100106/1100107/1100109` 资格类即停；不用 `preview=1` 预热。
- **单地域**：只抢广州 `regionId=1`（`TARGET_REGION`）。
- 对时 `get-now×7` 取往返最快 + 20ms 内中位数，实测偏差 ±20ms。

## 运行

前置：复制真实 profile 的 Chrome 已开 `9222`，窗口在 `https://cloud.tencent.com/act/pro/double11-202611`。

```bash
pip install -r requirements.txt
python3 snap-api.py              # 正式：自动等最近场次(10:00/15:00)开抢
python3 snap-api.py --dry-run    # 只查询不下单(验证登录/act_id/imageId)
python3 snap-api.py --mock       # 模拟：纯本地,零网络,验证时序/码处理(不真打)
python3 snap-api.py --now        # 立即测试(真发下单请求,慎用)
```

登录态备份文件（含 `.cloud.tencent.cn` 纯数字 `uin=100054189855` + `skey`）：
`cdp-cookie-backup-2026-11-03.json`（537 cookie，已设 expires=+1 年）。

## 关键返回码

| code | 含义 | 动作 |
|---|---|---|
| 0 | 成功已下单 | 停，去控制台付款 |
| 408 / 1100205 / 1100110 / 1101001 | 排队/未开始 | 继续 |
| 1100136 | 已售罄 | 停（本场没名额） |
| 1100106 / 1100107 / 1100109 | 资格/参数 | 停 |
| NOT-LOGINED | 登录态失效 | 停，重登 |

`--mock` 用 `MOCK_FIRE_JS` 纯本地模拟（不发 fetch、零网络），日志各码次数可判断卡点。
