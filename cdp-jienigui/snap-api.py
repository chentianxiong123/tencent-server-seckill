#!/usr/bin/env python3
r"""
1. 整点瞬间 prepare-do + do-goods 并行发出
   - v19: 整点→等prepare-do 610ms→发do-goods→等5508ms = 总6120ms
   - v20: 整点→prepare-do和do-goods同时发→最快结果回来即可
2. 整点前双重prepare-do(5秒+0.5秒各一次)，确保令牌在手
3. 每轮batch后追加"闪电单地域"请求(不限MAX_BATCH_ROUNDS)

API链路:
  query-seckill-by-date → 获取act_id + goods_param
  prepare-do(act_id)    → 预下单（约1-2秒）
  do-goods(act_id+goods_param) → 正式下单

CSRF:
  X-CSRF-TOKEN = hash(skey)，page.route()网络层注入

地域:
  上海=4, 北京=1, 广州=8

用法:
  python snap-api.py            # 正常秒杀（等整点）
  python snap-api.py --now      # 立刻测试（不等整点）
  python snap-api.py --dry-run  # 只查询不下单

前置条件:
  1. Chrome --remote-debugging-port=9222
  2. 已登录腾讯云，打开活动页
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ============================================================
# 配置
# ============================================================

CDP_URL = "http://127.0.0.1:9222"
ACTIVITY_ID = 166111704417621  # 2026 双十一活动期
COOKIE_BACKUP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "login_backup.json")

SECKILL_HOURS = [10, 15]

# ============================================================
# 上游(djs-91)抢购策略参数 — 单地域广州 + 提前量 + 开窗全速
# ============================================================
INFLIGHT = 30           # 同时最多30条, 回来一个补一个
POLL_MS = 40            # 看一次结果间隔(毫秒), 只影响发现成功速度
PRE_OPEN_FIRE_MS = 600  # 开窗前0.6秒全速铺开, 让第一批压在开窗到服务端
PROBE_MS = 200          # 开窗前探针间隔(毫秒)
DURATION_SEC = 10       # 持续发送时间(秒)
ADVANCE_MS = -2000      # 提前2秒开始抢(补偿网络延迟+对时误差)
QUIET_SECONDS = 15      # 最后15秒纯等待, 一次网络请求都不发
DO_GOODS_URL = "https://act-api.cloud.tencent.com/dianshi/do-goods"

# do-goods 业务返回码
CODE_OK = 0
CODE_NOT_STARTED = 1101001   # 秒杀尚未开始
CODE_ALREADY_BOUGHT = 1100106  # 已购买, 不满足首单
CODE_NO_QUALIFY = 1100107    # 不满足活动条件
CODE_BAD_PARAM = 1100109     # 商品参数不对
CODE_RATE_LIMIT = 408        # 频率过快(开抢瞬间正常)
CODE_BUSY = 1100205          # 系统繁忙(排队)
CODE_CONCURRENT = 1100110    # 同账号有单在提交
CODE_SOLD_OUT = 1100136      # 已售罄

# 命中就停的码: 重试无意义
NO_RETRY_CODES = {CODE_ALREADY_BOUGHT, CODE_NO_QUALIFY, CODE_BAD_PARAM, CODE_SOLD_OUT}

# 排队继续的码: 开抢瞬间都正常, 继续发
QUEUE_CODES = {CODE_NOT_STARTED, CODE_RATE_LIMIT, CODE_BUSY, CODE_CONCURRENT}

CODE_DESC = {
    CODE_NOT_STARTED: "秒杀尚未开始(未到时间或本场已结束)",
    CODE_ALREADY_BOUGHT: "该实名主体已买过这款首单商品, 不满足首单条件",
    CODE_NO_QUALIFY: "账号不满足本活动参与条件",
    CODE_BAD_PARAM: "商品参数不对(规格约束变了)",
    CODE_RATE_LIMIT: "访问频率限制(开抢瞬间大家都在抢), 继续",
    CODE_BUSY: "系统繁忙(开抢瞬间排队), 继续",
    CODE_CONCURRENT: "同账号有订单正在处理, 继续",
    CODE_SOLD_OUT: "商品已售罄(本场名额已经没了)",
    -1: "请求没发出去(网络断了或页面上下文没了)",
}

# 地域名映射（启动后由 discover_regions() 按当前活动页动态覆盖）。
# 当前 double11 期实测: 1=广州, 4=上海, 8=北京；region_id 是"大区号"，每期活动可能重分配。
REGIONS = {1: "广州", 4: "上海", 8: "北京"}
REGION_PRIORITY = [1, 4, 8]  # 抢购优先级: 广州 > 上海 > 北京（会被动态更新覆盖）

# 并行轮询: 每轮同时发多少个地域（三路一起抓）
MAX_PARALLEL_REGIONS = 3
# 最大轮次
MAX_BATCH_ROUNDS = 5
# 轮间间隔(秒)
BATCH_INTERVAL = 2.0
TERMINAL_PREPARE_CODES = {1100104, 1100106, 1100107, 1100136}
TERMINAL_DO_GOODS_CODES = {600001, 1100104, 1100106, 1100107}

# 历史日志中稳定发现过的轻量应用镜像。
# 当次页面/API发现结果始终优先；这些值只在发现失败时回退尝试。
HISTORICAL_IMAGE_IDS = [
    "lhbp-eqora508",
    "lhbp-etg1zbmm",
]

# ============================================================
# 日志
# ============================================================

_LOG_DIR = Path(__file__).parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)

_today = datetime.now().strftime('%Y-%m-%d')

def _get_session_tag():
    now = datetime.now()
    for hour in SECKILL_HOURS:
        target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if target > now:
            return f"{hour:02d}00"
    return f"{SECKILL_HOURS[0]:02d}00"

_session_tag = _get_session_tag()
LOG_FILE = _LOG_DIR / f"seckill_{_today}_{_session_tag}.log"

_log_fh = None

def _open_log():
    global _log_fh
    _log_fh = open(LOG_FILE, 'a', encoding='utf-8')

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {msg}"
    print(line)
    if _log_fh:
        _log_fh.write(line + '\n')
        _log_fh.flush()

def close_log():
    if _log_fh:
        _log_fh.close()


# ============================================================
# CSRF — 从skey cookie计算X-CSRF-TOKEN
# ============================================================

_csrf_token = ""
_server_time_offset_seconds = 0.0


def server_now() -> datetime:
    """返回按活动服务器时间校准后的本地时间。"""
    return datetime.fromtimestamp(time.time() + _server_time_offset_seconds)


def ordered_regions(available_region_ids: list) -> list:
    """按配置优先级排序，并保留活动接口新增的地域。"""
    available = list(dict.fromkeys(available_region_ids))
    available_set = set(available)
    prioritized = [
        region_id for region_id in REGION_PRIORITY if region_id in available_set
    ]
    return prioritized + [
        region_id for region_id in available if region_id not in REGION_PRIORITY
    ]


def region_batch(regions: list, batch_index: int) -> list:
    """从优先级列表轮换选取本轮地域，且不超过并发上限。"""
    if not regions:
        return []

    batch_size = min(max(1, MAX_PARALLEL_REGIONS), len(regions))
    start = (batch_index * batch_size) % len(regions)
    return [
        regions[(start + offset) % len(regions)]
        for offset in range(batch_size)
    ]

def calc_csrf_code(skey: str) -> int:
    """腾讯云getCsrfCode算法"""
    n = 5381
    for ch in skey:
        n += (n << 5) + ord(ch)
        n &= 0xFFFFFFFF
    return n & 0x7FFFFFFF

async def get_csrf_token(page) -> str:
    global _csrf_token

    # 优先用CDP读浏览器全部cookie(不依赖页面域, 任何页面都行)
    # 注意: 浏览器里新旧skey可能并存(如.dnspod.cn残留旧值),
    # 必须优先取.cloud.tencent.cn域的主skey, 否则算出错token导致CSRF-ERROR
    skey_val = ""
    try:
        cdp = await page.context.new_cdp_session(page)
        cookies_result = await cdp.send('Network.getAllCookies')
        await cdp.detach()
        all_cookies = cookies_result.get('cookies', [])
        # 优先级: .cloud.tencent.cn > .cloud.tencent.com > .qcloud.com > 任意
        for dom in ('cloud.tencent.cn', 'cloud.tencent.com', 'qcloud.com'):
            for cookie in all_cookies:
                if cookie.get('name') in ('skey', 'p_skey') and cookie.get('value') \
                   and dom in cookie.get('domain', ''):
                    skey_val = cookie['value']
                    break
            if skey_val:
                break
        if not skey_val:
            for cookie in all_cookies:
                if cookie.get('name') in ('skey', 'p_skey') and cookie.get('value'):
                    skey_val = cookie['value']
                    break
    except Exception as e:
        log(f"  CDP获取cookie失败: {e}")

    if skey_val:
        _csrf_token = str(calc_csrf_code(skey_val))
        log(f"  X-CSRF-TOKEN 从CDP获取skey计算: {_csrf_token}")
        return _csrf_token

    # 兜底: 页面evaluate读document.cookie(需腾讯域)
    try:
        skey_val = await page.evaluate("""
            () => {
                const cookies = document.cookie;
                const skeyMatch = cookies.match(/(?:^|;\\s*)skey=([^;]+)/);
                const pSkeyMatch = cookies.match(/(?:^|;\\s*)p_skey=([^;]+)/);
                return skeyMatch ? skeyMatch[1] : (pSkeyMatch ? pSkeyMatch[1] : '');
            }
        """)
    except Exception as e:
        log(f"  页面evaluate读cookie失败: {e}")
        skey_val = ""

    if skey_val:
        _csrf_token = str(calc_csrf_code(skey_val))
        log(f"  X-CSRF-TOKEN 从skey计算: {_csrf_token}")
        return _csrf_token

    log("  ⚠ 无法获取skey/p_skey!")
    return ""


async def setup_api_route(page):
    """注入X-CSRF-TOKEN后continue真发(所有act-api请求)。
    do-goods在mock模式由MOCK_FIRE_JS本地模拟, 不走网络, 无需拦截。"""
    log("设置CSRF头注入（X-CSRF-TOKEN + X-Requested-With）...")

    if not _csrf_token:
        log("  ⚠ 无CSRF token，API调用将失败!")

    async def inject_csrf(route, request):
        headers = {**request.headers}
        if _csrf_token:
            headers['X-CSRF-TOKEN'] = _csrf_token
        headers['X-Requested-With'] = 'XMLHttpRequest'
        await route.continue_(headers=headers)

    await page.route('**/act-api.cloud.tencent.com/**', inject_csrf)
    log(f"  CSRF注入路由已设置 (token={_csrf_token[:10]}...)")


# ============================================================
# API调用 — 浏览器内fetch
# ============================================================

async def api_fetch(page, url: str, body: dict) -> dict:
    result = await page.evaluate("""
        async ([url, body]) => {
            try {
                const resp = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                    credentials: 'include',
                });
                const status = resp.status;
                let data = null;
                try { data = await resp.json(); } catch(e) {}
                return { status, data, ok: resp.ok };
            } catch(e) {
                return { error: e.message };
            }
        }
    """, [url, body])
    return result


# ============================================================
# 连接Chrome
# ============================================================

CHROME_PROFILE = "/home/a1/chrome-cdp-profile"
ACTIVITY_PAGE_URL = "https://cloud.tencent.com/act/pro/double11-202611"

def _cdp_alive() -> bool:
    try:
        with urllib.request.urlopen(CDP_URL + "/json/version", timeout=2) as r:
            return r.status == 200
    except Exception:
        return False

def ensure_chrome():
    """若9222未启动, 自动拉起Chrome(带活动页URL), 等待就绪。"""
    if _cdp_alive():
        log("  Chrome 9222 已在运行")
        return True
    log("  Chrome 9222 未运行，自动启动...")
    for f in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        p = os.path.join(CHROME_PROFILE, f)
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass
    try:
        subprocess.Popen(
            ["/usr/bin/google-chrome",
             "--remote-debugging-port=9222",
             f"--user-data-dir={CHROME_PROFILE}",
             ACTIVITY_PAGE_URL],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:
        log(f"  启动Chrome失败: {e}")
        return False
    for _ in range(30):
        time.sleep(1)
        if _cdp_alive():
            log("  Chrome 已就绪")
            return True
    log("  ⚠ Chrome 启动超时")
    return False

async def connect_chrome():
    log("正在连接 Chrome CDP...")
    ensure_chrome()
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(CDP_URL)
    except Exception as e:
        log(f"无法连接 Chrome: {e}")
        log("  请确认 Chrome 已通过 --remote-debugging-port=9222 启动")
        await playwright.stop()
        sys.exit(1)

    context = browser.contexts[0] if browser.contexts else None
    if not context:
        log("没有浏览器上下文")
        await playwright.stop()
        sys.exit(1)

    pages = context.pages
    target_page = next((
        pg for pg in pages
        if "cloud.tencent.com/act/pro/" in pg.url
        and ("featured-" in pg.url or "#MS" in pg.url)
    ), None)
    if not target_page:
        target_page = next((
            pg for pg in pages if "cloud.tencent.com" in pg.url
        ), None)
    if not target_page:
        target_page = pages[0] if pages else None
    if not target_page:
        log("没有可用页面")
        await playwright.stop()
        sys.exit(1)

    log(f"已连接，当前页面: {target_page.url[:80]}")

    # 若页面不在腾讯域(如about:blank), 自动导航到活动页
    if "cloud.tencent.com" not in target_page.url:
        log("  页面不在腾讯域，自动导航到活动页...")
        try:
            await asyncio.wait_for(
                target_page.goto(
                    "https://cloud.tencent.com/act/pro/double11-202611",
                    wait_until="domcontentloaded", timeout=20000
                ),
                timeout=25
            )
            log(f"  导航完成: {target_page.url[:80]}")
        except Exception as e:
            log(f"  自动导航失败: {e}")

    # 恢复登录态: 若有备份文件, 全量写回浏览器(解决Chrome重启丢session cookie)
    if os.path.exists(COOKIE_BACKUP_FILE):
        try:
            import json as _json
            with open(COOKIE_BACKUP_FILE, "r") as f:
                backup_cookies = _json.load(f)
            cdp = await target_page.context.new_cdp_session(target_page)
            set_cnt = 0
            for ck in backup_cookies:
                try:
                    # 忽略已过期
                    if ck.get("expires", -1) and 0 < ck["expires"] < time.time():
                        continue
                    await cdp.send("Network.setCookie", {
                        "name": ck["name"], "value": ck["value"],
                        "domain": ck["domain"], "path": ck.get("path", "/"),
                        "secure": bool(ck.get("secure", False)),
                        "httpOnly": bool(ck.get("httpOnly", False)),
                        **({"expires": ck["expires"]} if ck.get("expires", -1) > 0 else {}),
                        **({"sameSite": ck["sameSite"]} if ck.get("sameSite") in ("Strict", "Lax", "None") else {}),
                    })
                    set_cnt += 1
                except Exception:
                    pass
            await cdp.detach()
            log(f"恢复登录态: 写回 {set_cnt} 条cookie (来自 {COOKIE_BACKUP_FILE})")
            # 刷新页面让UI读取新cookie(否则登录态要手动刷新才生效)
            if set_cnt > 0:
                try:
                    await asyncio.wait_for(
                        target_page.reload(wait_until="domcontentloaded", timeout=20000),
                        timeout=25
                    )
                    log("  已自动刷新页面, 登录态UI应已生效")
                except Exception as e:
                    log(f"  自动刷新页面失败(不影响API登录态): {e}")
        except Exception as e:
            log(f"恢复登录态失败: {e}")
    else:
        log(f"无登录态备份文件({COOKIE_BACKUP_FILE}), 跳过恢复")

    # 登录态真实校验: ① CDP读.cn纯数字uin ② 调check-available(腾讯端认不认)
    login_ok = False
    nick_shown = ""
    try:
        cdp = await target_page.context.new_cdp_session(target_page)
        cookies_result = await cdp.send('Network.getAllCookies')
        await cdp.detach()
        all_cookies = cookies_result.get('cookies', [])
        cn_uin = next((c['value'] for c in all_cookies
                       if c['name'] == 'uin' and 'cloud.tencent.cn' in c['domain']
                       and c['value'].isdigit()), None)
        if not cn_uin:
            log("⚠ 无 .cn 纯数字uin, 登录态无效(疑似微信态/未登录)")
        else:
            chk = await api_fetch(target_page,
                "https://act-api.cloud.tencent.com/dianshi/check-available",
                {"activity_id": ACTIVITY_ID,
                 "goods": [{"act_id": 1996855673581677, "region_id": [1]}],
                 "preview": 0})
            chk_code = (chk.get("data") or {}).get("code")
            if chk_code == 0:
                login_ok = True
                log("已登录: 登录态校验通过 (check-available code=0)")
            else:
                log(f"⚠ 登录态校验失败: check-available code={chk_code} msg={(chk.get('data') or {}).get('msg','')}")
    except Exception as e:
        log(f"登录检测异常: {e}")

    if not login_ok:
        log("⚠⚠ 当前未登录, 请先扫码登录后再跑! (否则抢购会返回 NOT-LOGINED)")

    return playwright, browser, context, target_page


# ============================================================
# imageId动态发现
# ============================================================

async def discover_image_ids(page) -> list:
    """从多个来源发现可用的Lighthouse镜像ID。
    返回策略列表: 当次发现的imageId优先，再追加历史回退值。

    do-goods省略imageId会返回500，因此不再把None作为下单策略。
    """
    strategies = []

    def add_image_id(image_id, source: str):
        if not isinstance(image_id, str) or not image_id.startswith("lhbp-"):
            return
        if image_id not in strategies:
            strategies.append(image_id)
            log(f"  ★ {source}发现imageId: {image_id}")
    
    # ---- 策略1: 从React fiber深度搜索 ----
    log("  发现imageId: 从React fiber搜索...")
    try:
        react_ids = await asyncio.wait_for(
            page.evaluate("""
                () => {
                    const found = [];
                    const seen = new WeakSet();
                    const cards = document.querySelectorAll('.uno3-buy-card');
                    for (const card of cards) {
                        const text = card.textContent || '';
                        if (!text.includes('轻量') && !text.includes('3.17')) continue;
                        const fiberKey = Object.keys(card).find(k =>
                            k.startsWith('__reactFiber$') || k.startsWith('__reactInternalInstance$'));
                        if (!fiberKey) continue;
                        let fiber = card[fiberKey];
                        for (let i = 0; i < 40 && fiber; i++) {
                            if (seen.has(fiber)) break;  // 防止循环引用
                            seen.add(fiber);
                            const props = fiber.memoizedProps || fiber.pendingProps || fiber.props;
                            if (props) {
                                const search = (obj, depth) => {
                                    if (depth > 12 || !obj || typeof obj !== 'object') return;
                                    if (seen.has(obj)) return;  // 防止循环引用
                                    try { seen.add(obj); } catch(e) {}
                                    try {
                                        for (const k of Object.keys(obj).slice(0, 50)) {
                                            const v = obj[k];
                                            if (typeof v === 'function') continue;
                                            if (k === 'imageId' || k === 'defaultImageId' || k === 'default_image_id') {
                                                if (typeof v === 'string' && v.length > 3 && v.length < 100) found.push(v);
                                            }
                                            if (k === 'imageIds' || k === 'images') {
                                                if (Array.isArray(v)) {
                                                    for (const item of v.slice(0, 20)) {
                                                        if (typeof item === 'string' && item.length > 3) found.push(item);
                                                        if (typeof item === 'object' && item && item.ImageId) found.push(item.ImageId);
                                                        if (typeof item === 'object' && item && item.imageId) found.push(item.imageId);
                                                    }
                                                }
                                            }
                                            if (typeof v === 'object' && !Array.isArray(v)) search(v, depth + 1);
                                        }
                                    } catch(e) {}
                                };
                                search(props, 0);
                            }
                            // 也搜hookState
                            let hookState = fiber.memoizedState;
                            for (let h = 0; h < 10 && hookState; h++) {
                                if (seen.has(hookState)) break;
                                try { seen.add(hookState); } catch(e) {}
                                const s = hookState.memoizedState;
                                if (s && typeof s === 'object') {
                                    const search2 = (obj, depth) => {
                                        if (depth > 8 || !obj || typeof obj !== 'object') return;
                                        try { seen.add(obj); } catch(e) {}
                                        try {
                                            for (const k of Object.keys(obj).slice(0, 30)) {
                                                if (typeof obj[k] === 'function') continue;
                                                if (k === 'imageId' || k === 'defaultImageId') {
                                                    if (typeof obj[k] === 'string' && obj[k].length > 3 && obj[k].length < 100) found.push(obj[k]);
                                                }
                                                if (typeof obj[k] === 'object' && !Array.isArray(obj[k])) search2(obj[k], depth + 1);
                                            }
                                        } catch(e) {}
                                    };
                                    search2(s, 0);
                                }
                                hookState = hookState.next;
                            }
                            fiber = fiber.return;
                        }
                    }
                    return [...new Set(found)];  // 去重
                }
            """),
            timeout=10.0
        )
    except asyncio.TimeoutError:
        log("  ⚠ React fiber搜索超时(10秒)，跳过")
        react_ids = []
    except Exception as e:
        log(f"  ⚠ React fiber搜索异常: {e}")
        react_ids = []
    if react_ids:
        for image_id in react_ids:
            add_image_id(image_id, "React fiber")
    
    # ---- 策略2: 从Lighthouse镜像API查询 ----
    log("  发现imageId: 从Lighthouse API查询...")
    region_api_map = {4: "ap-shanghai", 1: "ap-beijing", 8: "ap-guangzhou"}
    
    for region_id, region_api_name in region_api_map.items():
        try:
            lighthouse_result = await asyncio.wait_for(
                page.evaluate("""
                    async (regionName) => {
                        try {
                            const controller = new AbortController();
                            const timeout = setTimeout(() => controller.abort(), 5000);
                            const resp = await fetch('/lighthouse/api', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                credentials: 'include',
                                body: JSON.stringify({
                                    Action: 'DescribeImages',
                                    Region: regionName,
                                    ActionType: 'DescribeImages',
                                    Offset: 0,
                                    Limit: 100,
                                }),
                                signal: controller.signal,
                            });
                            clearTimeout(timeout);
                            if (!resp.ok) {
                                const resp2 = await fetch('/api/lighthouse/DescribeImages', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    credentials: 'include',
                                    body: JSON.stringify({ Region: regionName })
                                });
                                const data2 = await resp2.json();
                                return { source: 'alt', data: data2, status: resp2.status };
                            }
                            const data = await resp.json();
                            return { source: 'primary', data, status: resp.status };
                        } catch(e) {
                            return { error: e.message };
                        }
                    }
                """, region_api_name),
                timeout=8.0
            )
        except asyncio.TimeoutError:
            log(f"  ⚠ Lighthouse API超时(region={region_api_name})，跳过")
            continue
        except Exception as e:
            log(f"  ⚠ Lighthouse API异常(region={region_api_name}): {e}")
            continue
        
        if isinstance(lighthouse_result, dict) and not lighthouse_result.get("error"):
            data = lighthouse_result.get("data", {})
            # 提取ImageId字段
            def extract_image_ids(obj, depth=0):
                if depth > 10 or not obj or not isinstance(obj, dict):
                    return []
                ids = []
                if "Images" in obj and isinstance(obj["Images"], list):
                    for img in obj["Images"]:
                        iid = img.get("ImageId", "")
                        if iid:
                            ids.append(iid)
                if "Data" in obj and isinstance(obj["Data"], dict):
                    ids.extend(extract_image_ids(obj["Data"], depth + 1))
                return ids
            
            discovered = extract_image_ids(data)
            # 只保留lhbp开头的(轻量应用镜像)
            lhbp_ids = [id for id in discovered if id.startswith("lhbp")]
            for image_id in lhbp_ids:
                add_image_id(
                    image_id,
                    f"Lighthouse API(region={region_api_name})",
                )
    
    # ---- 策略3: 从网络请求拦截 ----
    # 在整点前的页面交互中，如果用户点了购买弹窗，
    # 我们可以拦截do-goods请求提取imageId
    # (这里只是预留，实际在抢购时动态捕获)
    
    # 当次发现失败时使用历史值。若已失效，接口会返回1100109并切换下一个策略。
    for image_id in HISTORICAL_IMAGE_IDS:
        if image_id not in strategies:
            strategies.append(image_id)
            log(f"  ★ 历史回退imageId: {image_id}")
    
    log(f"  imageId策略列表: {strategies}")
    return strategies


# ============================================================
# 查询秒杀商品
# ============================================================

async def query_seckill_by_date(page, activity_id) -> list:
    result = await api_fetch(page,
        "https://act-api.cloud.tencent.com/dianshi/query-seckill-by-date",
        {
            "activity_id": activity_id,
            "preview": 0,
            "seckill_type": "block",
            "goods_type": "goods",
            "days": 2,
        }
    )

    if result.get("error"):
        log(f"  query-seckill-by-date 错误: {result['error']}")
        return []

    data = result.get("data", {})
    if not data:
        log(f"  query-seckill-by-date 无数据, status={result.get('status')}")
        return []

    code = data.get("code", -1)
    msg = data.get("msg", data.get("message", ""))
    if code != 0:
        log(f"  query-seckill-by-date code={code}, msg={msg}")
        return []

    log(f"  query-seckill-by-date OK ✓")

    raw_data = data.get("data", {})
    all_goods = []

    def extract_goods(obj, depth=0):
        if depth > 15:
            return
        if isinstance(obj, dict):
            if "act_id" in obj and "product_name" in obj:
                all_goods.append(obj)
            for v in obj.values():
                extract_goods(v, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                extract_goods(item, depth + 1)

    extract_goods(raw_data)

    deduplicated = {}
    for goods in all_goods:
        act_id = goods.get("act_id")
        key = act_id if act_id is not None else id(goods)
        existing = deduplicated.get(key)
        existing_limits = (
            existing.get("$goods", {}).get("limits", {})
            if isinstance(existing, dict) else {}
        )
        current_limits = goods.get("$goods", {}).get("limits", {})
        if existing is None or (not existing_limits and current_limits):
            deduplicated[key] = goods
    all_goods = list(deduplicated.values())

    log(f"  找到 {len(all_goods)} 个商品")

    lighthouse_goods = []
    for g in all_goods:
        gtype = g.get("product_name", "")
        gid = g.get("act_id", "?")
        gname = g.get("goods_name", gtype)
        regions = []
        price = g.get("price", g.get("seckill_price", g.get("real_cost", "")))

        limits = g.get("$goods", {}).get("limits", {})
        region_limit = limits.get("regionId", {})
        if isinstance(region_limit, dict):
            constraint = region_limit.get("constraint", {})
            if isinstance(constraint, dict):
                for area_val, region_list in constraint.get("value", {}).items():
                    if isinstance(region_list, list):
                        regions = region_list

        if isinstance(gtype, str) and "lighthouse" in gtype.lower():
            lighthouse_goods.append(g)
            log(f"  ★ 轻量服务器: act_id={gid}, regions={regions}")
            
            goods_limits = g.get("$goods", {}).get("limits", {})
            if goods_limits:
                log(f"    limits: {json.dumps(goods_limits, ensure_ascii=False)[:300]}")

    return lighthouse_goods if lighthouse_goods else all_goods


def extract_goods_config(goods_list: list) -> dict | None:
    for item in goods_list:
        if not isinstance(item, dict):
            continue
        product_name = item.get("product_name", "")
        if not isinstance(product_name, str) or "lighthouse" not in product_name.lower():
            continue

        goods = item.get("$goods", {})
        limits = goods.get("limits", {}) if isinstance(goods, dict) else {}
        type_info = limits.get("type", {}) if isinstance(limits, dict) else {}
        region_info = limits.get("regionId", {}) if isinstance(limits, dict) else {}

        goods_type = ""
        constraint = type_info.get("constraint", {}) if isinstance(type_info, dict) else {}
        if isinstance(constraint, dict):
            for type_list in constraint.get("value", {}).values():
                if isinstance(type_list, list) and type_list:
                    goods_type = type_list[0]
                    break

        region_ids = []
        constraint = region_info.get("constraint", {}) if isinstance(region_info, dict) else {}
        if isinstance(constraint, dict):
            for region_list in constraint.get("value", {}).values():
                if isinstance(region_list, list) and region_list:
                    region_ids = region_list
                    break

        return {
            "activity_id": item.get("activity_id") or goods.get("activity_id"),
            "act_id": item.get("act_id") or goods.get("act_id"),
            "available": item.get("available"),
            "goods_limits": limits,
            "goods_type": goods_type,
            "region_ids": region_ids,
        }
    return None


# ============================================================
# check-available
# ============================================================

async def check_available(page, activity_id, act_id, region_ids: list) -> dict:
    result = await api_fetch(page,
        "https://act-api.cloud.tencent.com/dianshi/check-available",
        {
            "activity_id": activity_id,
            "goods": [{"act_id": act_id, "region_id": region_ids}],
            "preview": 0,
        }
    )
    return result


# ============================================================
# get-now
# ============================================================

async def get_server_time(page) -> int:
    """对时: get-now 采样 7 次，取往返最快那次（网络抖动只会变慢），
    与其相差 20ms 内的取中位数。比单次折半准得多（可到 ±20ms 内）。
    """
    global _server_time_offset_seconds

    samples = []
    for _ in range(7):
        req = time.time()
        try:
            result = await api_fetch(page,
                "https://act-api.cloud.tencent.com/dianshi/get-now", {})
        except Exception:
            result = {}
        rsp = time.time()
        data = result.get("data", {})
        if data and data.get("code") == 0:
            server_ts = data.get("data", 0)
            if server_ts:
                samples.append({"rtt": rsp - req, "req": req, "rsp": rsp,
                                "server": server_ts})
        await asyncio.sleep(0.05)

    if not samples:
        log("  ⚠ 对时失败（get-now 无有效采样）")
        return 0

    samples.sort(key=lambda s: s["rtt"])
    fastest = samples[0]
    close = [s for s in samples if s["rtt"] - fastest["rtt"] <= 0.02]
    close.sort(key=lambda s: s["server"] - (s["req"] + s["rsp"]) / 2)
    med = close[len(close) // 2]
    midpoint = (med["req"] + med["rsp"]) / 2
    _server_time_offset_seconds = med["server"] / 1000 - midpoint
    log(
        f"  服务器时间: {med['server']} (get-now×{len(samples)}, "
        f"最快往返{fastest['rtt'] * 1000:.0f}ms, "
        f"偏差{_server_time_offset_seconds * 1000:+.0f}ms)"
    )
    return med["server"]


# ============================================================
# prepare-do
# ============================================================

async def prepare_do(page, activity_id, act_id) -> dict:
    t0 = time.time()
    result = await api_fetch(page,
        "https://act-api.cloud.tencent.com/dianshi/prepare-do",
        {
            "activity_id": activity_id,
            "act_id": act_id,
            "preview": 0,
            "from_channel": "",
            "agent_key": "",
        }
    )
    elapsed = (time.time() - t0) * 1000

    if result.get("error"):
        log(f"  ✗ prepare-do 异常({elapsed:.0f}ms): {result['error']}")
        return {"success": False, "error": result["error"]}

    data = result.get("data", {})
    code = data.get("code", -1) if data else -1
    msg = data.get("msg", data.get("message", "")) if data else ""

    if result.get("status") == 200 and code == 0:
        log(f"  ✓ prepare-do 成功({elapsed:.0f}ms) act_id={act_id}")
        return {"success": True, "data": data}
    else:
        log(f"  ✗ prepare-do 失败({elapsed:.0f}ms): code={code}, msg={msg}")
        return {"success": False, "code": code, "msg": msg}


# ============================================================
# do-goods — 单地域(保留用于dry-run)
# ============================================================

async def do_goods(page, activity_id, act_id, region_id: int,
                   image_id=None, goods_type: str = "") -> dict:
    """发送带有效Lighthouse镜像ID的do-goods下单请求。"""
    if not isinstance(image_id, str) or not image_id.startswith("lhbp-"):
        log("  ⚠ 跳过do-goods：缺少有效的lhbp-* imageId")
        return {
            "success": False,
            "error": "missing valid imageId",
            "region_id": region_id,
        }

    goods_param = {
        "zone": "",
        "timeSpanUnit": "12m",
        "scenario": "0",
        "goodsNum": 1,
        "ddocUnionConnect": 0,
        "area": 1,
        "BlueprintId": "LINUX_UNIX",
        "regionId": region_id,
        "imageId": image_id,
    }
    if goods_type:
        goods_param["type"] = goods_type

    body = {
        "activity_id": activity_id,
        "goods": [{
            "act_id": act_id,
            "type": "lighthouse_v5",
            "goods_param": goods_param,
            "page_from": "act_buy",
        }],
        "preview": 0,
        "agent_channel": {
            "fromChannel": "",
            "fromSales": "",
            "isAgentClient": False,
            "fromUrl": page.url,
            "agent_key": "",
        },
    }

    t0 = time.time()
    result = await api_fetch(page,
        "https://act-api.cloud.tencent.com/dianshi/do-goods",
        body
    )
    elapsed = (time.time() - t0) * 1000

    if result.get("error"):
        log(f"  ✗ do-goods 异常({elapsed:.0f}ms): {result['error']}")
        return {"success": False, "error": result["error"], "elapsed_ms": elapsed}

    data = result.get("data", {})
    code = data.get("code", -1) if data else -1
    msg = data.get("msg", data.get("message", "")) if data else ""
    region_name = REGIONS.get(region_id, f"region{region_id}")

    if code == 0:
        log(f"  ★★★ do-goods 成功! {region_name}({region_id}) ({elapsed:.0f}ms) ★★★")
        return {"success": True, "data": data, "elapsed_ms": elapsed,
                "region_id": region_id}
    elif code == 408:
        log(f"  ✗ do-goods 408限流 {region_name}({region_id}) ({elapsed:.0f}ms)")
        return {"success": False, "code": 408, "msg": msg,
                "elapsed_ms": elapsed, "rate_limited": True,
                "region_id": region_id}
    elif code == 1100136:
        log(f"  ✗ do-goods 售罄 {region_name}({region_id}) ({elapsed:.0f}ms)")
        return {"success": False, "code": 1100136, "msg": msg,
                "elapsed_ms": elapsed, "sold_out": True,
                "region_id": region_id}
    elif code == 1100109:
        log(f"  ✗ do-goods 镜像不存在 {region_name}({region_id}): {msg} ({elapsed:.0f}ms)")
        return {"success": False, "code": 1100109, "msg": msg,
                "elapsed_ms": elapsed, "wrong_image": True,
                "region_id": region_id}
    else:
        log(f"  ✗ do-goods 失败 {region_name}({region_id}): code={code}, msg={msg} ({elapsed:.0f}ms)")
        return {"success": False, "code": code, "msg": msg,
                "elapsed_ms": elapsed, "region_id": region_id}


# ============================================================
# do-goods batch — 三地域并行(Promise.all)
# ============================================================

async def do_goods_batch(page, activity_id, act_id, region_ids: list,
                         image_id=None, goods_type: str = "") -> list:
    """并行发送多个地域的do-goods请求。
    用Promise.all在浏览器内同时发3个fetch，比串行快3倍。
    """
    if not isinstance(image_id, str) or not image_id.startswith("lhbp-"):
        log("  ⚠ 跳过do-goods：缺少有效的lhbp-* imageId")
        return [
            {
                "success": False,
                "error": "missing valid imageId",
                "region_id": region_id,
            }
            for region_id in region_ids
        ]

    # 构建每个地域的请求体
    requests_data = []
    for region_id in region_ids:
        goods_param = {
            "zone": "",
            "timeSpanUnit": "12m",
            "scenario": "0",
            "goodsNum": 1,
            "ddocUnionConnect": 0,
            "area": 1,
            "BlueprintId": "LINUX_UNIX",
            "regionId": region_id,
        }
        goods_param["imageId"] = image_id
        if goods_type:
            goods_param["type"] = goods_type

        body = {
            "activity_id": activity_id,
            "goods": [{
                "act_id": act_id,
                "type": "lighthouse_v5",
                "goods_param": goods_param,
                "page_from": "act_buy",
            }],
            "preview": 0,
            "agent_channel": {
                "fromChannel": "",
                "fromSales": "",
                "isAgentClient": False,
                "fromUrl": page.url,
                "agent_key": "",
            },
        }
        requests_data.append({
            "url": "https://act-api.cloud.tencent.com/dianshi/do-goods",
            "body": body,
            "regionId": region_id,
        })

    t0 = time.time()
    
    # Promise.all: 所有fetch同时发出
    raw_results = await page.evaluate("""
        async (requestsData) => {
            const results = await Promise.all(requestsData.map(rd =>
                fetch(rd.url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(rd.body),
                    credentials: 'include',
                }).then(async resp => {
                    const status = resp.status;
                    let data = null;
                    try { data = await resp.json(); } catch(e) {}
                    return { regionId: rd.regionId, status, data, ok: resp.ok };
                }).catch(e => ({ regionId: rd.regionId, error: e.message }))
            ));
            return results;
        }
    """, requests_data)
    
    elapsed = (time.time() - t0) * 1000
    log(f"  batch完成 ({elapsed:.0f}ms, {len(region_ids)}地域并行)")

    # 解析结果
    parsed = []
    for r in raw_results:
        region_id = r.get("regionId")
        region_name = REGIONS.get(region_id, f"region{region_id}")
        
        if r.get("error"):
            log(f"    ✗ {region_name}({region_id}): 异常 {r['error']}")
            parsed.append({"success": False, "error": r["error"],
                           "region_id": region_id})
            continue
        
        data = r.get("data", {})
        code = data.get("code", -1) if data else -1
        msg = data.get("msg", data.get("message", "")) if data else ""
        
        if code == 0:
            log(f"    ★★★ {region_name}({region_id}): 成功! ★★★")
            parsed.append({"success": True, "data": data,
                           "region_id": region_id})
        elif code == 1100109:
            log(f"    ✗ {region_name}({region_id}): 镜像不存在(code=1100109)")
            parsed.append({"success": False, "code": 1100109, "msg": msg,
                           "wrong_image": True, "region_id": region_id})
        elif code == 1100136:
            log(f"    ✗ {region_name}({region_id}): 售罄")
            parsed.append({"success": False, "code": 1100136, "msg": msg,
                           "sold_out": True, "region_id": region_id})
        elif code == 408:
            log(f"    ✗ {region_name}({region_id}): 408限流")
            parsed.append({"success": False, "code": 408, "msg": msg,
                           "rate_limited": True, "region_id": region_id})
        else:
            log(f"    ✗ {region_name}({region_id}): code={code}, msg={msg}")
            parsed.append({"success": False, "code": code, "msg": msg,
                           "region_id": region_id})
    
    return parsed


# ============================================================
# 上游(djs-91)快速连续发送: 页面内FIRE_JS + 探针→全速
# 不开并发上限30, 回来一个补一个; 408/1100205继续, 1100136售罄停
# ============================================================

FIRE_JS = """
(params) => {
    const st = window.__sk || (window.__sk = {
        inflight: 0, fired: 0, done: 0, codes: {}, msgs: {},
        hit: null, fatal: null, stopped: false, last: null,
    });
    if (params.reset) {
        st.inflight = 0; st.fired = 0; st.done = 0;
        st.codes = {}; st.msgs = {};
        st.hit = null; st.fatal = null; st.stopped = false; st.last = null;
    }
    if (params.stop) st.stopped = true;

    const body = JSON.stringify({
        activity_id: params.activity_id,
        goods: [{act_id: params.act_id, type: params.type,
                 goods_param: params.goods_param, page_from: "act_buy"}],
        preview: params.preview,
        agent_channel: {fromChannel: '', fromSales: '', isAgentClient: false,
                        fromUrl: location.href, agent_key: ''},
    });

    let n = 0;
    while (n < params.batch && st.inflight < params.cap && !st.stopped && !st.hit && !st.fatal) {
        st.inflight++; st.fired++; n++;
        fetch(params.url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            credentials: 'include',
            body: body,
        }).then(r => r.json()).then(d => {
            st.inflight--; st.done++;
            const c = (d && d.code !== undefined && d.code !== null) ? d.code : -1;
            st.last = c;
            st.codes[c] = (st.codes[c] || 0) + 1;
            if (d && d.msg) st.msgs[c] = String(d.msg).slice(0, 60);
            if (c === params.ok && !st.hit) st.hit = d;
            if (params.norety.indexOf(c) >= 0 && !st.fatal) st.fatal = c;
        }).catch(e => {
            st.inflight--; st.done++;
            st.last = -1;
            st.codes[-1] = (st.codes[-1] || 0) + 1;
            if (!st.msgs[-1]) st.msgs[-1] = String((e && e.message) || e).slice(0, 60);
        });
    }
    return {fired: st.fired, inflight: st.inflight, done: st.done,
            codes: st.codes, msgs: st.msgs, hit: st.hit, fatal: st.fatal,
            last: st.last};
}
"""


MOCK_FIRE_JS = """
(params) => {
    const st = window.__skm || (window.__skm = {
        fired: 0, codes: {}, msgs: {}, hit: null, fatal: null, last: null,
    });
    if (params.reset) {
        st.fired = 0; st.codes = {}; st.msgs = {};
        st.hit = null; st.fatal = null; st.last = null;
    }
    const seq = [1101001, 408, 408, 0];   // 探针未开始 → 408×2 → 命中
    const msgs = {1101001: "抱歉，秒杀尚未开始", 408: "系统访问频率过快，请稍后再试"};
    const n = params.batch || 1;
    for (let i = 0; i < n; i++) {
        const c = seq[st.fired % seq.length];
        st.fired++;
        st.codes[c] = (st.codes[c] || 0) + 1;
        if (!st.msgs[c] && msgs[c]) st.msgs[c] = msgs[c];
        if (c === 0 && !st.hit) st.hit = {code: 0, msg: "ok",
            data: {deal_names: ["MOCK-ORDER-20261103"], big_deal_no: "BIG-MOCK-001"}};
        if ([1100106, 1100107, 1100109, 1100136].indexOf(c) >= 0 && !st.fatal) st.fatal = c;
        st.last = c;
    }
    return {fired: st.fired, inflight: 0, done: st.fired,
            codes: st.codes, msgs: st.msgs, hit: st.hit, fatal: st.fatal, last: st.last};
}
"""


async def rapid_purchase(page, goods, duration=DURATION_SEC, open_in=None, mock=False):
    """上游策略: 开窗前每200ms探针, 最后0.6秒全速(30并发回来补一)。
    open_in: 距开窗秒(None=直接全速)。408/1100205继续, 售罄/资格/字符串码停。
    mock=True: 用MOCK_FIRE_JS纯本地模拟, 零网络(不发fetch), 不出网。"""
    open_at = None if open_in is None else time.monotonic() + open_in
    ramp_at = None if open_at is None else open_at - PRE_OPEN_FIRE_MS / 1000
    ramped = open_at is None or open_in <= 0
    if not ramped:
        log(f"距开窗{open_in:.1f}s, 先每{PROBE_MS}ms一条探针, "
            f"最后{PRE_OPEN_FIRE_MS/1000:g}秒全速")
    log(f"开始抢购: 同时最多{INFLIGHT}条, 每{POLL_MS}ms看一次, 持续{duration}s")

    params = {
        "url": DO_GOODS_URL,
        "activity_id": goods["activity_id"],
        "act_id": goods["act_id"],
        "type": goods["type"],
        "goods_param": goods.get("goods_param", {}),
        "preview": 0,
        "cap": int(INFLIGHT),
        "batch": int(INFLIGHT),
        "ok": CODE_OK,
        "norety": sorted(NO_RETRY_CODES),
        "reset": True,
        "stop": False,
    }

    start = time.monotonic()
    success = False
    last_code = None
    seen_codes = {}
    fired = 0
    err_count = 0
    next_progress = start + 2
    result = {}

    while time.monotonic() - start < duration:
        now = time.monotonic()
        if not ramped and now >= ramp_at:
            ramped = True
            params["cap"] = int(INFLIGHT)
            params["batch"] = int(INFLIGHT)
            left = open_at - now
            if left > 0:
                log(f"  进入最后{PRE_OPEN_FIRE_MS/1000:g}秒, 全速发(距开窗{left:.2f}s)")
            else:
                log("  开窗了, 全速发")
        elif not ramped:
            params["cap"] = 1
            params["batch"] = 1
        params["reset"] = False
        try:
            if mock:
                result = await page.evaluate(MOCK_FIRE_JS, params) or {}
            else:
                result = await page.evaluate(FIRE_JS, params) or {}
        except Exception as e:
            err_count += 1
            if err_count <= 3:
                log(f"  发射失败: {str(e)[:80]}")
            await asyncio.sleep(POLL_MS / 1000)
            continue

        fired = int(result.get("fired") or 0)
        msgs = result.get("msgs") or {}
        bad_code = None
        for key, cnt in (result.get("codes") or {}).items():
            try:
                code = int(key)
            except Exception:
                code = key
            if code == CODE_OK:
                continue
            if code in seen_codes:
                continue
            seen_codes[code] = cnt
            if isinstance(code, str) and code not in ("undefined", "null"):
                desc = "不是抢购结果码, 登录态可能失效了"
            else:
                desc = CODE_DESC.get(code, "未知返回码, 程序会继续")
            log(f"  返回 {code}: {desc} | {msgs.get(key, '')}")
            if isinstance(code, str) and code not in ("undefined", "null"):
                bad_code = code

        if result.get("last") is not None:
            last_code = result["last"]

        hit = result.get("hit")
        if hit:
            deal = hit.get("data") or {}
            log("下单成功")
            log(f"   订单号: {(deal.get('deal_names') or ['?'])[0]}")
            log(f"   大单号: {deal.get('big_deal_no') or '?'}")
            success = True
            break

        fatal = result.get("fatal")
        if fatal is not None:
            if fatal == CODE_SOLD_OUT:
                log("  商品已售罄, 本场没有名额了, 停止。")
            elif fatal in (CODE_ALREADY_BOUGHT, CODE_NO_QUALIFY):
                log(f"  账号这边过不去({CODE_DESC.get(fatal, '')}), 本场停止。")
            else:
                log("  这类错误重试没有意义, 本场停止。")
            last_code = fatal
            break

        if bad_code is not None:
            log("  本场停止, 重新扫码登录再来。")
            last_code = bad_code
            break

        now = time.monotonic()
        if now >= next_progress:
            next_progress = now + 2
            rate = fired / max(0.001, now - start)
            log(f"  已发{fired}条({rate:.0f}/秒), 还在等{int(result.get('inflight') or 0)}条 "
                f"({now - start:.1f}s)")

        if ramped:
            await asyncio.sleep(POLL_MS / 1000)
        else:
            await asyncio.sleep(min(PROBE_MS / 1000, max(0.005, ramp_at - time.monotonic())))

    # 收尾: 停发, 等最后半秒的成功返回(服务端按账号限1, 不会重复下单)
    try:
        params["stop"] = True
        deadline = time.monotonic() + 2.5
        while True:
            if mock:
                result = await page.evaluate(MOCK_FIRE_JS, params) or {}
            else:
                result = await page.evaluate(FIRE_JS, params) or {}
            if result.get("hit") and not success:
                deal = (result["hit"].get("data") or {})
                log("下单成功")
                log(f"   订单号: {(deal.get('deal_names') or ['?'])[0]}")
                log(f"   大单号: {deal.get('big_deal_no') or '?'}")
                success = True
            if success or int(result.get("inflight") or 0) == 0 or time.monotonic() > deadline:
                break
            await asyncio.sleep(0.1)
        fired = int(result.get("fired") or fired)
    except Exception:
        pass

    elapsed = time.monotonic() - start
    log(f"抢购结束: {fired}次请求, {elapsed:.1f}s, {'成功' if success else '未抢到'}")
    tally = result.get("codes") or {}
    if tally:
        def _label(k):
            try:
                k = int(k)
            except Exception:
                pass
            if k == CODE_OK:
                return "成功"
            if k == -1:
                return "没发出去"
            return str(k)
        parts = []
        for k, cnt in sorted(tally.items(), key=lambda x: str(x[0])):
            parts.append(f"{_label(k)}×{cnt}")
        log("  各码次数: " + ", ".join(parts))
    return success


# ============================================================
# 从React fiber中深度提取act_id
# ============================================================

async def extract_act_id_from_react(page) -> dict:
    result = await page.evaluate("""
        () => {
            const cards = document.querySelectorAll('.uno3-buy-card');
            const findings = [];

            for (const card of cards) {
                const text = card.textContent || '';
                if (!text.includes('轻量') && !text.includes('3.17')) continue;

                const fiberKey = Object.keys(card).find(k =>
                    k.startsWith('__reactFiber$') || k.startsWith('__reactInternalInstance$'));

                let fiberData = null;
                if (fiberKey) {
                    let fiber = card[fiberKey];
                    for (let i = 0; i < 30 && fiber; i++) {
                        const props = fiber.memoizedProps || fiber.pendingProps || fiber.props;
                        if (props) {
                            for (const key of ['goods', 'item', 'product', 'data',
                                               'seckillGoods', 'seckillItem', 'goodsItem',
                                               'goodsInfo', 'productInfo']) {
                                const val = props[key];
                                if (val && typeof val === 'object' && !Array.isArray(val)) {
                                    if (val.act_id || val.goods_id) {
                                        fiberData = {};
                                        for (const k of Object.keys(val)) {
                                            if (typeof val[k] !== 'function') {
                                                try {
                                                    fiberData[k] = typeof val[k] === 'object' ? JSON.stringify(val[k]).substring(0, 200) : val[k];
                                                } catch(e) { fiberData[k] = '(error)'; }
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                            if (fiberData) break;
                            let hookState = fiber.memoizedState;
                            for (let h = 0; h < 10 && hookState; h++) {
                                const s = hookState.memoizedState;
                                if (s && typeof s === 'object' && (s.act_id || s.goods_id)) {
                                    fiberData = { from: 'hookState', act_id: s.act_id, goods_id: s.goods_id };
                                    break;
                                }
                                hookState = hookState.next;
                            }
                            if (fiberData) break;
                        }
                        fiber = fiber.return;
                    }
                }

                findings.push({
                    cardText: text.substring(0, 80).replace(/\\s+/g, ' ').trim(),
                    fiberData,
                });
            }

            return findings;
        }
    """)
    return result


# ============================================================
# 动态地域: 从活动页 HTML 抓 region_id → 地域名
# ============================================================

async def discover_regions(page, act_id) -> dict:
    """从活动页抓取目标商品的 regionMap（region_id → 地域名）。

    活动页 HTML 里每个商品条目有:
      {"value":X,"label":"地域名","regionEn":"ap-xxx"}
    必须按 act_id 限定条目（避免串入相邻商品如境外套餐）。
    region_id 每期活动可能重分配，不能写死。
    """
    try:
        html = await page.content()
    except Exception:
        return {}
    entries = list(re.finditer(r'"act_id":(\d+),"goods_name":"', html))
    found = {}
    for i, e in enumerate(entries):
        if e.group(1) != str(act_id):
            continue
        end = entries[i + 1].start() if i + 1 < len(entries) else e.start() + 20000
        window = html[e.start():end]
        for m in re.finditer(
            r'\{"value":(\d+),"label":"([^"]*)","regionEn":"([^"]*)"\}', window
        ):
            rid = int(m.group(1))
            if rid not in found:
                found[rid] = m.group(2)
        break
    return found


# ============================================================
# 主流程
# ============================================================

async def main():
    dry_run = "--dry-run" in sys.argv
    run_now = "--now" in sys.argv
    mock_mode = "--mock" in sys.argv
    backup_login = "--backup-login" in sys.argv

    if backup_login:
        # 登录后立即备份全量cookies(解决Chrome重启丢session cookie)
        playwright = await async_playwright().start()
        browser = await playwright.chromium.connect_over_cdp(CDP_URL)
        context = browser.contexts[0] if browser.contexts else None
        if not context:
            print("无浏览器上下文")
            await playwright.stop()
            sys.exit(1)
        page = next((pg for pg in context.pages if "cloud.tencent.com" in pg.url), None) or context.pages[0]
        cdp = await context.new_cdp_session(page)
        cr = await cdp.send("Network.getAllCookies")
        await cdp.detach()
        cookies = cr.get("cookies", [])
        with open(COOKIE_BACKUP_FILE, "w") as f:
            json.dump(cookies, f, indent=2)
        # 统计
        cn_uin = next((c["value"] for c in cookies
                       if c["name"] == "uin" and "cloud.tencent.cn" in c["domain"]
                       and c["value"].isdigit()), None)
        skey_cnt = sum(1 for c in cookies if c["name"] in ("skey", "p_skey") and c.get("value"))
        print(f"已备份 {len(cookies)} 条cookie -> {COOKIE_BACKUP_FILE}")
        print(f"  .cn纯数字uin: {cn_uin or '(无)'}  skey条数: {skey_cnt}")
        print("  登录后立即跑 --backup-login, 之后每次启动会自动恢复登录态")
        await browser.close()
        await playwright.stop()
        sys.exit(0)

    _open_log()
    log("=" * 60)
    log("  腾讯云秒杀 — v20 纯API版")
    log(
        "  策略: prepare-do+do-goods并行 + imageId动态发现 + "
        f"每批最多{MAX_PARALLEL_REGIONS}地域"
    )
    if dry_run:
        log("  模式: --dry-run (只查询不购买)")
    elif run_now:
        log("  模式: --now (立即测试)")
    elif mock_mode:
        log("  模式: --mock (模拟, 不出网)")
    log(f"  日志文件: {LOG_FILE}")
    log("=" * 60)

    playwright, browser, context, page = await connect_chrome()

    # ---- 获取CSRF token ----
    log("获取X-CSRF-TOKEN...")
    await get_csrf_token(page)

    # ---- 设置CSRF头注入 ----
    await setup_api_route(page)

    # ---- 真实登录态校验 (token已就位, 能过CSRF) ----
    log("校验登录态(腾讯端)...")
    try:
        chk = await check_available(page, ACTIVITY_ID, 1996855673581677, [1])
        chk_code = (chk.get("data") or {}).get("code")
        if chk_code == 0:
            log("  ✓ 登录态有效 (check-available code=0)")
        else:
            log(f"  ⚠ 登录态无效: code={chk_code} msg={(chk.get('data') or {}).get('msg','')}")
            log("  ⚠⚠ 未登录! 请重新扫码登录后再跑, 否则抢购会返回 NOT-LOGINED")
            await playwright.stop()
            sys.exit(1)
    except Exception as e:
        log(f"  登录态校验异常: {e}")

    # 从React fiber优先提取当前页面的activity_id和act_id
    log("从React fiber提取活动参数...")
    react_findings = await extract_act_id_from_react(page)
    activity_id = ACTIVITY_ID
    react_act_id = None
    for f in react_findings:
        fd = f.get('fiberData') or {}
        if not isinstance(fd, dict):
            continue
        if fd.get("activity_id"):
            activity_id = int(fd["activity_id"])
        if fd.get("act_id"):
            react_act_id = int(fd["act_id"])
        if react_act_id:
            log(f"  ★ React fiber: activity_id={activity_id}, act_id={react_act_id}")
            break

    log(f"使用activity_id={activity_id}")

    # ---- 查询商品信息 ----
    log("查询秒杀商品信息...")
    goods_list = await query_seckill_by_date(page, activity_id)
    goods_config = extract_goods_config(goods_list)

    # 获取服务器时间
    log("获取服务器时间...")
    await get_server_time(page)

    target_act_id = None
    goods_limits = {}
    goods_type = ""
    region_ids = []

    if goods_config:
        activity_id = int(goods_config.get("activity_id") or activity_id)
        target_act_id = goods_config.get("act_id")
        goods_limits = goods_config.get("goods_limits") or {}
        goods_type = goods_config.get("goods_type") or ""
        region_ids = goods_config.get("region_ids") or []
        log(
            f"  [API] 选定: activity_id={activity_id}, "
            f"act_id={target_act_id}, type={goods_type}, "
            f"regions={region_ids}, available={goods_config.get('available')}"
        )
        if goods_limits:
            log(f"  [API] limits: {json.dumps(goods_limits, ensure_ascii=False)[:300]}")

    if not target_act_id and react_act_id:
        target_act_id = react_act_id
        log(f"  [React] 选定: act_id={react_act_id}")

    if not target_act_id:
        log("⚠ 未获取到act_id!")

    if goods_limits:
        image_id_info = goods_limits.get("imageId", {})
        log(f"  imageId配置: {image_id_info} (将动态发现，不硬编码)")

    # ---- 动态地域映射（覆盖写死的 REGIONS/REGION_PRIORITY）----
    if target_act_id:
        try:
            dyn = await discover_regions(page, target_act_id)
            if dyn:
                REGIONS.clear()
                REGIONS.update(dyn)
                pref = [r for r in (1, 4, 8) if r in dyn]
                REGION_PRIORITY[:] = pref + [r for r in dyn if r not in pref]
                log(f"  ★ 动态地域映射: {REGIONS} (优先级 {REGION_PRIORITY})")
            else:
                log(f"  ⚠ 动态地域未匹配到，用默认 {REGIONS}")
        except Exception as e:
            log(f"  ⚠ 动态地域获取异常({e})，用默认 {REGIONS}")

    # ---- 发现imageId ----
    log("动态发现可用imageId...")
    image_id_strategies = await discover_image_ids(page)

    # ---- dry-run ----
    if dry_run:
        log("")
        log("=== DRY-RUN: 只查询不下单 ===")

        if target_act_id:
            check_regions = region_ids or REGION_PRIORITY
            for rid in check_regions:
                rname = REGIONS.get(rid, f"region{rid}")
                log(f"  检查 {rname}({rid})...")
                avail = await check_available(page, activity_id, target_act_id, [rid])
                log(f"  结果: {json.dumps(avail, ensure_ascii=False)[:300]}")
        else:
            log("  无act_id，无法测试")

        log("  dry-run未发送prepare-do或do-goods下单请求")
        log("=== DRY-RUN 完成 ===")
        await playwright.stop()
        close_log()
        return

    missing_config = []
    if not target_act_id:
        missing_config.append("act_id")
    if not goods_type:
        missing_config.append("goods_type")
    if not goods_limits:
        missing_config.append("goods_limits")
    if not region_ids:
        missing_config.append("region_ids")
    if missing_config:
        log(f"⚠ 商品配置不完整({', '.join(missing_config)})，停止购买流程")
        await playwright.stop()
        close_log()
        return

    # ---- 等待整点 ----
    next_seckill = None
    now = server_now()
    for hour in SECKILL_HOURS:
        target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if target > now:
            next_seckill = target
            break

    if not next_seckill:
        next_seckill = (now + timedelta(days=1)).replace(
            hour=SECKILL_HOURS[0], minute=0, second=0, microsecond=0)

    if run_now or mock_mode:
        next_seckill = now + timedelta(seconds=3)

    wait_seconds = (next_seckill - server_now()).total_seconds()
    log(f"下一场秒杀: {next_seckill.strftime('%H:%M:%S')} (等待{wait_seconds:.0f}秒)")

    # ---- 整点前30秒开始预操作 ----
    pre_start = 30
    if wait_seconds > pre_start:
        log(f"等待{wait_seconds - pre_start:.0f}秒后开始预操作...")
        await asyncio.sleep(max(0, wait_seconds - pre_start))

    # 重新查询商品信息（act_id可能变化）
    log("重新查询秒杀商品信息...")
    goods_list = await query_seckill_by_date(page, activity_id)
    refreshed_config = extract_goods_config(goods_list)
    if not refreshed_config:
        log("⚠ 刷新后未获取到轻量服务器商品配置，停止购买流程")
        await playwright.stop()
        close_log()
        return

    activity_id = int(refreshed_config.get("activity_id") or activity_id)
    target_act_id = refreshed_config.get("act_id")
    goods_limits = refreshed_config.get("goods_limits") or {}
    goods_type = refreshed_config.get("goods_type") or ""
    region_ids = refreshed_config.get("region_ids") or []
    if not target_act_id or not goods_type or not goods_limits or not region_ids:
        log("⚠ 刷新后的商品配置不完整，停止购买流程")
        await playwright.stop()
        close_log()
        return
    log(
        f"  刷新配置: activity_id={activity_id}, act_id={target_act_id}, "
        f"type={goods_type}, regions={region_ids}"
    )
    refreshed_image_ids = await discover_image_ids(page)
    image_id_strategies = refreshed_image_ids
    if not image_id_strategies:
        log("⚠ 未发现有效lhbp-* imageId，停止购买流程")
        await playwright.stop()
        close_log()
        return

    log("校准服务器时间...")
    await get_server_time(page)

    # ============================================================
    # 上游(djs-91)策略: 单地域广州 + 提前2秒 + 开窗0.6秒全速30并发
    # 不调prepare-do(上游证明不需要); 不用preview=1(用户禁止)
    # ============================================================
    TARGET_REGION = 1
    goods = {
        "activity_id": activity_id,
        "act_id": target_act_id,
        "type": "lighthouse_v5",
        "goods_param": {
            "zone": "", "timeSpanUnit": "12m", "scenario": "0",
            "goodsNum": 1, "ddocUnionConnect": 0, "area": 1,
            "BlueprintId": "LINUX_UNIX", "regionId": TARGET_REGION,
            "imageId": image_id_strategies[0],
            "type": goods_type,
        },
    }
    log(f"  抢购商品: act_id={target_act_id}, type={goods_type}, "
        f"地域=广州({TARGET_REGION}), imageId={image_id_strategies[0]}")

    def _srv_ms():
        return int(time.time() * 1000 + _server_time_offset_seconds * 1000)

    target_ms = int(next_seckill.timestamp() * 1000)
    log(f"目标开窗: {next_seckill.strftime('%H:%M:%S')} (服务器时间)")

    # 最后 QUIET_SECONDS 纯等待, 一次网络请求都不发(避免吃掉提前量)
    while True:
        remaining = (target_ms - _srv_ms()) / 1000
        if remaining <= QUIET_SECONDS:
            log(f"最后{remaining:.1f}s, 只等待不发请求")
            gap = (target_ms + ADVANCE_MS - _srv_ms()) / 1000
            if gap > 0:
                await asyncio.sleep(gap)
            left = (target_ms - _srv_ms()) / 1000
            log(f"实际提前{left*1000:.0f}ms(设定{abs(ADVANCE_MS)}ms)")
            break
        await asyncio.sleep(min(1.0, max(0.05, remaining - QUIET_SECONDS)))

    open_in = 3.0 if mock_mode else (target_ms - _srv_ms()) / 1000
    success = await rapid_purchase(page, goods, open_in=open_in, mock=mock_mode)

    if success:
        log("★ 抢购成功! 请去Chrome中完成支付!")
        await page.evaluate("window.open('https://buy.cloud.tencent.com/order','_blank')")
    else:
        log("✗ 未抢到, 看上面各码次数判断卡在哪")

    await playwright.stop()
    close_log()


if __name__ == "__main__":
    asyncio.run(main())
