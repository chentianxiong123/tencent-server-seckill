"""登录态无害验证器: 周期性调 get-vip-info (绝不产生订单), 提前发现 cookie 过期.

原项目 README 明确 get-vip-info 为"登录态校验"安全接口, 可重复调用.
用法: python3 auth_monitor.py [--interval 60]
挂机循环, 一旦 NOT-LOGINED 立即高亮报警, 写 auth_status.txt.
"""
import json
import sys
import time
from pathlib import Path

import requests

BASE = Path(__file__).parent
STATE_PATH = BASE / ".state.json"
STATUS = BASE / "auth_status.txt"
VIP_URL = "https://act-api.cloud.tencent.com/user/get-vip-info"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def compute_g_tk(skey):
    h = 5381
    for ch in skey:
        h += (h << 5) + ord(ch)
    return str(h & 0x7FFFFFFF)


def check():
    """返回 (code, msg). 无害验证, 不产生订单."""
    try:
        d = json.load(open(STATE_PATH, encoding="utf-8"))
        s = requests.Session()
        skey = None
        for c in d["cookies"]:
            s.cookies.set(c["name"], c["value"], domain=c.get("domain", ""), path=c.get("path", "/"))
            if c["name"] == "skey" and c.get("value") and (skey is None or c.get("domain") == ".cloud.tencent.com"):
                skey = c["value"]
        if not skey:
            return None, "no skey"
        r = s.post(VIP_URL, json={}, headers={
            "x-csrf-token": compute_g_tk(skey),
            "Content-Type": "application/json",
            "User-Agent": UA,
            "referer": "https://cloud.tencent.com/act/pro/double11-202611",
        }, timeout=10)
        j = r.json()
        return j.get("code"), j.get("msg", "")
    except Exception as e:
        return None, str(e)[:80]


def main():
    interval = 60
    if len(sys.argv) > 2 and sys.argv[1] == "--interval":
        interval = int(sys.argv[2])
    print(f"🛡️ 登录态无害验证启动 (get-vip-info, {interval}s 一次), Ctrl+C 停止")
    while True:
        code, msg = check()
        ts = time.strftime("%H:%M:%S")
        if code == 0:
            line = f"[{ts}] ✅ 登录态有效 (get-vip-info code=0)"
            print(line)
        elif code in ("NOT-LOGINED", "CSRF-ERROR") or (isinstance(code, str) and "LOGIN" in str(code).upper()):
            line = f"[{ts}] 🚨 登录态已失效: code={code} msg={msg} —— 请重新扫码登录!"
            print(f"\n{'='*60}\n{line}\n{'='*60}", flush=True)
            STATUS.write_text(line, encoding="utf-8")
        else:
            line = f"[{ts}] ⚠️ 异常返回: code={code} msg={msg}"
            print(line)
        time.sleep(interval)


if __name__ == "__main__":
    main()
