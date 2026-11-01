"""人脸核身: 复用 djs-91 的 .state.json 登录态, 打开腾讯云人脸验证页.
用户微信扫一扫+人脸识别, 脚本轮询 ever_pass 直到通过, 写 .face_ok 标记.
用法: python facecheck_dj.py
"""
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

BASE = Path(__file__).parent
sys.path.insert(0, str(BASE))
import seckill as sf

STATE_PATH = BASE / ".state.json"
FACE_URL = "https://cloud.tencent.com/act/pro/face_authentication"
MARK = BASE / ".face_ok"


def compute_g_tk(skey):
    h = 5381
    for ch in skey:
        h += (h << 5) + ord(ch)
    return str(h & 0x7FFFFFFF)


def query_face_pass():
    try:
        d = json.load(open(STATE_PATH, encoding="utf-8"))
        s = requests.Session()
        skey = None
        for c in d["cookies"]:
            s.cookies.set(c["name"], c["value"], domain=c.get("domain", ""), path=c.get("path", "/"))
            if c["name"] == "skey" and c.get("value") and (skey is None or c.get("domain") == ".cloud.tencent.com"):
                skey = c["value"]
        if not skey:
            return None
        r = s.post("https://act-api.cloud.tencent.com/user/get-facecheck-info", json={},
                   headers={"x-csrf-token": compute_g_tk(skey), "Content-Type": "application/json",
                            "User-Agent": "Mozilla/5.0", "referer": FACE_URL}, timeout=10)
        j = r.json()
        if j.get("code") != 0:
            return None
        return bool((j.get("data") or {}).get("ever_pass"))
    except Exception as e:
        print(f"⚠️ 查询失败: {e}")
        return None


async def main():
    pw = await sf.async_playwright().start()
    browser = await sf.launch_browser(pw, headless=False)
    try:
        ctx = await browser.new_context(storage_state=str(STATE_PATH), viewport={"width": 1400, "height": 900})
        page = await ctx.new_page()
        page.set_default_timeout(30000)
        print("✅ 打开人脸核身页...")
        await page.goto(FACE_URL, wait_until="domcontentloaded", timeout=60000)
        print("请在浏览器里完成: 微信扫一扫 + 人脸识别 (也可用手机微信小程序「腾讯云」做)")
        deadline = time.time() + 900
        while time.time() < deadline:
            await asyncio.sleep(15)
            passed = query_face_pass()
            if passed is True:
                print("✅ 人脸核身已通过!")
                MARK.write_text(datetime.now().isoformat(), encoding="utf-8")
                break
            print("⌛ 等待人脸核身完成...")
        else:
            print("⚠️ 超时未通过")
    finally:
        try:
            await browser.close()
        except Exception:
            pass
        try:
            await pw.stop()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
