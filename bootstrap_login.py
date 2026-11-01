import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import seckill_ui as ui


async def main():
    ok = await ui.do_login()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
