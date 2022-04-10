import os
import sys
import time

from requests import get

sys.path.append(os.curdir)

import signal
import asyncio
import json


class GracefulKiller:
    is_kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        # signal.signal(signal.SIGKILL, self.exit_gracefully)
        signal.signal(signal.SIGHUP, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.is_kill_now = True


async def loop_task(arg):
    killer = GracefulKiller()
    while True:
        if killer.is_kill_now:
            break
        resp = get(f"http://localhost:3302/query-with-id/1?threshold=100000&batch_size=2")
        with open(f"tests/results/{arg[1]}_query_{time.time()}.json", 'w') as f:
            json.dump(resp.json(), f)
        await asyncio.sleep(30)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(loop_task(sys.argv))
    finally:
        loop.run_until_complete(
            # see: https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.AbstractEventLoop.shutdown_asyncgens
            loop.shutdown_asyncgens())
        loop.close()
