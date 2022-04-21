import os
import sys
import time

sys.path.append(os.curdir)

import signal
import asyncio
import json
import psutil


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
    # killer = GracefulKiller()
    while True:
        # if killer.is_kill_now:
        #     break
        cpu = psutil.cpu_percent(10)
        mem = psutil.virtual_memory()[2]
        t = time.time()
        with open(f"tests/results/{arg[1]}_top_{t}.json", 'w') as f:
            json.dump({"cpu": cpu, "mem": mem, "time": t}, f)
        await asyncio.sleep(60)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(loop_task(sys.argv))
    finally:
        loop.run_until_complete(
            # see: https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.AbstractEventLoop.shutdown_asyncgens
            loop.shutdown_asyncgens())
        loop.close()
