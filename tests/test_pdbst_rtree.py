import asyncio


class Writer:
    pass


async def run(loop):
    f = open('/tmp/x', 'wb')

    try:
        transport, protocol = await loop.connect_write_pipe(Writer, f)
    except ValueError as exc:
        print('Exception:', exc)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
