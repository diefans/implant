import asyncio

async def loop_task(event_loop: asyncio.BaseEventLoop):
    try:
        while event_loop.is_running():
            print("Running loop")
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        print("Cancelled")
        await asyncio.sleep(5)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    task = loop.create_task(loop_task(loop))

    future = asyncio.gather(task)

    print("foo")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        future.cancel()
        loop.run_until_complete(future)
        loop.close()
