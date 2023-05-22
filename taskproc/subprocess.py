import asyncio
import sys
from typing_extensions import Literal


def run_shell_command(command: str) -> int:
    return asyncio.run(_run_shell_command(command))


async def _run_shell_command(command: str) -> int:
    proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
    )

    assert proc.stdout is not None
    assert proc.stderr is not None
    tasks = [
            asyncio.create_task(transcript(proc.stdout, 'stdout')),
            asyncio.create_task(transcript(proc.stderr, 'stderr')),
            ]
    await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    return await proc.wait()


async def transcript(reader: asyncio.StreamReader, channel: Literal['stdout', 'stderr']):
    chan = sys.stdout if channel == 'stdout' else sys.stderr
    async for line in reader:
        print(line.decode(), end='', flush=True, file=chan)
