from anyio_racer.racer import race

import anyio
import pytest

pytestmark = pytest.mark.anyio


async def test_winner():
    async def sleep(i: int | float):
        await anyio.sleep(i)
        return i

    ret = await race(sleep, [(0.05,), (0.2,), (0.3,)])
    assert ret == 0.05


# Should allow us to still raise the cancellation error unless
# there is no winner seen.
async def test_enable_cancelling_when_no_winner_is_found():
    async def sleep(i: int | float):
        async with anyio.fail_after(0.1):
            # eternity.
            await sleep(400)
        return i

    with pytest.raises(anyio.get_cancelled_exc_class()):
        await race()
