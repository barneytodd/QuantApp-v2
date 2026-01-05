import asyncio
import pytest
from concurrent.futures import ThreadPoolExecutor
import time
from app.data_ingestion.executors import get_yfinance_executor, run_in_yf_executor


def mock_task(x):
    time.sleep(0.01)  # simulate work
    return x * 2


def test_executor_returns_correct_results():
    inputs = [1, 2, 3, 4, 5]
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(mock_task, inputs))
    assert results == [2, 4, 6, 8, 10]


@pytest.mark.asyncio
async def test_semaphore_limits():
    """
    Confirm that the semaphore prevents more than max_concurrent tasks from running simultaneously.
    """
    max_concurrent = 2
    current_active = 0
    peak_active = 0
    lock = asyncio.Lock()
    test_semaphore = asyncio.Semaphore(max_concurrent)

    # Mock task that simulates a small delay
    async def tracked_task(x):
        nonlocal current_active, peak_active
        async with lock:
            current_active += 1
            peak_active = max(peak_active, current_active)
        await asyncio.sleep(0.01)  # simulate work
        async with lock:
            current_active -= 1
        return x

    # Wrap task with semaphore to limit concurrency
    async def sem_task(x):
        async with test_semaphore:
            # run_in_yf_executor expects a sync function, wrap tracked_task
            return await run_in_yf_executor(lambda: asyncio.run(tracked_task(x)))

    # Test inputs
    inputs = list(range(5))
    results = await asyncio.gather(*(sem_task(i) for i in inputs))

    # Check that semaphore limited concurrency
    assert peak_active <= max_concurrent, f"Peak concurrency exceeded: {peak_active}"
    # Check all results are returned correctly
    assert results == inputs


def test_executor_propagates_exceptions():
    def fail_task(x):
        if x == 3:
            raise ValueError("fail")
        return x

    with ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(ValueError, match="fail"):
            list(executor.map(fail_task, [1, 2, 3, 4]))


def test_get_yfinance_executor_singleton(monkeypatch):
    """Ensure get_yfinance_executor returns a ThreadPoolExecutor singleton."""
    
    # Reset the module-level executor for test isolation
    monkeypatch.setattr("app.data_ingestion.executors._YF_EXECUTOR", None)
    
    # First call should create an executor
    executor1 = get_yfinance_executor()
    assert isinstance(executor1, ThreadPoolExecutor)
    assert executor1._max_workers == 4
    assert executor1._thread_name_prefix.startswith("yfinance")
    
    # Second call should return the same instance
    executor2 = get_yfinance_executor()
    assert executor1 is executor2  # singleton verified
