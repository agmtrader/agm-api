"""Bounded, ordered reads using the Drive connector's thread-local transports."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context


# Share the bound across report requests in one worker instead of creating an
# unbounded new pool for every account/month/request. No data or credentials
# are retained here. Source selection and freshness stay with each caller.
_reads = ThreadPoolExecutor(max_workers=3, thread_name_prefix='report-read')


def read_report_files(items, read):
    futures = [_reads.submit(copy_context().run, read, item) for item in items]
    try:
        # Keep source order so existing financial accumulation/sort ties agree.
        return [future.result() for future in futures]
    except BaseException:
        for future in futures:
            future.cancel()
        raise
