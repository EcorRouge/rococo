import contextvars
from concurrent.futures import ThreadPoolExecutor


class ContextAwareThreadPoolExecutor(ThreadPoolExecutor):
    """
    Drop-in replacement for concurrent.futures.ThreadPoolExecutor that
    automatically propagates the current OTel/contextvars context into
    every submitted task.

    Without this, any span created inside a worker thread has no parent
    context available (contextvars do not cross thread boundaries by
    default), so it starts as its own orphaned root trace instead of
    nesting under whatever span was active when .submit()/.map() was
    called — exactly the pattern seen with DataImportJobRepository and
    BaseRepository.bulk_insert appearing as separate root traces.

    Usage: swap the import/constructor, nothing else needs to change.

        from rococo.observability.concurrency import ContextAwareThreadPoolExecutor as ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(repo.bulk_insert, batch) for batch in batches]
            ...
    """

    def submit(self, fn, /, *args, **kwargs):
        ctx = contextvars.copy_context()
        return super().submit(ctx.run, fn, *args, **kwargs)

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        ctx = contextvars.copy_context()

        def _context_wrapped(*args):
            return ctx.run(fn, *args)

        return super().map(_context_wrapped, *iterables, timeout=timeout, chunksize=chunksize)
