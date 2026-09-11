"""Process-safe locks shared by storage, SQLite and synchronization."""
import errno
try:
    import fcntl
except ImportError:  # Windows
    fcntl = None
    import msvcrt
import os
import threading
import time
from pathlib import Path


def _lock_file(fd):
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return
    if os.fstat(fd).st_size == 0:
        os.write(fd, b"\0")
    os.lseek(fd, 0, os.SEEK_SET)
    try:
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
            raise BlockingIOError(exc.errno, str(exc)) from exc
        raise


def _unlock_file(fd):
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_UN)
    else:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


class FileLock:
    """An advisory, thread-reentrant flock. Never unlink a lock file."""

    def __init__(self, path: Path, timeout: float = 60, name: str = "file"):
        self.path = Path(path)
        self.timeout = timeout
        self.name = name
        self._thread_lock = threading.RLock()
        self._depth = 0
        self._fd = None
        self._owner = None

    def acquire(self, timeout=None) -> bool:
        timeout = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + max(0, timeout)
        if not self._thread_lock.acquire(timeout=max(0, timeout)):
            return False
        try:
            if self._depth:
                self._depth += 1
                return True
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                while True:
                    try:
                        _lock_file(fd)
                        self._fd = fd
                        self._depth = 1
                        self._owner = threading.get_ident()
                        return True
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            os.close(fd)
                            self._thread_lock.release()
                            return False
                        time.sleep(min(0.025, max(0, deadline - time.monotonic())))
            except BaseException:
                os.close(fd)
                raise
        except BaseException:
            self._thread_lock.release()
            raise

    def try_acquire(self) -> bool:
        return self.acquire(timeout=0)

    def release(self):
        if not self._depth or self._owner != threading.get_ident():
            raise RuntimeError("Lock must be released by its owning thread")
        self._depth -= 1
        if not self._depth:
            _unlock_file(self._fd)
            os.close(self._fd)
            self._fd = None
            self._owner = None
        self._thread_lock.release()

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError(f"Could not acquire {self.name} lock within {self.timeout}s")
        return self

    def __exit__(self, *args):
        self.release()


_locks = {}
_registry_lock = threading.Lock()


def storage_lock(storage_dir: Path) -> FileLock:
    """Return the shared lock for a KB; nested use in the same thread is safe."""
    key = (os.getpid(), str(Path(storage_dir).resolve()))
    with _registry_lock:
        if key not in _locks:
            _locks[key] = FileLock(Path(key[1]) / '.hkb-storage.lock', name='storage')
        return _locks[key]
