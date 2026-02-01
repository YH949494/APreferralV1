from database import db, get_collection, init_db
import logging
import os
import socket
import time
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
INSTANCE_ID = (os.getenv("FLY_MACHINE_ID") or os.getenv("FLY_ALLOC_ID") or f"{socket.gethostname()}:{os.getpid()}")

def _new_run_id() -> str:
    return uuid.uuid4().hex[:8]

class JobTimer:
    def __enter__(self):
        self._start = time.monotonic()
        self.elapsed_s = 0.0
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        self.elapsed_s = time.monotonic() - self._start
        return False
        
def main():
    init_db()
    run_id = _new_run_id()
    logger.info("[JOB][MONTHLY] start run_id=%s instance=%s task=backfill_monthly_xp", run_id, INSTANCE_ID)
    with JobTimer() as timer:
        users_collection = get_collection("users")
        result = users_collection.update_many(
            {"monthly_xp": {"$exists": False}},
            {"$set": {"monthly_xp": 0}},
        )
        logger.info(
            "[JOB][MONTHLY] done run_id=%s patched=%s elapsed_s=%.2f task=backfill_monthly_xp",
            run_id,
            result.modified_count,
            timer.elapsed_s,
        )

if __name__ == "__main__":
    main()
