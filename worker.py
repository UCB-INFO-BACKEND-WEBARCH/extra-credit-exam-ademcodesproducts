import os
import json
import re
import psycopg2
from redis import Redis
from rq import Queue, Worker

STOPWORDS = {"the", "a", "an", "and", "or", "but", "if", "then", "of",
             "in", "on", "at", "to", "for", "with", "by", "is", "are",
             "was", "were", "be", "been", "being", "this", "that"}


def _db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _init_db():
    conn = _db()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'pending',
                current_stage INT NOT NULL DEFAULT 0,
                failed_stage INT,
                error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS top_words (
                id SERIAL PRIMARY KEY,
                job_id TEXT NOT NULL REFERENCES jobs(id),
                word TEXT NOT NULL,
                count INT NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def _update_job(job_id, **kwargs):
    allowed = {"status", "current_stage", "failed_stage", "error"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return
    conn = _db()
    try:
        cur = conn.cursor()
        set_parts = [f"{k} = %s" for k in fields] + ["updated_at = NOW()"]
        values = list(fields.values()) + [job_id]
        cur.execute(f"UPDATE jobs SET {', '.join(set_parts)} WHERE id = %s", values)
        conn.commit()
    finally:
        conn.close()


def run_stage(job_id, stage_num, text=None):
    try:
        _update_job(job_id, status="running", current_stage=stage_num)

        data_dir = f"/data/{job_id}"
        os.makedirs(data_dir, exist_ok=True)

        if stage_num == 1:
            with open(f"{data_dir}/stage1.txt", "w") as f:
                f.write(text)

        elif stage_num == 2:
            with open(f"{data_dir}/stage1.txt") as f:
                content = f.read()
            with open(f"{data_dir}/stage2.txt", "w") as f:
                f.write(content.lower())

        elif stage_num == 3:
            with open(f"{data_dir}/stage2.txt") as f:
                content = f.read()
            tokens = re.findall(r"[a-z0-9]+", content)
            with open(f"{data_dir}/stage3.json", "w") as f:
                json.dump(tokens, f)

        elif stage_num == 4:
            with open(f"{data_dir}/stage3.json") as f:
                tokens = json.load(f)
            filtered = [t for t in tokens if t not in STOPWORDS]
            with open(f"{data_dir}/stage4.json", "w") as f:
                json.dump(filtered, f)

        elif stage_num == 5:
            with open(f"{data_dir}/stage4.json") as f:
                tokens = json.load(f)
            if not tokens:
                raise ValueError("No words remaining after stopword removal")
            freq: dict[str, int] = {}
            for token in tokens:
                freq[token] = freq.get(token, 0) + 1
            with open(f"{data_dir}/stage5.json", "w") as f:
                json.dump(freq, f)
            top5 = sorted(freq.items(), key=lambda x: -x[1])[:5]
            conn = _db()
            try:
                cur = conn.cursor()
                for word, count in top5:
                    cur.execute(
                        "INSERT INTO top_words (job_id, word, count) VALUES (%s, %s, %s)",
                        (job_id, word, count),
                    )
                conn.commit()
            finally:
                conn.close()

        if stage_num < 5:
            redis_conn = Redis.from_url(os.environ["REDIS_URL"])
            Queue("pipeline", connection=redis_conn).enqueue(run_stage, job_id, stage_num + 1)
        else:
            _update_job(job_id, status="completed", current_stage=5)

    except Exception as exc:
        _update_job(job_id, status="failed", failed_stage=stage_num, error=str(exc))


if __name__ == "__main__":
    _init_db()
    redis_conn = Redis.from_url(os.environ["REDIS_URL"])
    Worker(["pipeline"], connection=redis_conn).work()
