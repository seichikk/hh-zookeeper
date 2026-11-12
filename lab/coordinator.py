import argparse
import csv
import gzip
import json
import logging
import math
import os
import sys
import threading
import time
from collections import Counter
from datetime import datetime

from botocore.exceptions import BotoCoreError, ClientError
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, RolledBackError
from kazoo.retry import KazooRetry

from lab import common as c
from lab import mapreduce as mr

ZK_HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
ZK_TIMEOUT = float(os.environ.get("ZK_TIMEOUT", "10"))
FENCING = os.environ.get("FENCING", "0") == "1"

ROOT = "/hh"
ELECTION = ROOT + "/election"
JOB = ROOT + "/job"
EPOCH = ROOT + "/epoch"
CHECKPOINT = ROOT + "/checkpoint"
RESULT = ROOT + "/result"
HW3 = "results/mapreduce"


class Log:
    def __init__(self, name, path):
        self.name = name
        self.lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(path, "a", encoding="utf-8")

    def __call__(self, text):
        line = f"{datetime.now():%H:%M:%S.%f}"[:-3] + f" [{self.name}] {text}"
        with self.lock:
            print(line, flush=True)
            self.f.write(line + "\n")
            self.f.flush()


def slow_map(delay, *args):
    time.sleep(delay)
    return mr.map_task(*args)


def pack(res):
    out = dict(res)
    out["parts"] = [[[[list(combo), n] for combo, n in part.items()] for part in row] for row in res["parts"]]
    return gzip.compress(json.dumps(out, ensure_ascii=False).encode("utf-8"))


def unpack(data):
    res = json.loads(gzip.decompress(data))
    res["parts"] = [[Counter({tuple(combo): n for combo, n in part}) for part in row] for row in res["parts"]]
    return res


def ranges(ids):
    out = []
    for i in ids:
        if out and out[-1][1] == i - 1:
            out[-1][1] = i
        else:
            out.append([i, i])
    return ", ".join(f"{a}-{b}" if a != b else str(a) for a, b in out)


def control(summary):
    ks = ", ".join(f"k={k}: {summary['unique'][k - 1]} / {summary['sums'][k - 1]}" for k in c.KS)
    return f"вакансий {summary['vacancies']}, {ks}"


class Master(mr.Master):
    def __init__(self, workers, delay, on_done):
        super().__init__("opt", workers, 4, -1, False)
        self.delay = delay
        self.on_done = on_done

    def submit(self, tid):
        data, attempt = self.tasks[tid]
        try:
            f = self.pool.submit(slow_map, self.delay, tid, attempt, data, self.fields, self.mode, self.fail_task,
                                 self.reducers)
        except mr.BrokenProcessPool:
            self.recover("пул процессов сломан, один из мапперов аварийно завершился", extra=(tid,))
            return
        self.futures[f] = tid

    def collect(self, limit):
        while len(self.futures) > limit:
            before = set(self.done)
            super().collect(len(self.futures) - 1)
            for tid in sorted(set(self.done) - before):
                self.on_done(tid, self.done[tid])


class Coordinator:
    def __init__(self, a):
        self.a = a
        self.name = a.name
        self.log = Log(a.name, c.ROOT / "logs" / f"{a.run}-{a.name}.log")
        self.s3 = c.client()
        self.retry = KazooRetry(max_tries=-1, delay=0.2, max_delay=2)
        self.zk = KazooClient(hosts=ZK_HOSTS, timeout=ZK_TIMEOUT,
                              connection_retry=KazooRetry(max_tries=-1, delay=0.5, max_delay=2))
        self.zk.add_listener(self.on_state)
        self.epoch = None
        self.epoch_version = None
        self.rc = 0

    def on_state(self, state):
        self.log(f"сессия zookeeper: {state}")

    def run(self):
        self.zk.start(timeout=30)
        self.log(f"подключился к {ZK_HOSTS}, сессия 0x{self.zk.client_id[0]:x}, "
                 f"тайм-аут {self.zk._session_timeout / 1000:g} с, fencing {'вкл' if FENCING else 'выкл'}")
        for path in (ELECTION, CHECKPOINT):
            self.retry(self.zk.ensure_path, path)
        for path, value in ((EPOCH, b"0"), (RESULT, b"")):
            try:
                self.retry(self.zk.create, path, value)
            except NodeExistsError:
                pass
        self.election = self.zk.Election(ELECTION, self.name)
        leaders = self.election.contenders()
        if leaders:
            self.log(f"лидер сейчас {leaders[0]}, жду")
        self.election.run(self.lead)
        self.zk.remove_listener(self.on_state)
        self.zk.stop()
        return self.rc

    def epoch_tx(self, mine, epoch, version):
        tx = self.zk.transaction()
        tx.check(mine, 0)
        tx.set_data(EPOCH, str(epoch).encode(), version=version)
        return tx.commit()

    def take_epoch(self):
        mine = f"{self.election.lock.path}/{self.election.lock.node}"
        while True:
            data, st = self.retry(self.zk.get, EPOCH)
            epoch = int(data or b"0") + 1
            res = self.retry(self.epoch_tx, mine, epoch, st.version)
            if not isinstance(res[0], Exception) and not isinstance(res[1], Exception):
                return epoch, res[1].version
            if not isinstance(res[0], RolledBackError):
                self.log(f"узла {mine} уже нет ({type(res[0]).__name__}), лидерство потеряно до получения эпохи")
                return None, None

    def check_job(self):
        head = self.s3.head_object(Bucket=c.BUCKET, Key=c.KEY)
        job = {"run": self.a.run, "object": f"s3://{c.BUCKET}/{c.KEY}", "etag": head["ETag"],
               "bytes": head["ContentLength"], "chunk_mib": self.a.chunk_mib}
        data = json.dumps(job).encode()
        exists = False
        if FENCING:
            out = self.retry(self.fenced, JOB, data, True)
            exists = isinstance(out[1], NodeExistsError)
            if not exists and isinstance(out[0], Exception):
                self.log(f"манифест задания не записан: {type(out[0]).__name__}, моя эпоха {self.epoch} устарела")
                return None
        else:
            try:
                self.retry(self.zk.create, JOB, data)
            except NodeExistsError:
                exists = True
        if exists:
            stored = json.loads(self.retry(self.zk.get, JOB)[0])
            if stored != job:
                raise SystemExit(f"в {ROOT} чекпоинт другого задания: {stored}, у меня {job}")
        return job

    def lead(self):
        t0 = time.perf_counter()
        self.epoch, self.epoch_version = self.take_epoch()
        if self.epoch is None:
            self.rc = 1
            return
        self.log(f"стал лидером, эпоха {self.epoch}")
        job = self.check_job()
        if job is None:
            self.rc = 1
            return
        done = self.load_checkpoint()
        counts, vacancies, summary = self.compute(done, job)
        prefix, same = self.save(counts, vacancies, summary)
        self.publish(prefix, summary, same)
        self.log(f"работа заняла {time.perf_counter() - t0:.1f} с")

    def load_checkpoint(self):
        done = {}
        for child in sorted(self.retry(self.zk.get_children, CHECKPOINT)):
            meta = json.loads(self.retry(self.zk.get, f"{CHECKPOINT}/{child}")[0])
            done[int(child.split("-")[1])] = unpack(c.get_bytes(self.s3, meta["key"]))
        if done:
            self.log(f"в чекпоинте {len(done)} задач ({ranges(sorted(done))}), их результаты беру из s3")
        else:
            self.log("чекпоинт пустой, считаю с начала")
        return done

    def fenced(self, path, value, create=False):
        tx = self.zk.transaction()
        tx.check(EPOCH, self.epoch_version)
        if create:
            tx.create(path, value)
        else:
            tx.set_data(path, value)
        return tx.commit()

    def checkpoint(self, tid, res):
        node = f"{CHECKPOINT}/task-{tid:04d}"
        if self.retry(self.zk.exists, node):
            meta = json.loads(self.retry(self.zk.get, node)[0])
            self.log(f"задача {tid} готова, но в чекпоинте она уже есть (эпоха {meta['epoch']}, {meta['owner']})")
            return
        key = f"zk/{self.a.run}/checkpoint/task-{tid:04d}-e{self.epoch}.json.gz"
        c.put_bytes(self.s3, key, pack(res), "application/gzip")
        meta = json.dumps({"key": key, "epoch": self.epoch, "owner": self.name}).encode()
        if FENCING:
            out = self.retry(self.fenced, node, meta, True)
            if isinstance(out[1], NodeExistsError):
                return
            if isinstance(out[0], Exception):
                self.log(f"чекпоинт задачи {tid} отклонён: {type(out[0]).__name__}, моя эпоха {self.epoch}")
                return
        else:
            try:
                self.retry(self.zk.create, node, meta)
            except NodeExistsError:
                return
        self.log(f"задача {tid} готова: строк {res['scanned']}, вакансий {len(res['vacancies'])}, чекпоинт сохранён")

    def add(self, m, tid, data, done):
        if tid in done:
            m.done[tid] = done[tid]
            return
        m.tasks[tid] = [data, 1]
        m.submit(tid)

    def compute(self, done, job):
        a = self.a
        size = job["bytes"]
        chunk = a.chunk_mib << 20
        self.log(f"объект s3://{c.BUCKET}/{c.KEY}: {size} байт, чанк {a.chunk_mib} миб (около {math.ceil(size / chunk)} задач), "
                 f"мапперов {a.workers}, задержка {a.task_delay:g} с на задачу")
        m = Master(a.workers, a.task_delay, self.checkpoint)
        offset = 0
        carry = b""
        tid = 0
        while offset < size:
            end = min(offset + chunk, size) - 1
            data = self.s3.get_object(Bucket=c.BUCKET, Key=c.KEY, Range=f"bytes={offset}-{end}",
                                      IfMatch=job["etag"])["Body"].read()
            offset = end + 1
            buf = carry + data
            carry = b""
            if m.fields is None:
                if buf.startswith(mr.BOM):
                    buf = buf[3:]
                nl = buf.find(b"\n")
                m.fields = next(csv.reader([buf[:nl].decode("utf-8").rstrip("\r")]))
                buf = buf[nl + 1:]
                missing = [f for f in c.REQUIRED if f not in m.fields]
                if missing:
                    raise SystemExit("в датасете нет колонок: " + ", ".join(missing))
            cut = len(buf) if offset >= size else mr.record_boundary(buf)
            if cut <= 0:
                carry = buf
                continue
            self.add(m, tid, buf[:cut], done)
            carry = buf[cut:]
            tid += 1
            m.collect(a.workers * 2)
        if carry:
            self.add(m, tid, carry, done)
            tid += 1
        m.collect(0)
        shuffle = [[[] for _ in c.KS] for _ in range(m.reducers)]
        vacancies = []
        for t in sorted(m.done):
            res = m.done[t]
            vacancies.extend(res["vacancies"])
            for r in range(m.reducers):
                for k in c.KS:
                    shuffle[r][k - 1].append(res["parts"][r][k - 1])
        counts = {k: {} for k in c.KS}
        futs = [m.pool.submit(mr.reduce_task, r, k, shuffle[r][k - 1], "opt") for r in range(m.reducers) for k in c.KS]
        for f in futs:
            r, k, part = f.result()
            counts[k].update(part)
        m.pool.shutdown()
        combos = [sum(math.comb(len(v["skills"]), k) for v in vacancies) for k in c.KS]
        sums = [sum(counts[k].values()) for k in c.KS]
        if sums != combos:
            raise SystemExit("сумма частот не совпала с числом сочетаний")
        summary = {"owner": self.name, "epoch": self.epoch, "map_tasks": tid,
                   "from_checkpoint": sorted(t for t in done if t < tid), "vacancies": len(vacancies),
                   "unique": [len(counts[k]) for k in c.KS], "sums": sums, "combinations": combos}
        self.log(f"задач {tid}, из чекпоинта {len(summary['from_checkpoint'])}, посчитано заново {tid - len(summary['from_checkpoint'])}")
        self.log("контроль: " + control(summary))
        return counts, vacancies, summary

    def save(self, counts, vacancies, summary):
        prefix = f"zk/{self.a.run}/result-e{self.epoch}-{self.name}"
        files = {f"k{k}.csv": c.counts_to_csv(counts[k], k) for k in c.KS}
        files["vacancies.jsonl"] = c.vacancies_to_jsonl(vacancies)
        files["summary.json"] = (json.dumps(summary, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        for name, data in files.items():
            c.put_bytes(self.s3, f"{prefix}/{name}", data)
        return prefix, self.compare(files, summary)

    def compare(self, files, summary):
        try:
            ref = json.loads(c.get_bytes(self.s3, f"{HW3}/summary.json"))
            same = all(ref[f] == summary[f] for f in ("vacancies", "unique", "sums"))
            self.log(f"дз-3: {control(ref)}, совпадает: {'да' if same else 'НЕТ'}")
            for name in ("k1.csv", "k2.csv", "k3.csv", "vacancies.jsonl"):
                mine = c.sha256_bytes(files[name])
                theirs = c.sha256_bytes(c.get_bytes(self.s3, f"{HW3}/{name}"))
                same = same and mine == theirs
                self.log(f"{name}: sha256 {mine[:16]}..., дз-3 {theirs[:16]}..., совпадает: {'да' if mine == theirs else 'НЕТ'}")
        except (BotoCoreError, ClientError, ValueError, KeyError, IndexError, TypeError) as e:
            self.log(f"сверить с дз-3 не удалось: {e!r}")
            return None
        return same

    def publish(self, prefix, summary, same):
        pointer = json.dumps({"owner": self.name, "epoch": self.epoch, "prefix": prefix, "vacancies": summary["vacancies"],
                              "unique": summary["unique"], "sums": summary["sums"], "same_as_hw3": same}).encode()
        if not FENCING:
            st = self.retry(self.zk.set, RESULT, pointer)
        else:
            res = self.retry(self.fenced, RESULT, pointer)
            err = next((r for r in res if isinstance(r, Exception)), None)
            if err is not None:
                now = self.retry(self.zk.get, EPOCH)[0].decode()
                self.log(f"запись результата отклонена: {type(err).__name__}, моя эпоха {self.epoch}, в zookeeper эпоха {now}")
                self.rc = 1
                return
            st = res[1]
        self.log(f"записал результат: {RESULT} версия {st.version}, эпоха {self.epoch}, s3://{c.BUCKET}/{prefix}/")


def reset(run):
    zk = KazooClient(hosts=ZK_HOSTS, timeout=ZK_TIMEOUT)
    zk.start(timeout=30)
    zk.ensure_path(ROOT)
    if not zk.exists(EPOCH):
        zk.create(EPOCH, b"0")
    while True:
        data, st = zk.get(EPOCH)
        epoch = int(data or b"0") + 1
        try:
            zk.set(EPOCH, str(epoch).encode(), version=st.version)
            break
        except BadVersionError:
            continue
    for child in zk.get_children(ROOT):
        if f"{ROOT}/{child}" != EPOCH:
            zk.delete(f"{ROOT}/{child}", recursive=True)
    zk.stop()
    s3 = c.client()
    keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=c.BUCKET, Prefix=f"zk/{run}/")
            for o in page.get("Contents", [])]
    for key in keys:
        s3.delete_object(Bucket=c.BUCKET, Key=key)
    print(f"эпоха поднята до {epoch}, {ROOT} очищен, удалено {len(keys)} объектов s3://{c.BUCKET}/zk/{run}/")
    return 0


def main():
    logging.getLogger("kazoo").setLevel(logging.ERROR)
    p = argparse.ArgumentParser()
    p.add_argument("action", nargs="?", choices=["run", "reset"], default="run")
    p.add_argument("--name", default="a")
    p.add_argument("--run", default="test")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--chunk-mib", type=int, default=32)
    p.add_argument("--task-delay", type=float, default=1.0)
    a = p.parse_args()
    if a.action == "reset":
        return reset(a.run)
    return Coordinator(a).run()


if __name__ == "__main__":
    sys.exit(main())
