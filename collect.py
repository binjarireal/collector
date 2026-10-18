"""경기도 광역버스 남은 좌석 수집기 (binjari.pages.dev 용).

공공데이터포털 「경기도_버스위치정보 조회」(getBusLocationListv2)를 노선마다 불러
운행 중인 버스의 현재 정류장 순번과 남은 좌석(remainSeatCnt)을 기록한다.

사용법:
  python collect.py --start 06:40 --end 08:40               # 그 시각까지 기다렸다가 3분 간격 수집
  python collect.py --start now --minutes 3                  # 지금 바로 3분 시험
키: 환경변수 DATA_GO_KR_KEY (인코딩 키면 그대로, 디코딩 키면 주소용으로 바꿔 씀)
저장: data/YYYYMMDD_<시작시각>.csv.gz   한도 관리: data/YYYYMMDD_calls.json
"""
import argparse
import csv
import gzip
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
URL = "https://apis.data.go.kr/6410000/buslocationservice/v2/getBusLocationListv2"
# 차량번호(plateNo)는 분석에 필요 없어 저장하지 않음 — 버스 구분은 내부 번호(vehId)로 충분
FIELDS = ["ts", "routeId", "vehId", "stationSeq", "stationId",
          "remainSeatCnt", "crowded", "stateCd", "lowPlate", "routeTypeCd"]


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


class AuthError(Exception):
    pass


def fetch(key_q, route_id):
    url = f"{URL}?serviceKey={key_q}&routeId={route_id}&format=json"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            body = r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
    data = json.loads(body)
    if "OpenAPI_ServiceResponse" in data:
        h = data["OpenAPI_ServiceResponse"]["cmmMsgHeader"]
        raise AuthError(f"{h.get('returnAuthMsg')} ({h.get('errMsg')})")
    body = (data.get("response") or data).get("msgBody") or {}
    buses = body.get("busLocationList") or []
    return buses if isinstance(buses, list) else [buses]


class Calls:
    """그날 호출 수를 파일에 남겨 아침·저녁 실행을 합쳐도 하루 한도를 넘지 않게."""

    def __init__(self, day):
        self.path = DATA / f"{day}_calls.json"
        self.n = json.loads(self.path.read_text())["location"] if self.path.exists() else 0
        self.lock = threading.Lock()

    def add(self, k):
        with self.lock:
            self.n += k
            self.path.write_text(json.dumps({"location": self.n}))


def parse_hm(s, day):
    h, m = map(int, s.split(":"))
    return datetime.combine(day, datetime.min.time()).replace(hour=h, minute=m)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="06:00")
    ap.add_argument("--end", default="09:00")
    ap.add_argument("--minutes", type=int)
    ap.add_argument("--every", type=int, default=180, help="조회 간격(초)")
    ap.add_argument("--budget", type=int, default=9950, help="버스위치 하루 호출 상한 (한도 1만)")
    ap.add_argument("--workers", type=int, default=16, help="동시에 부르는 수")
    ap.add_argument("--limit", type=int, help="앞에서부터 노선 몇 개만(시험용)")
    a = ap.parse_args()

    key = os.environ.get("DATA_GO_KR_KEY", "").strip()
    if not key:
        sys.exit("DATA_GO_KR_KEY 가 없습니다.")
    key_q = key if "%" in key else urllib.parse.quote(key, safe="")
    routes = json.loads((ROOT / "routes.json").read_text(encoding="utf-8"))["routes"]
    active = ROOT / "active.json"  # 있으면 그 노선만 (하루 한도에 맞춘 우선 노선)
    if active.exists():
        ids = set(json.loads(active.read_text(encoding="utf-8"))["routeIds"])
        routes = [r for r in routes if r["routeId"] in ids]
    routes = routes[: a.limit]

    now = datetime.now()
    if now.weekday() >= 5 and a.start != "now":
        log("주말이라 수집하지 않음"); return
    start = now if a.start == "now" else parse_hm(a.start, now.date())
    end = start + timedelta(minutes=a.minutes or 3) if a.start == "now" else parse_hm(a.end, now.date())
    if datetime.now() > end:
        log("수집 시간이 이미 지남"); return
    DATA.mkdir(exist_ok=True)
    day = start.strftime("%Y%m%d")
    calls = Calls(day)
    need = len(routes) * int((end - start).total_seconds() // a.every + 1)
    log(f"{day} {start:%H:%M}~{end:%H:%M}, {a.every}초 간격, 노선 {len(routes)}개 → 예상 {need:,}회 (오늘 이미 {calls.n:,}회, 상한 {a.budget:,})")
    while datetime.now() < start:
        time.sleep(min(30, max(1, (start - datetime.now()).total_seconds())))

    out = DATA / f"{day}_{start:%H%M}.csv.gz"
    f = gzip.open(out, "at", encoding="utf-8", newline="")
    w = csv.DictWriter(f, fieldnames=FIELDS)
    if out.stat().st_size == 0 or f.tell() == 0:
        w.writeheader()
    wlock = threading.Lock()
    polls = rows = fails = 0
    dead_since = None
    nxt = datetime.now()
    with ThreadPoolExecutor(a.workers) as pool:
        while datetime.now() <= end:
            if calls.n + len(routes) > a.budget:
                log(f"하루 상한({a.budget:,}회) 도달 → 중단"); break
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            def one(r):
                try:
                    return r, fetch(key_q, r["routeId"]), None
                except AuthError:
                    raise
                except Exception as e:  # 시간 초과 등은 이번 회차만 건너뜀
                    return r, None, e

            try:
                res = list(pool.map(one, routes))
            except AuthError as e:
                log(f"인증 오류로 중단: {e}"); break
            calls.add(len(routes))
            ok = 0
            with wlock:
                for r, buses, err in res:
                    if err is not None:
                        fails += 1
                        continue
                    ok += 1
                    for b in buses:
                        w.writerow({"ts": ts, **{k: b.get(k, "") for k in FIELDS if k != "ts"}})
                        rows += 1
            f.flush()
            polls += 1
            if ok == 0:
                dead_since = dead_since or datetime.now()
                # 공개 저장소라 실행 시간 제한이 없으니 멈추지 않고 다음 회차에 계속 시도
                if polls % 5 == 0:
                    log(f"모든 노선 조회 실패 {int((datetime.now() - dead_since).total_seconds() // 60)}분째 — 계속 시도")
            else:
                if dead_since:
                    log(f"복구됨 (실패 {int((datetime.now() - dead_since).total_seconds() // 60)}분)")
                dead_since = None
            took = (datetime.now() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).total_seconds()
            if polls % 10 == 0 or polls == 1:
                log(f"{polls}회째: 성공 {ok}/{len(routes)}개 노선, 누적 {rows:,}줄, {took:.0f}초 걸림, 오늘 호출 {calls.n:,}회")
            nxt += timedelta(seconds=a.every)
            if nxt < datetime.now():
                nxt = datetime.now()
            time.sleep(max(0, (nxt - datetime.now()).total_seconds()))
    log(f"끝: {polls}회 조회, {rows:,}줄, 실패 {fails}건 → {out.name}, 오늘 호출 {calls.n:,}회")
    f.close()
    expected = int((end - start).total_seconds() // a.every + 1)
    if a.start != "now" and (rows == 0 or polls < expected * 0.5):
        log(f"⚠️ 수집 부족: {polls}/{expected}회 조회, {rows:,}줄 → 실패로 표시(알림)")
        sys.exit(2)


if __name__ == "__main__":
    main()
