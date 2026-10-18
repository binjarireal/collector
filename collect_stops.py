"""정류장 도착정보 수집기: 주요 정류장마다 「경기도_버스도착정보 조회」(getBusArrivalListv2)를 불러
그 정류장에 곧 도착하는 광역버스(다음 버스·그다음 버스)의 남은 좌석을 기록한다.

사용법:
  python collect_stops.py --set am --start 06:40 --end 08:40 --every 180
  python collect_stops.py --set pm --start 17:30 --end 19:30 --every 180
  python collect_stops.py --set am --start now --minutes 3          # 시험
대상: stops.json 의 am(출근, 경기 정류장) / pm(퇴근, 서울 정류장). 좌석형 광역버스(routes.json)만 저장.
저장: data/YYYYMMDD_HHMM_stops.csv.gz   한도 관리: data/YYYYMMDD_calls_arrival.json (위치정보와 한도가 따로)
차량번호는 저장하지 않음.
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
URL = "https://apis.data.go.kr/6410000/busarrivalservice/v2/getBusArrivalListv2"
FIELDS = ["ts", "stationId", "routeId", "staOrder",
          "vehId1", "locationNo1", "predictTimeSec1", "remainSeatCnt1", "crowded1",
          "vehId2", "locationNo2", "predictTimeSec2", "remainSeatCnt2", "crowded2"]


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


class AuthError(Exception):
    pass


def fetch(key_q, station_id):
    url = f"{URL}?serviceKey={key_q}&stationId={station_id}&format=json"
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
    items = body.get("busArrivalList") or []
    return items if isinstance(items, list) else [items]


def parse_hm(s, day):
    h, m = map(int, s.split(":"))
    return datetime.combine(day, datetime.min.time()).replace(hour=h, minute=m)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", choices=["am", "pm"], required=True)
    ap.add_argument("--start", default="06:00")
    ap.add_argument("--end", default="09:00")
    ap.add_argument("--minutes", type=int)
    ap.add_argument("--every", type=int, default=180)
    ap.add_argument("--budget", type=int, default=9950, help="도착정보 하루 호출 상한 (한도 1만)")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int)
    a = ap.parse_args()

    key = os.environ.get("DATA_GO_KR_KEY", "").strip()
    if not key:
        sys.exit("DATA_GO_KR_KEY 가 없습니다.")
    key_q = key if "%" in key else urllib.parse.quote(key, safe="")
    stops = json.loads((ROOT / "stops.json").read_text(encoding="utf-8"))[a.set][: a.limit]
    seat_routes = {r["routeId"] for r in json.loads((ROOT / "routes.json").read_text(encoding="utf-8"))["routes"]}

    now = datetime.now()
    if now.weekday() >= 5 and a.start != "now":
        log("주말이라 수집하지 않음"); return
    start = now if a.start == "now" else parse_hm(a.start, now.date())
    end = start + timedelta(minutes=a.minutes or 3) if a.start == "now" else parse_hm(a.end, now.date())
    if datetime.now() > end:
        log("수집 시간이 이미 지남"); return
    DATA.mkdir(exist_ok=True)
    day = start.strftime("%Y%m%d")
    cpath = DATA / f"{day}_calls_arrival.json"
    calls = json.loads(cpath.read_text())["arrival"] if cpath.exists() else 0
    need = len(stops) * int((end - start).total_seconds() // a.every + 1)
    log(f"{day} {a.set} {start:%H:%M}~{end:%H:%M}, {a.every}초 간격, 정류장 {len(stops)}곳 → 예상 {need:,}회 (오늘 이미 {calls:,}회, 상한 {a.budget:,})")
    while datetime.now() < start:
        time.sleep(min(30, max(1, (start - datetime.now()).total_seconds())))

    out = DATA / f"{day}_{start:%H%M}_stops.csv.gz"
    new = not out.exists()
    f = gzip.open(out, "at", encoding="utf-8", newline="")
    w = csv.DictWriter(f, fieldnames=FIELDS)
    if new:
        w.writeheader()
    polls = rows = fails = 0
    dead_since = None
    nxt = datetime.now()

    def one(s):
        try:
            return s, fetch(key_q, s["stationId"]), None
        except AuthError:
            raise
        except Exception as e:
            return s, None, e

    with ThreadPoolExecutor(a.workers) as pool:
        while datetime.now() <= end:
            if calls + len(stops) > a.budget:
                log(f"하루 상한({a.budget:,}회) 도달 → 중단"); break
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                res = list(pool.map(one, stops))
            except AuthError as e:
                log(f"인증 오류로 중단: {e}"); break
            calls += len(stops)
            cpath.write_text(json.dumps({"arrival": calls}))
            ok = 0
            for s, items, err in res:
                if err is not None:
                    fails += 1
                    continue
                ok += 1
                for it in items:
                    if str(it.get("routeId")) not in seat_routes:
                        continue  # 좌석형 광역버스만
                    w.writerow({"ts": ts, "stationId": s["stationId"], **{k: it.get(k, "") for k in FIELDS[2:]}})
                    rows += 1
            f.flush()
            polls += 1
            if ok == 0:
                dead_since = dead_since or datetime.now()
                # 공개 저장소라 실행 시간 제한이 없으니 멈추지 않고 다음 회차에 계속 시도
                if polls % 5 == 0:
                    log(f"모든 정류장 조회 실패 {int((datetime.now() - dead_since).total_seconds() // 60)}분째 — 계속 시도")
            else:
                if dead_since:
                    log(f"복구됨 (실패 {int((datetime.now() - dead_since).total_seconds() // 60)}분)")
                dead_since = None
            if polls % 10 == 0 or polls == 1:
                took = (datetime.now() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).total_seconds()
                log(f"{polls}회째: 성공 {ok}/{len(stops)}곳, 누적 {rows:,}줄, {took:.0f}초 걸림, 오늘 호출 {calls:,}회")
            nxt += timedelta(seconds=a.every)
            if nxt < datetime.now():
                nxt = datetime.now()
            time.sleep(max(0, (nxt - datetime.now()).total_seconds()))
    log(f"끝: {polls}회 조회, {rows:,}줄, 실패 {fails}건 → {out.name}, 오늘 호출 {calls:,}회")
    f.close()
    expected = int((end - start).total_seconds() // a.every + 1)
    if a.start != "now" and (rows == 0 or polls < expected * 0.5):
        log(f"⚠️ 수집 부족: {polls}/{expected}회 조회, {rows:,}줄 → 실패로 표시(알림)")
        sys.exit(2)


if __name__ == "__main__":
    main()
