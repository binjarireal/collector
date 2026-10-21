"""두 수집기가 같이 쓰는 안전장치.

- 실패 이유 기록: 어떤 오류가 몇 번, 처음·마지막 시각 → data/<날짜>_<시각>[_stops]_errors.json
- 다시 시작 신호: 수집이 계속 안 되면 종료 코드 3으로 끝낸다.
  그러면 GitHub Actions(collect.yml)가 새 실행 컴퓨터(다른 인터넷 주소)를 띄워 같은 시간대 수집을 이어 간다.
"""
import json
import socket
import threading
import urllib.error
import urllib.request
from datetime import datetime

RESTART = 3          # 종료 코드: 새 실행 컴퓨터로 다시 시작해 달라는 뜻
FAIL_POLLS = 4       # 연속으로 이만큼(3분 간격이면 약 12분) 전부 실패하면 다시 시작
MIN_LEFT_MIN = 15    # 남은 수집 시간이 이보다 짧으면 다시 시작하지 않고 그냥 계속 시도


def get_text(url):
    """주소를 불러 (상태 코드, 글자)를 돌려준다. 연결 자체가 안 되면 사람이 읽을 수 있는 이유로 오류를 낸다."""
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except (socket.timeout, TimeoutError):
        raise RuntimeError("시간 초과(20초)")
    except urllib.error.URLError as e:
        if isinstance(e.reason, (socket.timeout, TimeoutError)):
            raise RuntimeError("시간 초과(20초)")
        raise RuntimeError(f"연결 실패: {e.reason}")


def parse_json(status, body):
    try:
        return json.loads(body)
    except ValueError:
        head = " ".join(body.split())[:80]
        raise RuntimeError(f"JSON 아님(HTTP {status}): {head}")


class Errors:
    def __init__(self, path):
        self.path, self.d, self.lock = path, {}, threading.Lock()

    def add(self, err):
        key = str(err)[:160]
        now = datetime.now().strftime("%H:%M:%S")
        with self.lock:
            e = self.d.setdefault(key, {"n": 0, "first": now})
            e["n"] += 1
            e["last"] = now

    def save(self):
        with self.lock:
            if self.d:
                self.path.write_text(json.dumps(self.d, ensure_ascii=False, indent=1), encoding="utf-8")

    def top(self, k=3):
        with self.lock:
            return ", ".join(f"{key} ×{v['n']}" for key, v in sorted(self.d.items(), key=lambda x: -x[1]["n"])[:k])
