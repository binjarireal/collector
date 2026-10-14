# 광역버스 빈자리 수집기

[광역버스 빈자리 예보](https://binjari.pages.dev)의 실측 자료를 모으는 저장소입니다.

- 경기도 좌석형 광역버스 490개 노선(직행좌석·좌석·광역급행)의 운행 중 버스 위치와 **남은 좌석 수**를
  평일 출근(06:00~09:00)·퇴근(17:00~20:00) 시간에 2분 간격으로 기록합니다.
- 출처: 공공데이터포털 「경기도_버스위치정보 조회」 (경기도)
- 기록: `data/YYYYMMDD_HHMM.csv.gz` — `ts, routeId, vehId, stationSeq, stationId, remainSeatCnt, crowded, stateCd, lowPlate, routeTypeCd`
- 노선 목록: `routes.json` (2026-10-10 경기도 노선조회 API 기준)

노선·정류장·남은 좌석 정보만 다루며 개인정보와 차량번호는 저장하지 않습니다.
