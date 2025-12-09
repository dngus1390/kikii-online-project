import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
# from tqdm import tqdm

# 1. MySQL 연결 설정
load_dotenv()
database_url = os.getenv('DATABASE_URL')

engine = create_engine(
    f"{database_url}"
)


# 2. final_df 로드
final_df = pd.read_csv("C:/Users/USER/Desktop/키키아이(주)/과제전형/data/final/final.csv",
                    dtype={'교통수단타입명': 'str', '노선번호': 'str', '버스정류장ARS번호': 'str'},
                    low_memory=False
)



# final_df 컬럼:
# [사용년월, 노선번호, 노선명, 표준버스정류장ID, 버스정류장ARS번호,
# 역명, 교통수단타입코드, 교통수단타입명, 승객수, 시간, 승하차구분]

# dtype 조금 줄이기 (메모리 최적화)
final_df['승객수'] = final_df['승객수'].astype('int32')
final_df['시간'] = final_df['시간'].astype('int8')
final_df['사용년월'] = final_df['사용년월'].astype('int32')




# 3. route 테이블 생성
## '교통수단타입명'의 데이터중 앞이나 뒤에 공백이 있는 경우가 존재 -> db에 적재하는 과정에서 오류 발생
## -> 공백 제거
route_raw = final_df[['노선번호', '노선명', '교통수단타입코드', '교통수단타입명']].copy()

route_raw['교통수단타입명'] = route_raw['교통수단타입명'].astype(str).str.strip()
final_df['교통수단타입명'] = final_df['교통수단타입명'].astype(str).str.strip()


route_key_cols = ['노선번호', '노선명', '교통수단타입코드', '교통수단타입명']

route = (
    route_raw
    .sort_values(route_key_cols)
    .drop_duplicates(subset=route_key_cols)
    .reset_index(drop=True)
)
route['route_id'] = route.index + 1  # 대체키

# MySQL 적재
route_for_db = route.rename(columns={
    '노선번호': 'route_no',
    '노선명': 'route_name',
    '교통수단타입코드': 'vehicle_type_cd',
    '교통수단타입명': 'vehicle_type_nm',
})
route_for_db.to_sql('route', engine, if_exists='append', index=False)
# {'route_no': 'N37', 'route_name': 'N37번(진관공영차고지~송파공영차고지)', ...}, 
# {'route_no': 'N37', 'route_name': 'N37번(송파공영차고지~진관공영차고지)', ...}
# 위와 같이 다른 행이지만 MySQL의 UNIQUE 제약조건이 걸려 N37이 들어갈때 걸림 
# if_exists='append' + UNIQUE 제약조건 제거


# route_id로 매핑할 수 있는 컬럼들 (복합키(route_key_cols) → route_id)
# 예: ('470', '470번(상암차고지~안골마을)', '10', '서울간선버스') == (route_id = 1)
route_map = route.set_index(route_key_cols)['route_id']



# 4. stop 테이블 생성
stop = (
    final_df[['표준버스정류장ID', '버스정류장ARS번호', '역명']]
    .drop_duplicates()
    .reset_index(drop=True)
)
stop['stop_id'] = stop.index + 1    # 대체키

stop_for_db = stop.rename(columns={
    '표준버스정류장ID': 'standard_stop_id',
    '버스정류장ARS번호': 'ars_id',
    '역명': 'stop_name',
})
stop_for_db.to_sql('bus_stop', engine, if_exists='append', index=False)

# stop_id로 매핑할 수 있는 컬럼들(복합키 (stop_key_cols) -> stop_id)
stop_key_cols = ['표준버스정류장ID', '버스정류장ARS번호', '역명']
stop_map = stop.set_index(stop_key_cols)['stop_id']


# 5. dim_date 테이블 생성 (사용년월 기준)
dim_date = final_df[['사용년월']].drop_duplicates().reset_index(drop=True)
dim_date['year'] = (dim_date['사용년월'] // 100).astype(int)    # 2024, 2025..
dim_date['month'] = (dim_date['사용년월'] % 100).astype(int)    # 1, 2, 3 ..
dim_date['ymd'] = pd.to_datetime(
    dim_date['year'].astype(str) + '-' + dim_date['month'].astype(str) + '-01'
)   # 예시 : 2025-01-01
dim_date['date_id'] = dim_date.index + 1    # 대체키 부여

# 
dim_date_for_db = dim_date[['date_id', 'ymd', 'year', 'month']]
dim_date_for_db.to_sql('dim_date', engine, if_exists='append', index=False)

# dim_date_id로 매핑 할 수 있는 컬럼(사용년월 -> date_id)
dim_date_map = dim_date.set_index('사용년월')['date_id']


# 6. ridership 테이블 생성
#    (route_id / stop_id / dim_date_id FK 붙이기)


# route_id 붙이기
final_df = final_df.merge(
    route[route_key_cols + ['route_id']],   # route에서 필요한 컬럼만
    on=route_key_cols,
    how='left'
)


# 🔍 route_id가 NULL인 문제 row 찾기 (디버깅용)
missing_routes = final_df[final_df['route_id'].isna()][
    ['노선번호', '노선명', '교통수단타입코드', '교통수단타입명']
].drop_duplicates()

print("🔍 매칭 실패한 route key 목록:")
print(missing_routes.head(50))
print("총 개수:", len(missing_routes))


# stop_id 붙이기
final_df = final_df.merge(
    stop[stop_key_cols + ['stop_id']],
    on=stop_key_cols,
    how='left'
)


# date_id 붙이기
final_df['date_id'] = final_df['사용년월'].map(dim_date_map)


# ridership 테이블 형태로 정리
ridership = final_df[['route_id', 'stop_id', 'date_id', '시간', '승하차구분', '승객수']].copy()
ridership.rename(columns={
    '시간': 'hour',
    '승하차구분': 'ride_type',
    '승객수': 'passenger_cnt',
}, inplace=True)


# 7. ridership 테이블에 대량 적재 (chunksize 사용 - 데이터가 너무 많아 10만행씩 나눠서 적재)
ridership.to_sql(
    'ridership',
    engine,
    if_exists='append',
    index=False,
    chunksize=100_000  # 10만 행씩 나눠서 넣기
)

print("✅ MySQL 적재 완료")
