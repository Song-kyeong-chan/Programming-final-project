import pandas as pd
import sqlite3
import glob

# 1. 파일 읽기: stores
store_files = sorted(glob.glob("../data/stores_*.csv")) # 모든 csv 파일 정렬
store_list = []

for file in store_files:
    try:
        df = pd.read_csv(file)
        # 빈 파일 제외, "store_id", "가게명", "영업시간", "전화번호", "주소"포함 컬럼 파일
        if not df.empty and {"store_id", "가게명", "영업시간", "전화번호", "주소"}.issubset(df.columns):
            store_list.append(df)
        else:
            print(f"무시된 store 파일: {file}")
    except Exception as e:
        print(f"읽기 실패: {file} - {e}")

if not store_list:
    raise ValueError("유효한 store 파일이 없습니다.")

# 병합 및 중복 제거
stores_df = pd.concat(store_list, ignore_index=True) # 여러 파일 하나로 통합
stores_df.drop_duplicates(subset=["가게명", "전화번호", "주소"], inplace=True) # 가게명,전화번호,주소 중복 제거
stores_df.drop_duplicates(subset=["store_id"], inplace=True)  # store_id 중복 제거
stores_df.reset_index(drop=True, inplace=True)

# 2. 파일 읽기: menus (store_id 기준)
menu_files = sorted(glob.glob("../data/menus_*.csv")) # 모든 csv파일 정렬
menu_list = []

for file in menu_files:
    try:
        df = pd.read_csv(file)
        # 빈 파일 제외, "store_id", "가게명", "영업시간", "전화번호", "주소"포함 컬럼 파일
        if not df.empty and {"store_id", "메뉴명", "가격"}.issubset(df.columns):
            df = df[["store_id", "메뉴명", "가격"]] # df 생성
            menu_list.append(df)
        else:
            print(f"[열 누락] {file} - 포함된 열: {df.columns.tolist()}")
    except Exception as e:
        print(f"읽기 실패: {file} - {e}")

# 병합 및 중복 제거	
if menu_list:
    menus_df = pd.concat(menu_list, ignore_index=True)
    menus_df.drop_duplicates(subset=["store_id", "메뉴명", "가격"], inplace=True)
else:
    print("메뉴 정보가 없어 빈 menus 테이블로 생성합니다.")
    menus_df = pd.DataFrame(columns=["store_id", "메뉴명", "가격"])

# 3. 유효성 필터링: store_id가 실제 존재하는 것만 메뉴에 남기기
valid_store_ids = set(stores_df["store_id"])
menus_df = menus_df[menus_df["store_id"].isin(valid_store_ids)]

# 4. SQLite 저장 
conn = sqlite3.connect("yogiyo.db")
cursor = conn.cursor()

# store_id만 interger - 중복 방지
cursor.execute('''
CREATE TABLE IF NOT EXISTS stores (
    store_id INTEGER PRIMARY KEY,
    가게명 TEXT,
    영업시간 TEXT,
    전화번호 TEXT,
    주소 TEXT
)
''')
# 중복 방지 - 외래키 사용 : 데이터의 무결성 확보 / menus 테이블 -> stores 테이블 id 참조 store_id(menus) = menus.menu
cursor.execute('''
CREATE TABLE IF NOT EXISTS menus ( 
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id INTEGER,
    메뉴명 TEXT,
    가격 TEXT,
    FOREIGN KEY(store_id) REFERENCES stores(store_id)
)
''')

stores_df.to_sql("stores", conn, if_exists="replace", index=False)
menus_df.to_sql("menus", conn, if_exists="replace", index=False)

conn.commit()
conn.close()

print(f"SQLite 저장 완료 (가게 {len(stores_df)}개, 메뉴 {len(menus_df)}개)")
