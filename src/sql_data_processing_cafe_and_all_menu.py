import pandas as pd
import sqlite3
import glob

# 1. 파일 읽기: stores
store_cafe_files = sorted(glob.glob("../data/stores_cafe*.csv")) # 모든 csv 파일 정렬
store_cafe_list = []

for file in store_cafe_files:
    try:
        df = pd.read_csv(file)
        # 빈 파일 제외, "store_id", "가게명", "영업시간", "전화번호", "주소"포함 컬럼 파일
        if not df.empty and {"store_id", "가게명", "영업시간", "전화번호", "주소"}.issubset(df.columns):
            store_cafe_list.append(df)
        else:
            print(f"무시된 store 파일: {file}")
    except Exception as e:
        print(f"읽기 실패: {file} - {e}")

if not store_cafe_list:
    raise ValueError("유효한 store 파일이 없습니다.")

# 병합 및 중복 제거
stores_cafe_df = pd.concat(store_cafe_list, ignore_index=True) # 여러 파일 하나로 통합
stores_cafe_df.drop_duplicates(subset=["가게명", "전화번호", "주소"], inplace=True) # 가게명,전화번호,주소 중복 제거
stores_cafe_df.drop_duplicates(subset=["store_id"], inplace=True)  # store_id 중복 제거
stores_cafe_df.reset_index(drop=True, inplace=True)

# 2. 파일 읽기: menus (store_id 기준)
menu_cafe_files = sorted(glob.glob("../data/menus_cafe_*.csv")) # 모든 csv파일 정렬
menu_cafe_list = []

for file in menu_cafe_files:
    try:
        df = pd.read_csv(file)
        # 빈 파일 제외, "store_id", "가게명", "영업시간", "전화번호", "주소"포함 컬럼 파일
        if not df.empty and {"store_id", "메뉴명", "가격"}.issubset(df.columns):
            df = df[["store_id", "메뉴명", "가격"]] # df 생성
            menu_cafe_list.append(df)
        else:
            print(f"[열 누락] {file} - 포함된 열: {df.columns.tolist()}")
    except Exception as e:
        print(f"읽기 실패: {file} - {e}")

# 병합 및 중복 제거	
if menu_cafe_list:
    menus_cafe_df = pd.concat(menu_cafe_list, ignore_index=True)
    menus_cafe_df.drop_duplicates(subset=["store_id", "메뉴명", "가격"], inplace=True)
else:
    print("메뉴 정보가 없어 빈 menus 테이블로 생성합니다.")
    menus_cafe_df = pd.DataFrame(columns=["store_id", "메뉴명", "가격"])

cafe_store_names = set(stores_cafe_df["가게명"].unique()) # set: 중복방지

# 3. 기존 DB에서 데이터 불러오기
conn = sqlite3.connect("../db/yogiyo.db")
stores_df = pd.read_sql("SELECT * FROM stores;", conn)
menus_df = pd.read_sql("SELECT * FROM menus;", conn)

# 4. 가게명 기반으로 중복된 카페 store_id 리스트 추출
duplicated_store_ids = stores_df[stores_df["가게명"].isin(cafe_store_names)]["store_id"].tolist()

# 5. 카페 store_id 제거한 음식점 데이터 생성
filtered_stores_df = stores_df[~stores_df["store_id"].isin(duplicated_store_ids)]
filtered_menus_df = menus_df[~menus_df["store_id"].isin(duplicated_store_ids)]

## 6. 새 DB 생성 및 저장
new_db_path = "../data/yogiyo_separated.db"
conn_new = sqlite3.connect(new_db_path)

# 음식점 테이블 저장
filtered_stores_df.to_sql("stores_food", conn_new, index=False, if_exists="replace")
filtered_menus_df.to_sql("menus_food", conn_new, index=False, if_exists="replace")

# 카페 테이블 저장
stores_cafe_df.to_sql("stores_cafe", conn_new, index=False, if_exists="replace")
menus_cafe_df.to_sql("menus_cafe", conn_new, index=False, if_exists="replace")

conn_new.commit()
conn_new.close()

print(f" 새 DB 생성 완료: {new_db_path}")
