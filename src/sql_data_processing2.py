import pandas as pd
import sqlite3

# 1. CSV 파일 불러오기 (카페 데이터)
menus_cafe = pd.read_csv("../data/menus_cafe.csv")
stores_cafe = pd.read_csv("../data/stores_cafe.csv")
cafe_store_names = set(stores_cafe["가게명"].unique()) # set: 중복방지

# 2. 기존 DB에서 데이터 불러오기
conn = sqlite3.connect("../db/yogiyo.db")
stores_df = pd.read_sql("SELECT * FROM stores;", conn)
menus_df = pd.read_sql("SELECT * FROM menus;", conn)

# 3. 가게명 기반으로 중복된 카페 store_id 리스트 추출
duplicated_store_ids = stores_df[stores_df["가게명"].isin(cafe_store_names)]["store_id"].tolist()

# 4. 카페 store_id 제거한 음식점 데이터 생성
filtered_stores_df = stores_df[~stores_df["store_id"].isin(duplicated_store_ids)]
filtered_menus_df = menus_df[~menus_df["store_id"].isin(duplicated_store_ids)]

## 5. 새 DB 생성 및 저장
new_db_path = "../data/yogiyo_separated.db"
conn_new = sqlite3.connect(new_db_path)

# 음식점 테이블 저장
filtered_stores_df.to_sql("stores_food", conn_new, index=False, if_exists="replace")
filtered_menus_df.to_sql("menus_food", conn_new, index=False, if_exists="replace")

# 카페 테이블 저장
stores_cafe.to_sql("stores_cafe", conn_new, index=False, if_exists="replace")
menus_cafe.to_sql("menus_cafe", conn_new, index=False, if_exists="replace")

conn_new.commit()
conn_new.close()

print(f" 새 DB 생성 완료: {new_db_path}")
