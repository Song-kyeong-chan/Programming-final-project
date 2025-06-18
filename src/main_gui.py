import sqlite3
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
import koreanize_matplotlib

# DB 연결 / 음식점정보,음식점 메뉴, 카페정보, 카페메뉴
conn = sqlite3.connect("../db/yogiyo_separated.db")
stores_food = pd.read_sql("SELECT * FROM stores_food;", conn)
menus_food = pd.read_sql("SELECT * FROM menus_food;", conn)
stores_cafe = pd.read_sql("SELECT * FROM stores_cafe;", conn)
menus_cafe = pd.read_sql("SELECT * FROM menus_cafe;", conn)

# 음식점 / 카페 통합 하여 데이터프레임 생성
df_food = pd.merge(menus_food, stores_food, on="store_id")
df_cafe = pd.merge(menus_cafe, stores_cafe, on="store_id")

def classify_region(address):
    if "덕진동1가" in address:
        return "구정문"
    elif "금암동" in address:
        return "신정문/사대부고"
    return "기타"

# 데이터 프레임에 지역 컬럼 추가
df_food["지역"] = df_food["주소"].apply(classify_region)
df_cafe["지역"] = df_cafe["주소"].apply(classify_region)

# 라벨 이름 최소 최대 설정
def shorten_label(store, menu, max_store=8, max_menu=10):
    s = store if len(store) <= max_store else store[:max_store] + "..."
    m = menu if len(menu) <= max_menu else menu[:max_menu] + "..."
    return f"{s} ({m})"

# 검색 처리
def search_and_display(df, region_var, entry, output_box):
    keyword = entry.get() # 사용자 입력한 메뉴 키워드 추출
    region = region_var.get() # 라디오 버튼에서 선택한 지역 추출
    filtered = df.copy() # 원본 데이터프레임 복사 (필터링용)

    if region:
        filtered = filtered[filtered["지역"] == region]
    if keyword:
        filtered = filtered[filtered["메뉴명"].str.contains(keyword)]

    # 텍스트 레이블에 가격 오름차순별로 표시 
    try:
        filtered["가격_숫자"] = filtered["가격"].str.replace("원", "").str.replace(",", "").astype(int)
    except:
        messagebox.showerror("오류", "가격 데이터 변환 실패")
        return
    filtered = filtered.sort_values("가격_숫자")
    output_box.config(state='normal')
    output_box.delete("1.0", tk.END)

    if filtered.empty:
        output_box.insert(tk.END, "검색 결과가 없습니다.\n") # 출력창 편집 가능하게 설정
        output_box.config(state='disabled') # 이전 검색 결과 삭제 (초기화)
        return

    for _, row in filtered.iterrows():
        output_box.insert(tk.END, f"[{row['가게명']}]\n")
        output_box.insert(tk.END, f" - 메뉴명: {row['메뉴명']}\n")
        output_box.insert(tk.END, f" - 가격: {row['가격_숫자']}\n")
        output_box.insert(tk.END, f" - 영업시간: {row['영업시간'] or 'None'}\n")
        output_box.insert(tk.END, f" - 전화번호: {row['전화번호'] or 'None'}\n\n")
    output_box.config(state='disabled')

    show_graph_window(filtered)

# 그래프 별도 창 표시
def show_graph_window(filtered):
    # TOP 10
    top10 = filtered.sort_values("가격_숫자").head(10)
    fig1, ax1 = plt.subplots(figsize=(6.5, 4))
    labels1 = [f"{row['가게명']} ({row['메뉴명']})" for _, row in top10.iterrows()]
    prices1 = top10["가격_숫자"].tolist()
    colors1 = ['red'] + ['orange'] * min(2, len(prices1)-1) + ['gray'] * (len(prices1)-3)

    bars1 = ax1.barh(labels1, prices1, color=colors1)
    ax1.set_title("가격 낮은 TOP 10", fontsize=11)
    ax1.set_xlabel("가격 (원)", fontsize=9)
    ax1.tick_params(axis='both', labelsize=7)
    ax1.invert_yaxis()

    for bar, price in zip(bars1, prices1):
        ax1.text(bar.get_width() + 100, bar.get_y() + bar.get_height()/2, f"{price:,}원", fontsize=7, va='center')

    # TOP 3
    top3 = filtered.sort_values("가격_숫자", ascending=False).head(3)
    fig2, ax2 = plt.subplots(figsize=(6.5, 2.5))
    labels2 = [f"{row['가게명']} ({row['메뉴명']})" for _, row in top3.iterrows()]
    prices2 = top3["가격_숫자"].tolist()
    bars2 = ax2.barh(labels2, prices2, color='darkblue')
    ax2.set_title("가격 높은 TOP 3", fontsize=10)
    ax2.set_xlabel("가격 (원)", fontsize=8)
    ax2.tick_params(axis='both', labelsize=7)
    ax2.invert_yaxis()

    for bar, price in zip(bars2, prices2):
        ax2.text(bar.get_width() + 100, bar.get_y() + bar.get_height()/2, f"{price:,}원", fontsize=7, va='center')

    plt.show()


# GUI
root = tk.Tk()
root.title("전북대 인근 상권 메뉴 가격 비교")
root.geometry("950x700")

notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True)

def create_tab(tab_name, df):
    frame = ttk.Frame(notebook)
    notebook.add(frame, text=tab_name)

    region_var = tk.StringVar()
    tk.Label(frame, text="메뉴 검색어:").pack()
    entry = tk.Entry(frame)
    entry.pack()
    tk.Label(frame, text="지역 선택:").pack()
    tk.Radiobutton(frame, text="구정문", variable=region_var, value="구정문").pack()
    tk.Radiobutton(frame, text="신정문/사대부고", variable=region_var, value="신정문/사대부고").pack()

    tk.Label(frame, text="검색 결과 리스트").pack()
    output_box = tk.Text(frame, height=15, width=110)
    output_box.pack()
    output_box.config(state='disabled')

    tk.Button(
        frame,
        text="🔍 검색 및 그래프 보기",
        font=("Helvetica", 12, "bold"),
        bg="#4CAF50", fg="white",
        command=lambda: search_and_display(df, region_var, entry, output_box)
    ).pack(pady=10)

create_tab("🍚 음식점", df_food)
create_tab("☕ 카페", df_cafe)

root.mainloop()
