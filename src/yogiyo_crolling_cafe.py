import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

#  설정 
SCROLL_LIMIT = 6
SAVE_INTERVAL = 3
COUNT = 54 # 임의 설정 f'stores_{count}', 'menus_{count}'
필요_지점키워드 = ['전북대', '덕진', '금암', '본점', '1호', '2호']
필요_주소키워드 = ['금암동', '덕진동1가']
전체_가게목록 = []
scroll_num = 0
total_count = 0
store_id_counter = 1
# 드라이버 설정 
def setup_driver():
    options = Options()
    prefs = {
        "profile.default_content_setting_values.geolocation": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.images": 2,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=options)

# 주소 설정
def set_location(driver, address = "전북특별자치도 전주시 덕진구 금암동 663 전북대학교"):
    driver.get("https://www.yogiyo.co.kr/mobile/#/")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "form-control")))
    input_field = driver.find_element(By.CLASS_NAME, "form-control")
    input_field.clear()
    input_field.send_keys(address)
    input_field.send_keys(Keys.ENTER)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="search"]/div/form/ul/li[3]/a'))
    ).click()
    time.sleep(2)

# 저장 
def save_partial(data, count):
    global store_id_counter
    print(f"{count}회차 데이터 저장 완료")
    # 전체 크롤링 데이터 -> DataFrame
    df = pd.DataFrame(data)
    # 가게 정보 분리 및 store_id 부여
    stores = {}
    menus = []
    # itertuple()이 속성값으로 전달하여 훨씬 빠르지만 혹시나 컬럼 공백 대응 하기 위해 iterrows() 사용
    for _, row in df.iterrows():
        store_key = (
            row["가게명"],
            row["영업시간"],
            row["전화번호"],
            row["주소"]
        )

        if store_key not in stores:
            stores[store_key] = store_id_counter
            store_id_counter += 1

        store_id = stores[store_key]
        menus.append({
            "store_id": store_id,
            "메뉴명": row["메뉴명"],
            "가격": row["가격"]
        })

    #  DataFrame 생성
        store_rows = []
        for key, sid in stores.items():
            store_rows.append({
                "store_id": sid,
                "가게명": key[0],
                "영업시간": key[1],
                "전화번호": key[2],
                "주소": key[3]
            })
    stores_df = pd.DataFrame(store_rows)
    menus_df = pd.DataFrame(menus)

    # 저장
    stores_df.to_csv(f"../data/stores_cafe_{count}.csv", index=False, encoding="utf-8-sig")
    menus_df.to_csv(f"../data/menus_cafe_{count}.csv", index=False, encoding="utf-8-sig")

    print("저장 완료: stores.csv / menus.csv")

#  스크롤
def perform_scroll(driver, count):
    for _ in range(count):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        
# 스크롤 + 수집
def scroll_and_collect(driver, store_data):
    global scroll_num, total_count
    # 카페/디저트 클릭
    WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, '//*[@id="category"]/ul/li[13]'))).click()
    # 스크롤 후 카페 클릭
    perform_scroll(driver, scroll_num)
    cards = driver.find_elements(By.CSS_SELECTOR, "div.item.clearfix")
    new_cards = cards[len(전체_가게목록):] # 기존 전체 가게 저장 목록 끝부터 : 스크롤 후 최신 목록 끝
    print(f'전체가게목록: {len(전체_가게목록)},new_cards 개수: {len(new_cards)}') # new_cards = 0 확인용도
    전체_가게목록.extend(new_cards)
    print(f'수정된 전체 가게목록: {len(전체_가게목록)}')
    for i in range(len(new_cards)):
        try:
            
            total_count +=1 # 가게에 고유값 매겨서 지나간 가게 있는 지 버그 확인용도
            cards = driver.find_elements(By.CSS_SELECTOR, "div.item.clearfix") # 요기요 홈페이지 새로 고침 후 버그 방지
            card = cards[len(전체_가게목록) - len(new_cards) + i] # card: 스크롤 후 새로 나타난 가게[index] 단일 element
            name_elem = card.find_element(By.CLASS_NAME, "restaurant-name")
            store_name = name_elem.text.strip()
            should_enter = '-' not in store_name or any(kw in store_name.split('-')[-1] for kw in 필요_지점키워드) # 리스트 컴프리헨션 사용
            if not should_enter:
                continue
            # 가게 클릭
            driver.execute_script("arguments[0].scrollIntoView();", card)
            WebDriverWait(driver, 3).until(EC.element_to_be_clickable(name_elem))
            driver.execute_script("arguments[0].click();", name_elem)
            time.sleep(1.5)

            # 가게 정보
            info_tab = driver.find_element(By.XPATH, '//*[@id="content"]/div[2]/div[1]/ul/li[3]/a')
            info_tab.click()
            time.sleep(1)
            info_spans = driver.find_elements(By.CSS_SELECTOR, "span.tc.ng-binding")
            print(f"[DEBUG] info_spans 길이: {len(info_spans)}")
            try:
                영업시간 = info_spans[1].text.strip()
            except IndexError:
                    영업시간 = "정보 없음"
            try:
                    전화번호 = info_spans[2].text.strip()
            except IndexError:
                    전화번호 = "정보 없음"
            try:
                    주소 = info_spans[3].text.strip()
            except IndexError:
                    주소 = "정보 없음"

            # 주소 일치 확인
            if not any(kw in 주소 for kw in 필요_주소키워드):
                print(f"주소 조건 불일치: {store_name} - {주소} {total_count}")
                driver.back()
                perform_scroll(driver, scroll_num)
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "restaurant-name")))
                time.sleep(2)
                continue

            # 메뉴 수집
            len_menu = 0
            menu_tab = driver.find_element(By.XPATH, '//*[@id="content"]/div[2]/div[1]/ul/li[1]/a')
            menu_tab.click()
            time.sleep(1)
            category_links = driver.find_elements(By.CSS_SELECTOR, "div.panel-heading a.clearfix")
            for link in category_links[2:]: # 0,1번은 대표메뉴로 메뉴리스트 중복
                try:
                    len_menu+=1
                    driver.execute_script("arguments[0].scrollIntoView();", link)
                    WebDriverWait(driver, 2).until(EC.element_to_be_clickable(link))
                    link.click()
                    time.sleep(0.3)
                except Exception as e:
                    print(f"카테고리 클릭 실패: {e}")
                    break

            panel_bodies = driver.find_elements(By.CSS_SELECTOR, "div.panel-body")
            for body in panel_bodies[1:]: # 0번은 메뉴 소개 text
                try:
                    menu_rows = body.find_elements(By.CSS_SELECTOR, "td.menu-text")
                    for row in menu_rows:
                        name = row.find_element(By.CLASS_NAME, "menu-name").text.strip()
                        price = row.find_element(By.CSS_SELECTOR, "div.menu-price > span.ng-binding").text.strip()
                        store_data.append({
                            "가게명": store_name,
                            "메뉴명": name,
                            "가격": price,
                            "영업시간": 영업시간,
                            "전화번호": 전화번호,
                            "주소": 주소
                        })
                except Exception as e:
                    print(f"메뉴 수집 실패: {e}")
                    break
            print(f"저장됨: {store_name} ({len_menu}메뉴) {total_count}")
            driver.back()
            perform_scroll(driver, scroll_num) 
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "restaurant-name")))
            time.sleep(2)


        except StaleElementReferenceException:
            print("stale element - 새로고침 필요")
            continue
        except Exception as e:
            if 'invalid session id' in str(e).lower():
                print(f"세션 만료. 중단: {e}")
                return False
            continue
    scroll_num +=1
    return True

# 메인 실행
def run():
    total_data = []
    # 스크롤 제한과 저장 간격을 정해 stores 정보를 파일에 저장
    for i in range(SCROLL_LIMIT):
        if i % SAVE_INTERVAL == 0:
            if i != 0:
                time.sleep(5)
                driver.quit()
                save_partial(total_data, COUNT+ i) # menus_{count}.csv , stores_{count}.csv의 count는 임의로 지정
                total_data = []
            driver = setup_driver()
            set_location(driver)
        result = scroll_and_collect(driver, total_data)
        if result is False:
            break

    driver.quit()
    if total_data:
        save_partial(total_data, SCROLL_LIMIT+COUNT)

if __name__ == "__main__":
    run()
