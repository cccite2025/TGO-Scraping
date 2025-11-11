# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import warnings
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
import json
import re
import joblib # เพิ่ม import นี้

# --- 1. การตั้งค่าเริ่มต้น ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
BASE_URL = "https://thaicarbonlabel.tgo.or.th/"

# --- 2. เชื่อมต่อ SUPABASE ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ เชื่อมต่อ Supabase สำเร็จ!")
except Exception as e:
    print(f"❌ เชื่อมต่อ Supabase ไม่สำเร็จ: {e}")
    exit()
# ... connect Supabase (ส่วนที่ 2) ...
print("✅ เชื่อมต่อ Supabase สำเร็จ!")

# --- [ใหม่] โหลดโมเดล AI ที่เราสร้างไว้ ---
try:
    print("🧠 กำลังโหลดโมเดล AI สำหรับจัดหมวดหมู่...")
    category_classifier = joblib.load('category_model.joblib')
    print("✅ โหลดโมเดล AI สำเร็จ!")
except FileNotFoundError:
    print("⚠️ ไม่พบไฟล์ 'category_model.joblib', จะใช้หมวดหมู่ 'อื่นๆ' แทน")
    category_classifier = None
except Exception as e:
    print(f"❌ เกิดข้อผิดพลาดในการโหลดโมเดล AI: {e}")
    category_classifier = None

# --- 3. ฟังก์ชันแปลงวันที่ ---
def convert_be_to_iso(be_date_str):
    if not be_date_str or be_date_str == '-': return None
    try:
        parts = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", be_date_str)
        if not parts: return None
        day, month, be_year = parts.group(1).zfill(2), parts.group(2).zfill(2), int(parts.group(3))
        if be_year < 2500: return None
        ce_year = be_year - 543
        return f"{ce_year}-{month}-{day}"
    except (ValueError, AttributeError):
        return None

# --- 4. ฟังก์ชันดึงข้อมูล (Selenium + รอ ตาราง หรือ ไม่พบข้อมูล/รายการ) ---
def fetch_tgo_data_with_selenium(url_to_fetch):
    """
    ใช้ Selenium เพื่อโหลด URL ที่ระบุ และรอ table หรือ no results (Timeout 3 นาที)
    """
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless') # รันแบบเห็นหน้าต่าง
    options.add_argument('--disable-gpu')
    options.add_argument('window-size=1280x720')
    options.add_argument("--log-level=3")
    driver = None
    try:
        print("     กำลังเปิดเบราว์เซอร์ (Selenium)...") # เพิ่มเว้นวรรค
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(180) # 3 นาทีต่อหน้า
        driver.implicitly_wait(5)

        print(f"     กำลังเข้าไปที่: {url_to_fetch}")
        driver.get(url_to_fetch)

        print("     รอให้หน้าเว็บโหลด...")
        wait = WebDriverWait(driver, 180) # 3 นาทีต่อหน้า

        table_selector = (By.CLASS_NAME, 'catalog-table')
        
        # --- 🎯 [แก้ไข] ---
        # เปลี่ยนเป็น XPATH เพื่อให้รองรับการตรวจสอบข้อความได้หลายแบบ
        # (ตรวจสอบ class 'alert-warning' หรือ ข้อความ 'ไม่พบข้อมูล' หรือ ข้อความ 'ไม่พบรายการ')
        no_results_selector = (By.XPATH, "//*[contains(@class, 'alert-warning') or contains(text(), 'ไม่พบข้อมูล') or contains(text(), 'ไม่พบรายการ')]")
        # --- 🎯 [สิ้นสุดการแก้ไข] ---

        # [แก้ไข] อัปเดต Log ให้สื่อความหมาย
        print(f"     รอให้ '{table_selector[1]}' หรือ 'ข้อความไม่พบข้อมูล/รายการ' ปรากฏ...")

        element_found = wait.until(
            EC.any_of(
                EC.presence_of_element_located(table_selector),
                EC.presence_of_element_located(no_results_selector) # <-- ใช้ตัวเลือกใหม่
            )
        )

        try:
            driver.find_element(*table_selector) # ลองหาตาราง
            print("     -> พบตารางข้อมูล!")
            print("     รอเพิ่มเติม 5 วินาที...")
            time.sleep(5)
            print("     ✅ ตารางโหลดสำเร็จ! กำลังดึงโค้ด HTML...")
            return driver.page_source
        except NoSuchElementException:
            # [แก้ไข] อัปเดต Log 
            print("     -> ไม่พบตาราง (เจอข้อความ 'ไม่พบข้อมูล' หรือ 'ไม่พบรายการ')")
            return None # คืนค่า None ถ้าไม่มีข้อมูล

    except TimeoutException:
         current_state = "unknown"
         try:
             if driver: current_state = driver.execute_script('return document.readyState;')
         except: pass
         if current_state != 'complete':
             print(f"     ❌ เกิดข้อผิดพลาด: Timeout! หน้าเว็บโหลดไม่เสร็จ (State: {current_state}) ภายใน 3 นาที")
         else:
             print(f"     ❌ เกิดข้อผิดพลาด: Timeout! ไม่พบทั้งตารางและข้อความ 'ไม่พบข้อมูล/รายการ' ภายใน 3 นาที")
         return None
    except Exception as e:
        print(f"     ❌ เกิดข้อผิดพลาดระหว่างการทำงานของ Selenium: {e}")
        return None
    finally:
        if driver:
            print("     ปิดเบราว์เซอร์...")
            driver.quit()

# --- 5. [แก้ไข] Dictionary คำสำคัญ (แบบจัดลำดับความสำคัญ) ---
CATEGORIES_KEYWORDS = {
    # 🎯 หมวดหลัก (คำเฉพาะ จะถูกให้คะแนนสูง)
    "ปูนซีเมนต์และผลิตภัณฑ์คอนกรีต": {
        "priority": ['ปูนซีเมนต์', 'ซีเมนต์', 'ปูนไฮดรอลิก', 'คอนกรีตผสมเสร็จ', 'มอร์ตาร์', 'ปูนก่อ', 'ปูนฉาบ', 'ปูนเท', 'อิฐบล็อก', 'บล็อกคอนกรีต'],
        "secondary": ['ปูน', 'คอนกรีต', 'ก่อ', 'ฉาบ', 'เท', 'บล็อก']
    },
    "ผลิตภัณฑ์เหล็ก": {
        "priority": ['เหล็กเส้น', 'เหล็กรูปพรรณ', 'ไวร์เมช', 'ตะแกรงเหล็ก', 'ลวดเหล็ก'],
        "secondary": ['เหล็ก', 'ตะแกรง', 'ลวด']
    },
    "กระเบื้องและเซรามิก": {
        "priority": ['กระเบื้องเซรามิก', 'แกรนิตโต้', 'กระเบื้องปูพื้น', 'กระเบื้องบุผนัง'],
        "secondary": ['กระเบื้อง', 'เซรามิก']
    },
    "สีและเคมีภัณฑ์": {
        "priority": ['สีทาอาคาร', 'สีรองพื้น', 'กันซึม', 'กาวยาแนว', 'เคมีภัณฑ์ก่อสร้าง', 'กาวซีเมนต์'],
        "secondary": ['สี', 'สีทา', 'เบส', 'รองพื้น', 'กาว', 'ยาแนว']
    },
    "วัสดุมุงหลังคา": {
        "priority": ['เมทัลชีท', 'กระเบื้องหลังคา', 'ซีแพค', 'ลอนคู่'],
        "secondary": ['หลังคา', 'ลอน']
    },
    "ฉนวนกันความร้อน": {
        "priority": ['ฉนวนใยแก้ว', 'ฉนวนใยหิน', 'พียูโฟม', 'PU Foam'],
        "secondary": ['ฉนวน']
    },
    "ประตูและหน้าต่าง": {
        "priority": ['ประตู', 'หน้าต่าง', 'วงกบ', 'uPVC', 'อลูมิเนียม'], # หมวดนี้คำค่อนข้างเฉพาะอยู่แล้ว
        "secondary": []
    },
    "กระจก": {
        "priority": ['กระจก'], # คำเฉพาะ
        "secondary": []
    },
    "สุขภัณฑ์": {
        "priority": ['สุขภัณฑ์', 'ชักโครก', 'อ่างล้างหน้า', 'ก๊อก'], # คำเฉพาะ
        "secondary": []
    },
    
    # 🎯 หมวดรอง (คำทั่วไป จะถูกให้คะแนนต่ำ)
    "วัสดุผนังและฝ้า": {
        "priority": ['ยิปซั่ม', 'แผ่นฝ้า', 'สมาร์ทบอร์ด', 'วีว่าบอร์ด'],
        "secondary": ['ผนัง', 'ฝ้า', 'ปูพื้น', 'บุผนัง'] # คำกว้างๆ ที่อาจซ้ำกับหมวดอื่น
    }
}
# --- 6. ฟังก์ชันแยกข้อมูล ([แก้ไข] ใช้โมเดล AI แทน Keyword) ---
def parse_product_data(html_content, year_be, quarter): # เพิ่ม quarter สำหรับ Debug
    if not html_content: return []
    print(f"   กำลังแยกข้อมูล (Parsing) ปี {year_be} ไตรมาส {quarter} แบบการ์ด...")
    # ... (โค้ด BeautifulSoup, ค้นหา main_table, product_rows เหมือนเดิม) ...
    soup = BeautifulSoup(html_content, 'html.parser')
    all_products = []
    main_table = soup.find('table', class_='catalog-table') 
    if not main_table:
        print(f"   ⚠️ ไม่พบตารางหลัก 'catalog-table' ในปี {year_be}/Q{quarter}!")
        return []
    product_rows = main_table.find('tbody').find_all('tr', recursive=False)
    if not product_rows:
        print(f"   ⚠️ พบตารางหลัก แต่ไม่พบแถว (tr) โดยตรงในปี {year_be}/Q{quarter}!")
        return []

    processed_count = 0
    for i, row in enumerate(product_rows):
        # ... (โค้ดตั้งค่าตัวแปรเริ่มต้นเหมือนเดิม) ...
        table = row.find('table', class_='catalog-template')
        if not table: continue
        
        product_id = f"CFP_Y{year_be}Q{quarter}_R{i+1}"; 
        label_logo_type = "UNKNOWN"; product_name = None; functional_unit = None; scope = None; company_name = None; contact_person = None; phone = None; email = None; image_url = 'N/A'; detail_page_url = None; 
        carbon_value = None; carbon_unit = None; 
        cert_start_date_iso = None; cert_end_date_iso = None
        category = "อื่นๆ" # 🎯 เริ่มต้นเป็น "อื่นๆ"

        try:
            # ... (โค้ดดึง ID, ดึง H1 (product_name) เหมือนเดิม) ...
            header_span = table.find('th', class_='catalog-header').find('span')
            if header_span and header_span.text.strip():
                real_id = header_span.text.strip()
                product_id = real_id 
                if "CFR" in real_id: label_logo_type = "CFR"
                elif "CFP" in real_id: label_logo_type = "CFP"
            else:
                if "CFR" in product_id: label_logo_type = "CFR"
                elif "CFP" in product_id: label_logo_type = "CFP"
            
            name_tag = table.find('h1')
            if name_tag: product_name = name_tag.text.strip()
            
            # --- 🎯 [แก้ไข] ตรรกะการจัดหมวดหมู่ (ใช้ AI) ---
            if product_name and category_classifier: # 1. เช็คว่ามีชื่อ และ โหลดโมเดลสำเร็จ
                try:
                    # 2. "ถาม" โมเดล AI (ส่งชื่อผลิตภัณฑ์ไป 1 ชื่อ)
                    # 💡 หมายเหตุ: ต้องส่งเป็น list [product_name]
                    predicted_category_list = category_classifier.predict([product_name])
                    
                    # 3. รับ "คำตอบ" (จะได้คำตอบกลับมา 1 อัน)
                    if predicted_category_list:
                        category = predicted_category_list[0]
                except Exception as e:
                    print(f"   - ⚠️ เกิด Error ตอนใช้ AI (จะใช้ 'อื่นๆ'): {e}")
                    category = "อื่นๆ"
            
            # ⛔ [ลบออก] เราไม่ต้องใช้ตรรกะ Scoring (วิธีที่ 1) อีกต่อไป
            # if product_name:
            #    product_name_lower = product_name.lower()
            #    ... (ลบส่วนนี้ทิ้ง) ...
            # --- 🎯 [สิ้นสุดการแก้ไข] ---


            # ... (โค้ดส่วนที่เหลือ (col_r, col_l, H4, Regex, product_data) ทั้งหมดเหมือนเดิม) ...
            
            col_r = table.find('td', class_='catalog-col-r')
            if col_r:
                img_tag = col_r.find('img');
                if not img_tag: img_tag = col_r.find('p').find('img')
                if img_tag and img_tag.get('src'):
                    img_src = img_tag['src']; image_url = img_src if img_src.startswith('http') else BASE_URL + img_src.lstrip('/')
                qr_div = col_r.find('div', class_='catalog-qrcode');
                if qr_div:
                    qr_link = qr_div.find('a');
                    if qr_link and qr_link.get('href'): detail_page_url = qr_link['href']
            
            col_l = table.find('td', class_='catalog-col-l')
            if col_l:
                if not product_name: 
                    all_text_nodes = col_l.find_all(string=True, recursive=False);
                    if all_text_nodes: product_name = all_text_nodes[0].strip()

                carbon_h4 = col_l.find('h4') 
                if carbon_h4:
                    carbon_span = carbon_h4.find('span')
                    if carbon_span:
                        carbon_unit_tag = carbon_span.find('i')
                        if carbon_unit_tag:
                            carbon_unit = carbon_unit_tag.text.strip()
                            value_text_nodes = [node for node in carbon_span.contents if isinstance(node, str)]
                            if value_text_nodes:
                                carbon_value_str = value_text_nodes[0].strip().replace(',', '')
                                if carbon_value_str and carbon_value_str != '-':
                                    try: carbon_value = float(carbon_value_str)
                                    except ValueError: pass
                
                full_text_col_l = col_l.get_text(separator='\n', strip=True)
                
                unit_match = re.search(r"หน่วยการทำงาน:\s*(.+)", full_text_col_l);
                if unit_match: functional_unit = unit_match.group(1).strip()
                scope_match = re.search(r"ขอบเขต:\s*(.+)", full_text_col_l);
                if scope_match: scope = scope_match.group(1).strip()
                strong_tag = col_l.find('strong');
                if strong_tag: company_name = strong_tag.text.strip()
                contact_match = re.search(r"ติดต่อ\s*(.+)", full_text_col_l, re.MULTILINE);
                if contact_match: contact_person = contact_match.group(1).strip()
                phone_match = re.search(r"โทรศัพท์\s*([^#\n]+)(?:#(\d+))?", full_text_col_l, re.MULTILINE);
                if phone_match:
                    phone = phone_match.group(1).strip();
                    if phone_match.group(2): phone += f" #{phone_match.group(2).strip()}"
                email_match = re.search(r"อีเมล์\s*(.+)", full_text_col_l, re.MULTILINE);
                if email_match: email = email_match.group(1).strip()
                
                if carbon_value is None: 
                    carbon_match = re.search(r"(คาร์บอนฟุตพริ้นท์|Carbon Footprint|ลดการปล่อย)[^:]*:\s*([\d,.-]+)\s*(.*)", full_text_col_l);
                    if carbon_match:
                        carbon_value_str = carbon_match.group(2).replace(',', ''); 
                        if not carbon_unit:
                            carbon_unit = carbon_match.group(3).strip()
                        if carbon_value_str and carbon_value_str != '-':
                            try: carbon_value = float(carbon_value_str)
                            except ValueError: pass
                
                date_match = re.search(r"(วันรับรอง|Date of Approval)[^:]*:\s*(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})", full_text_col_l);
                if date_match:
                    cert_start_date_iso = convert_be_to_iso(date_match.group(2)); cert_end_date_iso = convert_be_to_iso(date_match.group(3))
            
            product_data = {
                "product_id": product_id, "label_type": label_logo_type,
                "product_name": product_name, "category": category, # 🎯 นี่คือ Category ที่มาจาก AI
                "functional_unit": functional_unit, "scope": scope,
                "company_name": company_name, "contact_person": contact_person,
                "phone": phone, "email": email, "image_url": image_url,
                "detail_page_url": detail_page_url,
                "carbon_value": carbon_value, "carbon_unit": carbon_unit,
                "cert_start_date": cert_start_date_iso, "cert_end_date": cert_end_date_iso,
            }
            all_products.append(product_data)
            processed_count += 1
        except Exception as e:
            # print(f"   - ข้ามแถวที่ {i+1} เพราะ Error: {e} (ID: {product_id})")
            continue
    return all_products

# --- 7. ฟังก์ชันส่งข้อมูลเข้า Supabase ---
def upload_to_supabase(products_list):
    if not products_list:
        print("   -> ไม่มีข้อมูลให้ส่ง")
        return True
    print(f"   -> กำลังส่ง {len(products_list)} รายการเข้า Supabase...")
    try:
        data, count = supabase.table('materials').upsert(
            products_list,
            on_conflict='product_id',
        ).execute()
        print("   -> ✅ ส่งข้อมูลสำเร็จ!")
        return True
    except Exception as e:
        print(f"   -> ❌ เกิดข้อผิดพลาดตอนส่งข้อมูลเข้า Supabase: {e}")
        return False

# --- 8. ส่วนโปรแกรมหลัก (แบบวนลูป ปี -> ไตรมาส -> ประเภท + [แก้ไข] จัดระเบียบ print) ---
if __name__ == "__main__":
    start_year_be = 2010 # 🎯 ปี พ.ศ. เริ่มต้น (CFP เริ่ม 2010)
    end_year_be = 2025  # ปี พ.ศ. สิ้นสุด

    total_products_scraped_all_periods = 0
    processed_tasks = 0

    print(f"=== เริ่มกระบวนการดึงข้อมูล CFP (2010+) และ CFR (2014+) ... ===")

    # 🎯 กำหนดประเภทที่จะดึงข้อมูล
    scrape_types = [
        {"label": "CFP", "section": "_SBPRODUCTS"},
        {"label": "CFR", "section": "_SBREDUCTION"}
    ]

    for year_be in range(start_year_be, end_year_be + 1): # ลูปนี้เริ่มที่ 2010
        print(f"\n--- กำลังประมวลผลปี พ.ศ. {year_be} ---")
        
        # 🎯 วนลูปไตรมาส 1 ถึง 4
        for quarter in range(1, 5):
            print(f"  --- ไตรมาส {quarter} ---")

            # 🎯 วนลูปตามประเภท (CFP ก่อน แล้ว CFR)
            for scrape_type in scrape_types:
                
                label = scrape_type["label"]
                section = scrape_type["section"]

                # --- 🎯 [แก้ไข] ตรรกะการข้าม + จัดระเบียบ print ---
                
                # ข้ามเฉพาะ CFR ถ้ายีงไม่ถึงปี 2014
                if label == "CFR" and year_be < 2014:
                    # [ใหม่] แสดงผลแบบกระชับเมื่อข้าม
                    print(f"    [ประเภท: {label}] ⚠️ ข้ามปี {year_be} (CFR เริ่ม 2014)") 
                    processed_tasks += 1 
                    continue # ข้ามไปงานถัดไป
                
                # [ใหม่] ย้าย print นี้มาไว้ตรงนี้ (จะทำงานเฉพาะเมื่อ "ไม่ข้าม")
                print(f"\n    --- [ประเภท: {label}] ---") 
                # --- 🎯 [สิ้นสุดการแก้ไข] ---

                # สร้าง URL ที่ถูกต้อง (CFP หรือ CFR)
                period_url = f'https://thaicarbonlabel.tgo.or.th/index.php?lang=TH&mod=WTJGMFlXeHZadz09&action=Y0c5emRBPT0&section={section}&industry=3&style=_ROW&sorting=_ASC&year={year_be}&quarter={quarter}'
                                    
                html = fetch_tgo_data_with_selenium(period_url) 

                if html:
                    # ใช้ Parser แบบการ์ด (ตัวเดิม)
                    products_this_period = parse_product_data(html, year_be, quarter)

                    if products_this_period:
                        initial_count = len(products_this_period)

                        # --- 🎯 [เพิ่มเพื่อ DEBUG] ---
                        # ลองพิมพ์ค่า carbon_value ของทุกรายการที่ดึงได้ในรอบนี้
                        print(f"   [DEBUG] ตรวจสอบ {initial_count} รายการที่ดึงได้ (ก่อนกรอง ID ซ้ำ):")
                        for p in products_this_period:
                            print(f"     - ID: {p.get('product_id')}, Carbon: {p.get('carbon_value')}, Unit: {p.get('carbon_unit')}")
                        print("   [DEBUG] สิ้นสุดการตรวจสอบ")
                        # --- 🎯 [สิ้นสุด DEBUG] ---

                        # กรอง ID ซ้ำ (ตรรกะเดิม)
                        unique_products_dict = {}
                        

                        for product in products_this_period:
                            pid = product.get('product_id', f"TEMP_Y{year_be}Q{quarter}_{len(unique_products_dict)}")
                            if pid != "ID_NOT_FOUND" and pid not in unique_products_dict:
                                unique_products_dict[pid] = product
                        unique_products_this_period = list(unique_products_dict.values())
                        filtered_count = len(unique_products_this_period)

                        if filtered_count < initial_count:
                            print(f"   ⚠️ [{label}] กรอง ID ซ้ำแล้ว เหลือ {filtered_count} รายการในไตรมาสนี้")

                        print(f"   ✅ [{label}] แยกข้อมูลปี {year_be}/Q{quarter} สำเร็จ! ได้ {filtered_count} รายการ (หลังกรอง ID ซ้ำ)")
                        total_products_scraped_all_periods += filtered_count

                        if not upload_to_supabase(unique_products_this_period):
                            print(f"   ❌ [{label}] ไม่สามารถส่งข้อมูลของปี {year_be}/Q{quarter} เข้า Supabase ได้, ข้าม...")
                    else:
                        print(f"   ⚠️ [{label}] ไม่พบข้อมูลผลิตภัณฑ์ในปี {year_be}/Q{quarter} (Parser ไม่เจอข้อมูล)")
                else:
                    print(f"   ⚠️ [{label}] ไม่มีข้อมูล หรือ ไม่สามารถดึง HTML ของปี {year_be}/Q{quarter} ได้, ข้าม...")

                processed_tasks += 1
                if html: 
                    print(f"   --- สิ้นสุด {label} ปี {year_be}/Q{quarter}, หยุดพัก 3 วินาที ---") 
                    time.sleep(3) 

            # 🎯 [ลบออก] บรรทัด "สิ้นสุดไตรมาส" ถูกลบออกจากตรงนี้

        print(f"--- สิ้นสุดปี {year_be} ---")

    print(f"\n=== สิ้นสุดกระบวนการ ===")
    print(f"ประมวลผลทั้งหมด {processed_tasks} งาน (ปี x ไตรมาส x ประเภท, รวมที่ข้าม)")
    print(f"ดึงข้อมูลผลิตภัณฑ์ (CFP+CFR) (ที่ไม่ซ้ำ ID) ได้ทั้งหมด: {total_products_scraped_all_periods} รายการ")
