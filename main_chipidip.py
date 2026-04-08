"""
ОПИСАНИЕ СКРИПТА:
Этот код предназначен для автоматической выгрузки товаров брендов Prompower и Unimat для магазина "Чип и Дип".
Где он используется: В репозитории feed-from-prompower-for-chipidip на GitHub Actions.
Что он делает:
1. Забирает данные по API поставщика.
2. Рассчитывает costPrice и rPrice с учетом НДС 22% (1.22) и скидок (MRPPercent).
3. Раз в месяц (1-го числа) обходит сайт и кэширует ссылки на PDF-файлы. В остальные дни использует кэш.
4. Генерирует XML-feed, строго соблюдая ограничения на длину строк (name) и требуемые теги.
"""

import requests
import json
import os
import re
import datetime
import time
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

# --- НАСТРОЙКИ СЕКРЕТОВ И ОТЛАДКИ ---
EMAIL = os.getenv("API_EMAIL")
KEY = os.getenv("API_KEY")

DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
try:
    DEBUG_LIMIT = int(os.getenv("DEBUG_LIMIT", "3"))
except:
    DEBUG_LIMIT = 3

if not EMAIL or not KEY:
    print("КРИТИЧЕСКАЯ ОШИБКА: Не заданы секреты API_EMAIL или API_KEY в GitHub Secrets!")
    exit(1)

API_URL = "https://prompower.ru/api/prod/"
SITE_URL = "https://www.prompower.ru"
XML_FILENAME = "feed-from-prompower-for-chipidip.xml"
CACHE_FILENAME = "chipidip_pdf_cache.json"

GROUP_MAP = {
    "19“ комплектующие": "2953",
    "Аксессуары": "2953",
    "Двери": "3059",
    "Коллаборативные роботы": "2968",
    "Модификационные комплекты для моторов": "2958",
    "Модули расширения ПЛК PMP301": "3061",
    "Монтажные панели": "3054",
    "Моторные дроссели": "2958",
    "Моторы": "2958",
    "Серводвигатели": "2954",
    "Сетевые дроссели": "2954",
    "Соединительные комплекты": "3073",
    "Шасси": "3073",
    "Шкафы электротехнические": "3073",
    "Опции для двигателей": "2958",
    "Аксессуары для ПЛК": "3061",
    "Аксессуары для реле": "2947",
    "Дополнительные контактные приставки": "2945",
    "Заземление": "3085",
    "Опции для преобразователей частоты": "2954",
    "Дополнительные контактные приставки PULSE": "2945",
    "Колодки для реле": "2926",
    "Прокладка кабеля": "2804",
    "MCB (Miniature Circuit Breaker)": "3109",
    "MCB": "3109",
    "Реле общего назначения": "2947",
    "Контакторы PULSE": "2947",
    "Панели основания": "3067",
    "Аксессуары для сервосистем": "2954",
    "Контакторы": "2947",
    "Миниатюрные силовые реле": "2947",
    "Сувенирная продукция": "2954",
    "Реле тонкие": "2947",
    "Кабели для датчиков": "2925",
    "Миниконтакторы": "2947",
    "Миниконтакторы PULSE": "2947",
    "Цоколи": "3067",
    "Климат + Свет": "3184",
    "Блок питания HDR в пластиковом корпусе": "2939",
    "Пластроны": "3124",
    "Блок питания MDR в пластиковом корпусе": "2939",
    "Боковые панели": "3069",
    "Секционирование": "3062",
    "Индуктивные датчики": "1403",
    "Дополнительные контактные приставки для MCB": "3115",
    "Опции для устройств плавного пуска": "2968",
    "Модули расширения ПЛК PMP20/PMP30": "3061",
    "Блок питания NDR в металлическом корпусе": "2939",
    "Автоматы защиты двигателя PULSE": "2930",
    "Фотоэлектрические датчики": "2744",
    "Полки": "3071",
    "Потолочные панели": "3067",
    "Преобразователи частоты PD100": "2954",
    "Преобразователи частоты PD101": "2954",
    "Тормозные резисторы": "2954",
    "Преобразователи частоты PD150": "2954",
    "Панели оператора PH1": "3061",
    "Задние панели": "3069",
    "Промышленные коммутаторы": "3413",
    "Панели оператора PH": "3061",
    "ЭМС фильтры": "2954",
    "Сейсмостойкость": "3062",
    "Дроссели dU/dt": "2954",
    "Устройства плавного пуска P2S 050": "2968",
    "Дроссели для цепей постоянного тока": "2954",
    "Преобразователи частоты PD210": "2954",
    "Преобразователи частоты PD110": "2954",
    "Устройства плавного пуска P2S 100": "2968",
    "Сервоприводы": "2954",
    "Регуляторы мощности": "2968",
    "Программируемые логические контроллеры PMP20": "3061",
    "Преобразователи частоты PD310": "2954",
    "Электродвигатели класс энергоэфф. IE1": "2958",
    "Электродвигатели класс энергоэффективности IE1": "2958",
    "ПЛК PMP301": "3061",
    "Программируемые логические контроллеры PMP301": "3061",
    "Каркасы": "3072",
    "Синус-фильтры": "2954",
    "Внешние тормозные модули для ПЧ": "2954",
    "Преобразователи частоты PD310 IP54": "2954",
    "Промышленный монитор": "3061",
    "Устройства плавного пуска P2S 300": "2968",
    "Промышленный ПК": "3061",
    "ПЛК PMP30": "3061",
    "Программируемые логические контроллеры PMP30": "3061",
    "Панельный ПК": "3061",
    "Кабели и аксессуары": "2804",
    "Модули для ПЛК": "3061",
    "Панели оператора UniMAT": "3061",
    "ПЛК UniMAT": "3061",
    "Программируемые логические контроллеры UniMAT": "3061",
    "Серво": "2954"
}

NORMALIZED_GROUP_MAP = {k.strip().lower(): v for k, v in GROUP_MAP.items()}

UNIMAT_PICTURES =[
    "https://unimat-russia.ru/uploads/product-1654003344077-0.4960815358606392.png",
    "https://unimat-russia.ru/uploads/product-1654005665354-0.5424921694625866.jpg",
    "https://unimat-russia.ru/uploads/product-1703188936539-0.01815614639060681.jpg",
    "https://unimat-russia.ru/uploads/product-1654002861798-0.35138493486299605.jpg",
    "https://unimat-russia.ru/uploads/product-33.png"
]

# ИСПРАВЛЕНИЕ: Список из 10 файлов .png с GitHub Pages
DEFAULT_PROMPOWER_PICTURES =[
    f"https://brilka.github.io/feed-from-prompower-for-chipidip/{i}.png" for i in range(1, 11)
]

def load_pdf_cache():
    if os.path.exists(CACHE_FILENAME):
        try:
            with open(CACHE_FILENAME, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {"last_update_month": -1, "urls": {}}
    return {"last_update_month": -1, "urls": {}}

def save_pdf_cache(cache_data):
    if DEBUG_MODE: return
    with open(CACHE_FILENAME, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

def make_api_request(endpoint):
    url = f"{API_URL}{endpoint}"
    payload = {"email": EMAIL, "key": KEY, "format": "json"}
    headers = {"Content-type": "application/json"}
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ОШИБКА API] при запросе к {url}: {e}")
        return[]

def get_categories_dict():
    categories = {}
    try:
        resp = requests.get("https://prompower.ru/api/categories", timeout=30)
        if resp.status_code == 200:
            for cat in resp.json():
                categories[int(cat['id'])] = {'title': cat.get('title', 'Без названия'), 'parentId': cat.get('parentId')}
    except:
        pass

    endpoints_to_try =["https://prompower.ru/api/unimatCategories", "https://prompower.ru/api/unimat-categories"]
    for ep in endpoints_to_try:
        try:
            resp = requests.get(ep, timeout=10)
            if resp.status_code == 200 and isinstance(resp.json(), list):
                for cat in resp.json():
                    categories[int(cat['id'])] = {'title': cat.get('title', 'Без названия'), 'parentId': cat.get('parentId')}
        except:
            continue
    return categories

def scrape_docs(url):
    docs =[]
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if '.pdf' in href.lower():
                    div_tag = a_tag.find('div', class_=lambda x: x and "text-caption" in x)
                    doc_name = div_tag.text.strip() if div_tag else "Документация"
                    full_link = SITE_URL + href if href.startswith('/') else href
                    if not any(d['url'] == full_link for d in docs):
                        docs.append({"url": full_link, "name": doc_name})
    except:
        pass
    return docs

def process_products(products, brand, categories_dict, pdf_cache, is_first_offer):
    items_xml =[]
    param_regex = re.compile(r"^(.*?)(?:\s*\((.*?)\))?$")
    
    today = datetime.datetime.now()
    is_first_of_month = (today.day == 1)
    need_global_pdf_update = True if DEBUG_MODE else (is_first_of_month and pdf_cache.get("last_update_month") != today.month)

    for prod in products:
        article = str(prod.get('article', '')).strip()
        if not article:
            continue
            
        raw_price = prod.get('price')
        if not raw_price or float(raw_price) <= 0:
            continue
            
        price_val = float(raw_price)
        mrp_percent = float(prod.get('MRPPercent', 0))
        instock = str(prod.get('instock', 0))
        description = str(prod.get('description', ''))
        title = str(prod.get('title', ''))
        
        url = ""
        if brand == "Prompower" and prod.get('path'):
            path_str = prod.get('path')
            if not path_str.startswith('/'): path_str = '/' + path_str
            url = f"{SITE_URL}/catalog{path_str}"
            
        if mrp_percent == 0:
            cost_price = (price_val * 1.22) / 0.85
            r_price = (price_val * 1.22) / 0.9
        else:
            cost_price = (price_val * 1.22) * 0.85
            r_price = price_val * 1.22
            
        cost_price = round(cost_price, 2)
        r_price = round(r_price, 2)
        
        item_group_id = ""
        cat_id = prod.get('categoryId', '')
        
        cat_raw = None
        for k, v in prod.items():
            if k.lower() == 'category':
                cat_raw = v
                break
                
        direct_category_name = ""
        if isinstance(cat_raw, str):
            direct_category_name = cat_raw
        elif isinstance(cat_raw, dict):
            direct_category_name = cat_raw.get('title', '') or cat_raw.get('name', '')
        elif isinstance(cat_raw, list) and len(cat_raw) > 0:
            direct_category_name = str(cat_raw[0])
            
        direct_category_name = direct_category_name.strip()
        
        if direct_category_name:
            item_group_id = NORMALIZED_GROUP_MAP.get(direct_category_name.lower(), "")
            
        if not item_group_id and cat_id and int(cat_id) in categories_dict:
            current_id = int(cat_id)
            for _ in range(5):
                if current_id not in categories_dict: break
                cat_data = categories_dict[current_id]
                cat_name = cat_data['title'].strip()
                item_group_id = NORMALIZED_GROUP_MAP.get(cat_name.lower(), "")
                if item_group_id: break
                if cat_data.get('parentId'):
                    current_id = int(cat_data['parentId'])
                else:
                    break
                    
        offer_xml = ["<offer>"]
        
        if is_first_offer: offer_xml.append('<!--  уникальный идентификатор товара поставщика. может быть буквенно-цифровой. используется для дальнейшей трансляции заказов поставщику. У Prompower и Unimat это article в API.  -->')
        offer_xml.append(f"<id>{escape(article)}</id>")
        
        if is_first_offer: offer_xml.append('<!--  кол-во товара, доступное для продажи. В API Prompower и Unimat это instock -->')
        offer_xml.append(f"<qty>{instock}</qty>")
        
        if url:
            if is_first_offer: offer_xml.append('<!--  ссылка на карточку товара на сайте поставщика. Используется для просмотра информации о товаре складом или отделом закупок Чип и Дип. У Prompower это значение в path в API (но в path в API указан неполный путь, например, /mcb/ESM163C12 - поэтому в начале нужно дописать www.prompower.ru ). Для Unimat url недоступен.  -->')
            offer_xml.append(f"<url>{escape(url)}</url>")
            
        if is_first_offer: 
            offer_xml.append('<!--  costPrice - цена продажи (за единицу измерения) поставщика для Чип и Дип.  Для Prompower и Unimat costPrice определяется так: Вариант1. если MRPPercent в API для данной позиции отсутствует или равен 0, то нужно price (из API Prompower) умножить на НДС (1.22) и полученное значение разделить на 0.85. Вариант2. если MRPPercent в API для данной позиции присутствует и не равен 0, то нужно price (из API Prompower) умножить на НДС (1.22) и умножить на (0.85).  -->')
        offer_xml.append(f'<price qty="1" costPrice="{cost_price}" rPrice="{r_price}"/>')
        
        if cat_id:
            if is_first_offer: offer_xml.append('<!--  принадлежность товара к категории поставщика. Код категории должен быть указан в списке categories. Не обязателльное поле  -->')
            offer_xml.append(f"<categoryId>{cat_id}</categoryId>")
            
        if is_first_offer: 
            offer_xml.append('<!--  Список ссылок на фото данного товара. Максимум 10 фото. Фото должны быть без водяных знаков. Не обязательное поле. Для Prompower фото загружаются по API - у разных товаров может быть разное количество фото: нужно предусмотреть, чтобы код правильно обработал подгрузку всех доступных фото. Если в каком-то img API Prompower нет значения, т.е. отсутствует ссылка на фото, то нужно подгрузить все фото, которые лежат здесь: https://brilka.github.io/feed-from-prompower-for-chipidip/ (в коде нужно указать все ссылки в списке DEFAULT_PROMPOWER_PICTURES ). Для Unimat у всех позиций нужно указать 5 фото со следующими адресами: https://unimat-russia.ru/uploads/product-1654003344077-0.4960815358606392.png ; https://unimat-russia.ru/uploads/product-1654005665354-0.5424921694625866.jpg ; https://unimat-russia.ru/uploads/product-1703188936539-0.01815614639060681.jpg ; https://unimat-russia.ru/uploads/product-1654002861798-0.35138493486299605.jpg ; https://unimat-russia.ru/uploads/product-33.png  -->')
        
        final_images =[]
        if brand == "Unimat":
            final_images = UNIMAT_PICTURES
        else:
            api_images = prod.get('img',[])
            if isinstance(api_images, str): api_images = [api_images]
            if not api_images and prod.get('image'): api_images = [prod.get('image')]
            
            for img in api_images:
                if img and str(img).strip():
                    img_url = img if img.startswith('http') else SITE_URL + img
                    final_images.append(img_url)
            
            if len(final_images) == 0:
                final_images = DEFAULT_PROMPOWER_PICTURES
                
        for pic in final_images[:10]:
            offer_xml.append(f"<picture>{escape(pic)}</picture>")
            
        if is_first_offer: offer_xml.append('<!--  Наименование товара. Макс. 250 символов. Обязателльное поле. Для Prompower и Unimat это description в API  -->')
        safe_name = (description if description else (title if title else "Товар без названия"))[:250]
        offer_xml.append(f"<name>{escape(safe_name)}</name>")
        
        if is_first_offer: offer_xml.append('<!--  Артикул (оригинальный парт номер) по каталогу производителя данного товара. Не оябязательное поле. Для Prompower и Unimat это title в API  -->')
        if title: offer_xml.append(f"<partNumber>{escape(title)}</partNumber>")
            
        if is_first_offer: offer_xml.append('<!--  Название производеителя (бренда) товара. Может быть полное или сокращенное название. Не оябязательное поле. Для Prompower нужно указывать Prompower. Для Unimat нужно указывать Unimat  -->')
        offer_xml.append(f"<vendor>{brand}</vendor>")
        
        if is_first_offer: offer_xml.append('<!--  Описание товара. Может быть в формате html. Не оябязательное поле. Для Prompower и Unimat это description в API  -->')
        if description: offer_xml.append(f"<description><![CDATA[{description}]]></description>")
            
        if is_first_offer: offer_xml.append('<!--  список параметров товара. Максимум 20 параметров для одного товара. -->')
        for prop in prod.get('props', [])[:20]:
            p_name, p_val = prop.get('name', ''), prop.get('value', '')
            match = param_regex.match(p_name)
            clean_name = match.group(1).strip() if match else p_name
            unit = match.group(2) if match and match.group(2) else ""
            offer_xml.append(f'<param name="{escape(clean_name)}" unit="{escape(unit)}">{escape(str(p_val))}</param>')
            
        if is_first_offer: offer_xml.append('<!--  Дополнительные файлы для скачивания. Парсинг с кэшированием (обновляется 1 числа месяца). -->')
        if brand == "Prompower" and url:
            if need_global_pdf_update or url not in pdf_cache["urls"]:
                docs = scrape_docs(url)
                if not docs and url in pdf_cache["urls"]: docs = pdf_cache["urls"][url]
                pdf_cache["urls"][url] = docs
            else:
                docs = pdf_cache["urls"][url]
            for doc in docs: offer_xml.append(f'<docFile url="{escape(doc["url"])}" name="{escape(doc["name"])}"/>')
                
        if is_first_offer: 
            long_comment = """<!--  Код группы товара из каталога Чип и Дип. Если указан - товар будет размещен в данный раздел товара сайта Чип и Дип. Не обязательное поле. Для Prompower и Unimat вот сопоставление кодов и категорий: 
2953;19“ комплектующие;
2953;Аксессуары;
3059;Двери;
2968;Коллаборативные роботы;
2958;Модификационные комплекты для моторов;
3061;Модули расширения ПЛК PMP301;
3054;Монтажные панели;
2958;Моторные дроссели;
2958;Моторы;
2954;Серводвигатели;
2954;Сетевые дроссели;
3073;Соединительные комплекты;
3073;Шасси;
3073;Шкафы электротехнические;
2958;Опции для двигателей;
3061;Аксессуары для ПЛК;
2947;Аксессуары для реле;
2945;Дополнительные контактные приставки;
3085;Заземление;
2954;Опции для преобразователей частоты;
2945;Дополнительные контактные приставки PULSE;
2926;Колодки для реле;
2804;Прокладка кабеля;
3109;MCB (Miniature Circuit Breaker);
2947;Реле общего назначения;
2947;Контакторы PULSE;
3067;Панели основания;
2954;Аксессуары для сервосистем;
2947;Контакторы;
2947;Миниатюрные силовые реле;
2954;Сувенирная продукция;
2947;Реле тонкие;
2925;Кабели для датчиков;
2947;Миниконтакторы;
2947;Миниконтакторы PULSE;
3067;Цоколи;
3184;Климат + Свет;
2939;Блок питания HDR в пластиковом корпусе;
3124;Пластроны;
2939;Блок питания MDR в пластиковом корпусе;
3069;Боковые панели;
3062;Секционирование;
1403;Индуктивные датчики;
3115;Дополнительные контактные приставки для MCB;
2968;Опции для устройств плавного пуска;
3061;Модули расширения ПЛК PMP20/PMP30;
2939;Блок питания NDR в металлическом корпусе;
2930;Автоматы защиты двигателя PULSE;
2744;Фотоэлектрические датчики;
3071;Полки;
3067;Потолочные панели;
2954;Преобразователи частоты PD100;
2954;Преобразователи частоты PD101;
2954;Тормозные резисторы;
2954;Преобразователи частоты PD150;
3061;Панели оператора PH1;
3069;Задние панели;
3413;Промышленные коммутаторы;
3061;Панели оператора PH;
2954;ЭМС фильтры;
3062;Сейсмостойкость;
2954;Дроссели dU/dt;
2968;Устройства плавного пуска P2S 050;
2954;Дроссели для цепей постоянного тока;
2954;Преобразователи частоты PD210;
2954;Преобразователи частоты PD110;
2968;Устройства плавного пуска P2S 100;
2954;Сервоприводы;
2968;Регуляторы мощности;
3061;Программируемые логические контроллеры PMP20;
2954;Преобразователи частоты PD310;
2958;Электродвигатели класс энергоэфф. IE1;
3061;ПЛК PMP301;
3072;Каркасы;
2954;Синус-фильтры;
2954;Внешние тормозные модули для ПЧ;
2954;Преобразователи частоты PD310 IP54;
3061;Промышленный монитор;
2968;Устройства плавного пуска P2S 300;
3061;Промышленный ПК;
3061;ПЛК PMP30;
3061;Панельный ПК;
2804;Кабели и аксессуары;
3061;Модули для ПЛК;
3061;Панели оператора UniMAT;
3061;ПЛК UniMAT;
2954;Серво;
   -->"""
            offer_xml.append(long_comment)
            
        if item_group_id:
            offer_xml.append(f"<itemGroupId>{item_group_id}</itemGroupId>")
            
        if is_first_offer: offer_xml.append('<!--  Единица измерения товара. Допустимые значения (соглано ОКЕИ) -->')
        offer_xml.append("<unitID>шт</unitID>")
        
        if is_first_offer: offer_xml.append('<!--  Вес товара в граммах. Используется для вычисления тарифов по доставке товара. -->')
        weight = prod.get('weight')
        if brand == "Prompower" and weight:
            try:
                weight_grams = int(float(weight) * 1000)
                offer_xml.append(f"<weight>{weight_grams}</weight>")
            except (ValueError, TypeError):
                offer_xml.append(f"<weight>{escape(str(weight))}</weight>")
                
        item_width = None
        item_height = None
        item_depth = None
        
        if brand == "Prompower":
            for prop in prod.get('props',[]):
                p_name = prop.get('name', '').strip().lower()
                p_val = prop.get('value')
                
                if p_val in [0, 0.0, "0", "", None]:
                    continue
                    
                if p_name in ['ширина (мм)', 'ширина']:
                    if item_width is None: item_width = p_val
                elif p_name in ['высота (мм)', 'высота']:
                    if item_height is None: item_height = p_val
                elif p_name in['глубина (мм)', 'глубина']:
                    if item_depth is None: item_depth = p_val
                    
        if is_first_offer: offer_xml.append('<!--  Ширина товара, в миллиметрах. В API Prompower находится в props среди остальных записей. У Unimat отсутствуют данные. -->')
        if item_width is not None:
            offer_xml.append(f"<width>{escape(str(item_width))}</width>")
            
        if is_first_offer: offer_xml.append('<!--  Высота товара, в миллиметрах. В API Prompower находится в props среди остальных записей. У Unimat отсутствуют данные. -->')
        if item_height is not None:
            offer_xml.append(f"<height>{escape(str(item_height))}</height>")
            
        if is_first_offer: offer_xml.append('<!--  Глубина товара, в миллиметрах. В API Prompower находится в props среди остальных записей. У Unimat отсутствуют данные. -->')
        if item_depth is not None:
            offer_xml.append(f"<depth>{escape(str(item_depth))}</depth>")
            
        offer_xml.append("</offer>")
        items_xml.append("\n".join(offer_xml))
        is_first_offer = False
        
    return items_xml, is_first_offer

def main():
    start_time = time.time()
    print("=========================================")
    if DEBUG_MODE:
        print(f"!!! РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН !!!")
        print(f"ВНИМАНИЕ: Скрипт обработает {DEBUG_LIMIT} товаров для Prompower и {DEBUG_LIMIT} товаров для Unimat.")
    print("=========================================")
    
    categories_dict = get_categories_dict()
    prompower_products = make_api_request("getProducts")
    unimat_products = make_api_request("getUnimatProducts")
    
    if DEBUG_MODE:
        prompower_products = prompower_products[:DEBUG_LIMIT]
        unimat_products = unimat_products[:DEBUG_LIMIT]
    
    pdf_cache = load_pdf_cache()
    all_offers_xml =[]
    is_first_offer = True
    
    if prompower_products:
        xml_data, is_first_offer = process_products(prompower_products, "Prompower", categories_dict, pdf_cache, is_first_offer)
        all_offers_xml.extend(xml_data)
    if unimat_products:
        xml_data, is_first_offer = process_products(unimat_products, "Unimat", categories_dict, pdf_cache, is_first_offer)
        all_offers_xml.extend(xml_data)
        
    save_pdf_cache(pdf_cache)
        
    xml_lines =[
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<shop>',
        '<name>Prompower и Unimat</name>',
        '<company>Мотрум</company>',
        '<url>https://brilka.github.io/feed-from-prompower-for-chipidip/</url>',
        '<categories>'
    ]
    
    for cat_id, data in categories_dict.items():
        parent_attr = f' parentId="{data["parentId"]}"' if data['parentId'] else ''
        xml_lines.append(f'<category id="{cat_id}"{parent_attr}>{escape(data["title"])}</category>')
        
    xml_lines.append('</categories>')
    xml_lines.append('<!--  список товаров к продаже  -->')
    xml_lines.append('<offers>')
    xml_lines.extend(all_offers_xml)
    xml_lines.append('</offers>')
    xml_lines.append('</shop>')
    
    try:
        with open(XML_FILENAME, "w", encoding="utf-8") as f:
            f.write("\n".join(xml_lines))
        print(f"\nФайл {XML_FILENAME} успешно сгенерирован!")
    except Exception as e:
        print(f"Ошибка сохранения файла: {e}")

    print(f"Парсинг и формирование завершены за {time.time() - start_time:.2f} секунд.")

if __name__ == "__main__":
    main()
