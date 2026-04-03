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
    "Заземление": "3085", "Опции для преобразователей частоты": "2954", 
    "Дополнительные контактные приставки PULSE": "2945", "Колодки для реле": "2926", 
    "Прокладка кабеля": "2804", "MCB": "3109", "Реле общего назначения": "2947", 
    "Контакторы PULSE": "2947", "Панели основания": "3067", "Аксессуары для сервосистем": "2954", 
    "Контакторы": "2947", "Миниатюрные силовые реле": "2947", "Сувенирная продукция": "2954", 
    "Реле тонкие": "2947", "Кабели для датчиков": "2925", "Миниконтакторы": "2947", 
    "Миниконтакторы PULSE": "2947", "Цоколи": "3067", "Климат + Свет": "3184", 
    "Блок питания HDR в пластиковом корпусе": "2939", "Пластроны": "3124", 
    "Блок питания MDR в пластиковом корпусе": "2939", "Боковые панели": "3069", 
    "Секционирование": "3062", "Индуктивные датчики": "1403", 
    "Дополнительные контактные приставки для MCB": "3115", "Опции для устройств плавного пуска": "2968", 
    "Модули расширения ПЛК PMP20/PMP30": "3061", "Блок питания NDR в металлическом корпусе": "2939", 
    "Автоматы защиты двигателя PULSE": "2930", "Фотоэлектрические датчики": "2744", 
    "Полки": "3071", "Потолочные панели": "3067", "Преобразователи частоты PD100": "2954", 
    "Преобразователи частоты PD101": "2954", "Тормозные резисторы": "2954", 
    "Преобразователи частоты PD150": "2954", "Панели оператора PH1": "3061", 
    "Задние панели": "3069", "Промышленные коммутаторы": "3413", "Панели оператора PH": "3061", 
    "ЭМС фильтры": "2954", "Сейсмостойкость": "3062", "Дроссели dU/dt": "2954", 
    "Устройства плавного пуска P2S 050": "2968", "Дроссели для цепей постоянного тока": "2954", 
    "Преобразователи частоты PD210": "2954", "Преобразователи частоты PD110": "2954", 
    "Устройства плавного пуска P2S 100": "2968", "Сервоприводы": "2954", 
    "Регуляторы мощности": "2968", "Программируемые логические контроллеры PMP20": "3061", 
    "Преобразователи частоты PD310": "2954", "Электродвигатели класс энергоэффективности IE1": "2958", 
    "Программируемые логические контроллеры PMP301": "3061", "Каркасы": "3072", 
    "Синус-фильтры": "2954", "Внешние тормозные модули для ПЧ": "2954", 
    "Преобразователи частоты PD310 IP54": "2954", "Промышленный монитор": "3061", 
    "Устройства плавного пуска P2S 300": "2968", "Промышленный ПК": "3061", 
    "Программируемые логические контроллеры PMP30": "3061", "Панельный ПК": "3061", 
    "Кабели и аксессуары": "2804", "Модули для ПЛК": "3061", "Панели оператора UniMAT": "3061", 
    "Программируемые логические контроллеры UniMAT": "3061", "Серво": "2954"
}

NORMALIZED_GROUP_MAP = {k.strip().lower(): v for k, v in GROUP_MAP.items()}

UNIMAT_PICTURES =[
    "https://unimat-russia.ru/uploads/product-1654003344077-0.4960815358606392.png",
    "https://unimat-russia.ru/uploads/product-1654005665354-0.5424921694625866.jpg",
    "https://unimat-russia.ru/uploads/product-1703188936539-0.01815614639060681.jpg",
    "https://unimat-russia.ru/uploads/product-1654002861798-0.35138493486299605.jpg",
    "https://unimat-russia.ru/uploads/product-33.png"
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
    except Exception as e:
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

    # ДЛЯ ОТЛАДКИ: Если это Prompower, выведем структуру самого первого товара, чтобы своими глазами увидеть ключи
    if DEBUG_MODE and brand == "Prompower" and len(products) > 0:
        print("\n[!!! РЕНТГЕН API !!!] Структура первого товара Prompower:")
        print(json.dumps(products[0], ensure_ascii=False, indent=2)[:1000] + "\n... (обрезано)\n")

    for prod in products:
        raw_price = prod.get('price')
        if not raw_price or float(raw_price) <= 0:
            continue
            
        price_val = float(raw_price)
        mrp_percent = float(prod.get('MRPPercent', 0))
        article = str(prod.get('article', ''))
        instock = str(prod.get('instock', 0))
        description = str(prod.get('description', ''))
        title = str(prod.get('title', ''))
        
        url = ""
        if brand == "Prompower" and prod.get('path'):
            path_str = prod.get('path')
            if not path_str.startswith('/'): path_str = '/' + path_str
            url = f"{SITE_URL}/catalog{path_str}"
            
        cost_price = round(price_val * 1.22 * ((100 - mrp_percent) / 100), 2)
        r_price = round((price_val * 1.22) / 0.9 if mrp_percent == 0 else price_val * 1.22, 2)
        
        # --- СУПЕР-ПОИСК КАТЕГОРИИ ---
        item_group_id = ""
        cat_id = prod.get('categoryId', '')
        
        # Ищем любой ключ, похожий на 'category' (Category, CATEGORY и т.д.)
        cat_raw = None
        for k, v in prod.items():
            if k.lower() == 'category':
                cat_raw = v
                break
                
        direct_category_name = ""
        # Обрабатываем, если категория пришла словарем или списком
        if isinstance(cat_raw, str):
            direct_category_name = cat_raw
        elif isinstance(cat_raw, dict):
            direct_category_name = cat_raw.get('title', '') or cat_raw.get('name', '')
        elif isinstance(cat_raw, list) and len(cat_raw) > 0:
            direct_category_name = str(cat_raw[0])
            
        direct_category_name = direct_category_name.strip()
        
        # Шаг 1: Ищем по текстовому имени
        if direct_category_name:
            item_group_id = NORMALIZED_GROUP_MAP.get(direct_category_name.lower(), "")
            
        # Шаг 2: Если не нашли по тексту, ищем по дереву (categoryId)
        fallback_used = False
        if not item_group_id and cat_id and int(cat_id) in categories_dict:
            current_id = int(cat_id)
            for _ in range(5):
                if current_id not in categories_dict: break
                cat_data = categories_dict[current_id]
                cat_name = cat_data['title'].strip()
                item_group_id = NORMALIZED_GROUP_MAP.get(cat_name.lower(), "")
                if item_group_id: 
                    fallback_used = True
                    break
                if cat_data.get('parentId'):
                    current_id = int(cat_data['parentId'])
                else:
                    break
                    
        # ЛОГИРОВАНИЕ ОШИБОК ДЛЯ ОТЛАДКИ
        if DEBUG_MODE and brand == "Prompower":
            if item_group_id:
                pass # Всё ок, не спамим лог
            else:
                print(f"\n[ОШИБКА СОПОСТАВЛЕНИЯ - {brand}] Артикул: {article}")
                print(f"  > Значение из API 'category': '{direct_category_name}'")
                print(f"  > Значение из API 'categoryId': '{cat_id}'")
                if cat_id and int(cat_id) in categories_dict:
                    print(f"  > В дереве категорий это папка: '{categories_dict[int(cat_id)]['title']}'")
                print("  => РЕЗУЛЬТАТ: В словаре GROUP_MAP совпадений не найдено!")
                
        offer_xml =["<offer>"]
        
        if is_first_offer: offer_xml.append('<!--  уникальный идентификатор товара поставщика. может быть буквенно-цифровой. используется для дальнейшей трансляции заказов поставщику. У Prompower и Unimat это article в API.  -->')
        offer_xml.append(f"<id>{escape(article)}</id>")
        
        if is_first_offer: offer_xml.append('<!--  кол-во товара, доступное для продажи. В API Prompower и Unimat это instock -->')
        offer_xml.append(f"<qty>{instock}</qty>")
        
        if url:
            if is_first_offer: offer_xml.append('<!--  ссылка на карточку товара на сайте поставщика. Используется для просмотра информации о товаре складом или отделом закупок Чип и Дип. У Prompower это значение в path в API (но в path в API указан неполный путь, например, /mcb/ESM163C12 - поэтому в начале нужно дописать www.prompower.ru ). Для Unimat url недоступен.  -->')
            offer_xml.append(f"<url>{escape(url)}</url>")
            
        if is_first_offer: offer_xml.append('<!--  список ценовых предложений. costPrice и rPrice рассчитаны согласно ТЗ с НДС 1.22 -->')
        offer_xml.append(f'<price qty="1" costPrice="{cost_price}" rPrice="{r_price}"/>')
        
        if cat_id:
            if is_first_offer: offer_xml.append('<!--  принадлежность товара к категории поставщика. Код категории должен быть указан в списке categories. Не обязателльное поле  -->')
            offer_xml.append(f"<categoryId>{cat_id}</categoryId>")
            
        if is_first_offer: offer_xml.append('<!--  Список ссылок на фото данного товара. Максимум 10 фото. -->')
        if brand == "Unimat":
            for pic in UNIMAT_PICTURES: offer_xml.append(f"<picture>{escape(pic)}</picture>")
        else:
            images = prod.get('img',[])
            if isinstance(images, str): images =[images] 
            if not images and prod.get('image'): images =[prod.get('image')]
            for img in images[:10]:
                img_url = img if img.startswith('http') else SITE_URL + img
                offer_xml.append(f"<picture>{escape(img_url)}</picture>")
                
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
        for prop in prod.get('props',[])[:20]:
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
                
        if is_first_offer: offer_xml.append("""<!--  Код группы товара из каталога Чип и Дип... (сокращено) -->""")
            
        if item_group_id:
            offer_xml.append(f"<itemGroupId>{item_group_id}</itemGroupId>")
            
        if is_first_offer: offer_xml.append('<!--  Единица измерения товара. Допустимые значения (соглано ОКЕИ) -->')
        offer_xml.append("<unitID>шт</unitID>")
        
        if is_first_offer: offer_xml.append('<!--  Вес товара в граммах. Используется для вычисления тарифов по доставке товара. -->')
        weight = prod.get('weight')
        if brand == "Prompower" and weight: offer_xml.append(f"<weight>{weight}</weight>")
            
        offer_xml.append("</offer>")
        items_xml.append("\n".join(offer_xml))
        is_first_offer = False
        
    return items_xml, is_first_offer

def main():
    start_time = time.time()
    print("=========================================")
    if DEBUG_MODE:
        print(f"!!! РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН !!!")
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
