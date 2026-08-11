"""
ОПИСАНИЕ СКРИПТА:
Этот код предназначен для автоматической выгрузки товаров брендов Prompower и Unimat для магазина "Чип и Дип".
Где он используется: В репозитории feed-from-prompower-for-chipidip на GitHub Actions.
Что он делает:
1. Забирает данные по API поставщика.
2. Рассчитывает costPrice и rPrice с учетом НДС 22% (1.22) и скидок (по сложной формуле с коэффициентом K).
3. Раз в месяц (1-го числа) обходит сайт и кэширует ссылки на PDF-файлы. В остальные дни использует кэш.
4. Генерирует XML-feed, строго соблюдая ограничения на длину строк (name) и требуемые теги, заменяя спецсимволы и кавычки.
5. Генерирует аварийный XML-feed ZEROwarehouse, где все остатки (qty) равны 0.
"""

import requests
import json
import os
import re
import datetime
import time
import urllib.parse
from bs4 import BeautifulSoup
from xml.sax.saxutils import escape

# --- НАСТРОЙКИ СЕКРЕТОВ И ОТЛАДКИ ---
# Получаем ключи доступа к API из зашифрованных секретов GitHub
EMAIL = os.getenv("API_EMAIL")
KEY = os.getenv("API_KEY")

# Проверяем, запущен ли режим отладки (задается вручную при запуске workflow)
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
try:
    DEBUG_LIMIT = int(os.getenv("DEBUG_LIMIT", "3"))
except:
    DEBUG_LIMIT = 3

# Если ключи не найдены, останавливаем скрипт, чтобы не делать пустые запросы
if not EMAIL or not KEY:
    print("КРИТИЧЕСКАЯ ОШИБКА: Не заданы секреты API_EMAIL или API_KEY в GitHub Secrets!")
    exit(1)

# Базовые адреса API и названия итоговых файлов
API_URL = "https://prompower.ru/api/prod/"
SITE_URL = "https://www.prompower.ru"
XML_FILENAME = "feed-from-prompower-for-chipidip.xml"
XML_ZERO_FILENAME = "feed-from-prompower-for-chipidip-ZEROwarehouse.xml"
CACHE_FILENAME = "chipidip_pdf_cache.json"

# --- ФУНКЦИЯ ДЛЯ 100% ВАЛИДНОГО XML ---
def xml_escape(text):
    """Экранирует <, >, &, а также заменяет двойные и одинарные кавычки на &quot; и &apos;"""
    if text is None:
        return ""
    return escape(str(text), {'"': '&quot;', "'": '&apos;'})

# --- СЛОВАРЬ СОПОСТАВЛЕНИЯ КАТЕГОРИЙ ---
# Ключ: Название категории от Prompower/Unimat. Значение: Код группы для Чип и Дип.
GROUP_MAP = {
    "19“ комплектующие": "3073",
    "Аксессуары": "3073",
    "Двери": "3059",
    "Коллаборативные роботы": "2584",
    "Модификационные комплекты для моторов": "2178",
    "Модули расширения ПЛК PMP301": "2273",
    "Монтажные панели": "3054",
    "Моторные дроссели": "2926",
    "Моторы": "2178",
    "Серводвигатели": "2585",
    "Сетевые дроссели": "2926",
    "Соединительные комплекты": "3073",
    "Шасси": "3073",
    "Шкафы электротехнические": "3073",
    "Опции для двигателей": "2178",
    "Аксессуары для ПЛК": "2273",
    "Аксессуары для реле": "2459",
    "Дополнительные контактные приставки": "2947",
    "Заземление": "3073",
    "Опции для преобразователей частоты": "2954",
    "Дополнительные контактные приставки PULSE": "2947",
    "Колодки для реле": "3010",
    "Прокладка кабеля": "3065",
    "MCB (Miniature Circuit Breaker)": "3109",
    "MCB": "3109",
    "Реле общего назначения": "1559",
    "Контакторы PULSE": "2947",
    "Панели основания": "3067",
    "Аксессуары для сервосистем": "2585",
    "Контакторы": "2947",
    "Миниатюрные силовые реле": "1559",
    "Сувенирная продукция": "2598",
    "Реле тонкие": "3624",
    "Кабели для датчиков": "1266",
    "Миниконтакторы": "2947",
    "Миниконтакторы PULSE": "2947",
    "Цоколи": "3062",
    "Климат + Свет": "3184",
    "Блок питания HDR в пластиковом корпусе": "2939",
    "Пластроны": "3124",
    "Блок питания MDR в пластиковом корпусе": "2939",
    "Боковые панели": "3069",
    "Секционирование": "3062",
    "Индуктивные датчики": "1266",
    "Дополнительные контактные приставки для MCB": "3115",
    "Опции для устройств плавного пуска": "2968",
    "Модули расширения ПЛК PMP20/PMP30": "2273",
    "Блок питания NDR в металлическом корпусе": "2939",
    "Автоматы защиты двигателя PULSE": "2930",
    "Фотоэлектрические датчики": "2744",
    "Полки": "3071",
    "Потолочные панели": "3067",
    "Преобразователи частоты PD100": "2954",
    "Преобразователи частоты PD101": "2954",
    "Тормозные резисторы": "1667",
    "Преобразователи частоты PD150": "2954",
    "Панели оператора PH1": "2622",
    "Задние панели": "3069",
    "Промышленные коммутаторы": "3256",
    "Панели оператора PH": "2273",
    "ЭМС фильтры": "2926",
    "Сейсмостойкость": "3062",
    "Дроссели dU/dt": "2926",
    "Устройства плавного пуска P2S 050": "2968",
    "Дроссели для цепей постоянного тока": "2926",
    "Преобразователи частоты PD210": "2954",
    "Преобразователи частоты PD110": "2954",
    "Устройства плавного пуска P2S 100": "2968",
    "Сервоприводы": "2585",
    "Регуляторы мощности": "1556",
    "Программируемые логические контроллеры PMP20": "2273",
    "Преобразователи частоты PD310": "2954",
    "Электродвигатели класс энергоэфф. IE1": "2178",
    "Электродвигатели класс энергоэффективности IE1": "2178",
    "ПЛК PMP301": "2273",
    "Программируемые логические контроллеры PMP301": "2273",
    "Каркасы": "3072",
    "Синус-фильтры": "2926",
    "Внешние тормозные модули для ПЧ": "2954",
    "Преобразователи частоты PD310 IP54": "2954",
    "Промышленный монитор": "2273",
    "Устройства плавного пуска P2S 300": "2968",
    "Промышленный ПК": "2273",
    "ПЛК PMP30": "2273",
    "Программируемые логические контроллеры PMP30": "2273",
    "Панельный ПК": "2273",
    "Кабели и аксессуары": "2273",
    "Модули для ПЛК": "2273",
    "Панели оператора UniMAT": "2273",
    "ПЛК UniMAT": "2273",
    "Программируемые логические контроллеры UniMAT": "2273",
    "Серво": "2585"
}

# Делаем дубликат словаря, где все названия написаны маленькими буквами.
# Это нужно для надежного поиска (чтобы "MCB" и "mcb" считались одним и тем же)
NORMALIZED_GROUP_MAP = {k.strip().lower(): v for k, v in GROUP_MAP.items()}

# --- СТАТИЧНЫЕ ФАЙЛЫ И КАРТИНКИ ---
UNIMAT_PICTURES = [
    "https://unimat-russia.ru/uploads/product-1654003344077-0.4960815358606392.png",
    "https://unimat-russia.ru/uploads/product-1654005665354-0.5424921694625866.jpg",
    "https://unimat-russia.ru/uploads/product-1703188936539-0.01815614639060681.jpg",
    "https://unimat-russia.ru/uploads/product-1654002861798-0.35138493486299605.jpg",
    "https://unimat-russia.ru/uploads/product-33.png"
]

UNIMAT_PDFS = [
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/HMI%20Catalogue%206-18.pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20120%20Series%20PLC%20(1-2).pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20120%20Series%20PLC%20(61-82(%E6%9B%B4%E6%96%B0).pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%201200%20series%20PLC.pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20200%20Series%20PLC%20(29-60(%E6%9B%B4%E6%96%B0%EF%BC%89).pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20300%20%D0%BC%D0%BE%D0%B4%D1%83%D0%BB%D0%B8%20(3-28).pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UniMAT%20%D0%9B%D0%B8%D1%81%D1%82%D0%BE%D0%B2%D0%BA%D0%B0.pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/Unimat%20-%202025%20%D0%B1%D1%80%D0%BE%D1%88%D1%8E%D1%80%D0%B0.pdf",
    "https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/%D0%9C%D0%BE%D0%B4%D1%83%D0%BB%D0%B8%20UniMAT%20UN%20300%20%3D%20Siemens%20S7-300%20(100%25%20%D0%B0%D0%BD%D0%B0%D0%BB%D0%BE%D0%B3).pdf"
]

# Генерируем список дефолтных фото для Prompower (1.png - 10.png)
DEFAULT_PROMPOWER_PICTURES = [
    f"https://brilka.github.io/feed-from-prompower-for-chipidip/{i}.png" for i in range(1, 11)
]

def load_pdf_cache():
    """Загружает кэш PDF-ссылок из файла json. Нужно для того, чтобы не парсить сайт каждый день."""
    if os.path.exists(CACHE_FILENAME):
        try:
            with open(CACHE_FILENAME, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {"last_update_month": -1, "urls": {}}
    return {"last_update_month": -1, "urls": {}}

def save_pdf_cache(cache_data):
    """Сохраняет кэш PDF-ссылок. В режиме отладки сохранение отключено, чтобы не затереть боевой кэш."""
    if DEBUG_MODE: return
    with open(CACHE_FILENAME, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)

def make_api_request(endpoint):
    """Универсальная функция для отправки POST-запросов к API Prompower."""
    url = f"{API_URL}{endpoint}"
    payload = {"email": EMAIL, "key": KEY, "format": "json"}
    headers = {"Content-type": "application/json"}
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ОШИБКА API] при запросе к {url}: {e}")
        return []

def get_categories_dict():
    """Получает иерархию категорий (и Prompower, и Unimat) и собирает их в удобный словарь."""
    categories = {}
    # Сначала пытаемся получить категории Prompower
    try:
        resp = requests.get("https://prompower.ru/api/categories", timeout=30)
        if resp.status_code == 200:
            for cat in resp.json():
                categories[int(cat['id'])] = {'title': cat.get('title', 'Без названия'), 'parentId': cat.get('parentId')}
    except:
        pass

    # Пытаемся получить категории Unimat через известные эндпоинты
    endpoints_to_try = ["https://prompower.ru/api/unimatCategories", "https://prompower.ru/api/unimat-categories"]
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
    """Парсит HTML страницу товара и ищет на ней ссылки на PDF файлы (инструкции, чертежи и т.д.)."""
    docs = []
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if '.pdf' in href.lower():
                    # Ищем div с классом text-caption (название документа)
                    div_tag = a_tag.find('div', class_=lambda x: x and "text-caption" in x)
                    doc_name = div_tag.text.strip() if div_tag else "Документация"
                    full_link = SITE_URL + href if href.startswith('/') else href
                    # Избегаем дублей ссылок
                    if not any(d['url'] == full_link for d in docs):
                        docs.append({"url": full_link, "name": doc_name})
    except:
        pass
    return docs

def process_products(products, brand, categories_dict, pdf_cache, is_first_offer):
    """Основная логика обработки массива товаров: пересчет цен, парсинг параметров, формирование XML блоков."""
    items_xml = []
    param_regex = re.compile(r"^(.*?)(?:\s*\((.*?)\))?$")
    
    today = datetime.datetime.now()
    is_first_of_month = (today.day == 1)
    
    # Кэш PDF обновляется принудительно если это 1-е число месяца или если включен режим отладки
    need_global_pdf_update = True if DEBUG_MODE else (is_first_of_month and pdf_cache.get("last_update_month") != today.month)

    for prod in products:
        article = str(prod.get('article', '')).strip()
        # Если у товара нет артикула - пропускаем (требование ТЗ)
        if not article:
            continue
            
        raw_price = prod.get('price')
        # Если цены нет или она нулевая - пропускаем (требование ТЗ)
        if not raw_price or float(raw_price) <= 0:
            continue
            
        # Извлекаем базовые параметры из API
        price_val = float(raw_price)
        mrp_percent = float(prod.get('MRPPercent', 0))
        discount_percent = float(prod.get('discountPercent', 0)) # Скидка для расчета "B"
        instock = str(prod.get('instock', 0))
        description = str(prod.get('description', ''))
        title = str(prod.get('title', ''))
        tnved = str(prod.get('tnved') or '').strip()
        
        # Формируем корректную ссылку на карточку товара
        url = ""
        if brand == "Prompower" and prod.get('path'):
            path_str = prod.get('path')
            if not path_str.startswith('/'): path_str = '/' + path_str
            url = f"{SITE_URL}/catalog{path_str}"
            
        # ========================================================
        # РАСЧЕТ ЦЕНООБРАЗОВАНИЯ (costPrice и rPrice) СОГЛАСНО ТЗ
        # ========================================================
        
        # rPrice (Рекомендованная цена) всегда считается одинаково для всех
        r_price = price_val * 1.22
        
        # costPrice (Закупочная цена для Чип и Дип)
        if mrp_percent == 0:
            # Вариант 1: MRPPercent отсутствует или 0
            cost_price = (price_val * 1.22) / 0.85
        else:
            # Вариант 2: MRPPercent присутствует
            K = 1.25
            # Рассчитываем значение B (С учетом скидки)
            B = price_val * 1.22 * ((100.0 - discount_percent) / 100.0)
            
            # Цикл понижения коэффициента K, пока соотношение B/A не станет <= 0.85
            while True:
                # Защита от бесконечного цикла (если K опустится ниже или равно 0)
                if K <= 0.01:
                    A = B / 0.85 
                    break
                    
                # Рассчитываем значение A (Используя текущий коэффициент K)
                A = (price_val * 1.22) / K
                
                # Защита от деления на ноль
                if A == 0:
                    break
                    
                # Если B/A больше 0.85, уменьшаем K на 0.01 и проверяем заново
                if (B / A) > 0.85:
                    K = round(K - 0.01, 2) # Округляем до 2 знаков, чтобы избежать проблем с плавающей точкой
                else:
                    # Условие выполнено (B/A <= 0.85), прерываем цикл
                    break
                    
            cost_price = A
            
        # Округляем итоговые цены до копеек (2 знака после запятой)
        cost_price = round(cost_price, 2)
        r_price = round(r_price, 2)
        
        # ========================================================
        # ПОИСК ГРУППЫ (itemGroupId)
        # ========================================================
        item_group_id = ""
        cat_id = prod.get('categoryId', '')
        
        # Ищем текстовое поле категории в товаре (category, Category и т.д.)
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
        
        # Сценарий А: Ищем по точному текстовому названию категории
        if direct_category_name:
            item_group_id = NORMALIZED_GROUP_MAP.get(direct_category_name.lower(), "")
            
        # Сценарий Б (Fallback): Если не нашли, идем по дереву ID вверх (до 5 шагов)
        if not item_group_id and cat_id and int(cat_id) in categories_dict:
            current_id = int(cat_id)
            for _ in range(5):
                if current_id not in categories_dict: break
                cat_data = categories_dict[current_id]
                cat_name = cat_data['title'].strip()
                item_group_id = NORMALIZED_GROUP_MAP.get(cat_name.lower(), "")
                if item_group_id: break
                # Поднимаемся к родителю
                if cat_data.get('parentId'):
                    current_id = int(cat_data['parentId'])
                else:
                    break
                    
        # ========================================================
        # ФОРМИРОВАНИЕ XML ТЕГОВ ТОВАРА
        # ========================================================
        offer_xml = ["<offer>"]
        
        if is_first_offer: offer_xml.append('<!--  уникальный идентификатор товара поставщика. может быть буквенно-цифровой. используется для дальнейшей трансляции заказов поставщику. У Prompower и Unimat это article в API.  -->')
        offer_xml.append(f"<id>{xml_escape(article)}</id>")
        
        if is_first_offer: offer_xml.append('<!--  кол-во товара, доступное для продажи. В API Prompower и Unimat это instock -->')
        offer_xml.append(f"<qty>{instock}</qty>")
        
        if url:
            if is_first_offer: offer_xml.append('<!--  ссылка на карточку товара на сайте поставщика. Используется для просмотра информации о товаре складом или отделом закупок Чип и Дип. У Prompower это значение в path в API (но в path в API указан неполный путь, например, /mcb/ESM163C12 - поэтому в начале нужно дописать www.prompower.ru ). Для Unimat url недоступен.  -->')
            offer_xml.append(f"<url>{xml_escape(url)}</url>")
            
        # Комментарий к costPrice (дословно из ТЗ)
        if is_first_offer: 
            price_comment = """<!--  costPrice - цена продажи (за единицу измерения) поставщика для Чип и Дип. Для Prompower и Unimat costPrice определяется так: 
Вариант1. если MRPPercent в API для данной позиции отсутствует или равен 0, то нужно price (из API Prompower) умножить на НДС (1.22) и полученное значение разделить на 0.85. 
Вариант2. если MRPPercent в API для данной позиции присутствует и не равен 0, то логика следующая: 
costPrice=A в том случае, если B/A меньше или равно 0.85, где 
A считается как price (из API Prompower) умножить на НДС (1.22) и разделить на коэффициент K (изначально при расчёте для каждой позиции K=1.25); 
B считается как price (из API Prompower) умножить на НДС (1.22) и умножить на (100-discountPercent)/100. discountPercent взять из API Prompower. 
Если B/A выше 0.85 - то нужно пересчитать A, уменьшая коэффициент K с шагом 0.01 (т.е. 1.25, 1.24, 1.23 и т.д.) до тех пор, пока соотношение B/A не станет меньше или равно 0.85.

rPrice - рекомендуемая цена продажи на сайте Чип и Дип. Не обязательное поле (если не указано - будет использовано заданное ценообразование для поставщика, т.е. costPrice*1.25). Для всех позиций Prompower и Unimat rPrice равен price (из API Prompower) умножить на НДС (1.22).  -->"""
            offer_xml.append(price_comment)
        offer_xml.append(f'<price qty="1" costPrice="{cost_price}" rPrice="{r_price}"/>')
        
        if cat_id:
            if is_first_offer: offer_xml.append('<!--  принадлежность товара к категории поставщика. Код категории должен быть указан в списке categories. Не обязателльное поле  -->')
            offer_xml.append(f"<categoryId>{cat_id}</categoryId>")
            
        # ФОТОГРАФИИ
        if is_first_offer: 
            offer_xml.append('<!--  Список ссылок на фото данного товара. Максимум 10 фото. Фото должны быть без водяных знаков. Не обязательное поле. Для Prompower фото загружаются по API - у разных товаров может быть разное количество фото: нужно предусмотреть, чтобы код правильно обработал подгрузку всех доступных фото. Если в каком-то img API Prompower нет значения, т.е. отсутствует ссылка на фото, то нужно подгрузить все фото, которые лежат здесь: https://brilka.github.io/feed-from-prompower-for-chipidip/ (в коде нужно указать все ссылки в списке DEFAULT_PROMPOWER_PICTURES ). Для Unimat у всех позиций нужно указать 5 фото со следующими адресами: https://unimat-russia.ru/uploads/product-1654003344077-0.4960815358606392.png ; https://unimat-russia.ru/uploads/product-1654005665354-0.5424921694625866.jpg ; https://unimat-russia.ru/uploads/product-1703188936539-0.01815614639060681.jpg ; https://unimat-russia.ru/uploads/product-1654002861798-0.35138493486299605.jpg ; https://unimat-russia.ru/uploads/product-33.png  -->')
        
        final_images = []
        if brand == "Unimat":
            final_images = UNIMAT_PICTURES
        else:
            api_images = prod.get('img', [])
            if isinstance(api_images, str): api_images = [api_images]
            if not api_images and prod.get('image'): api_images = [prod.get('image')]
            
            # Собираем ссылки на картинки, если они есть
            for img in api_images:
                if img and str(img).strip():
                    img_url = img if img.startswith('http') else SITE_URL + img
                    final_images.append(img_url)
            
            # Если после всех проверок картинок нет, берем дефолтные 10 штук
            if len(final_images) == 0:
                final_images = DEFAULT_PROMPOWER_PICTURES
                
        for pic in final_images[:10]:
            offer_xml.append(f"<picture>{xml_escape(pic)}</picture>")
            
        # НАЗВАНИЕ
        if is_first_offer: offer_xml.append('<!--  Наименование товара. Макс. 250 символов. Обязателльное поле. Для Prompower и Unimat это description в API  -->')
        safe_name = (description if description else (title if title else "Товар без названия"))[:250]
        offer_xml.append(f"<name>{xml_escape(safe_name)}</name>")
        
        # АРТИКУЛ
        if is_first_offer: offer_xml.append('<!--  Артикул (оригинальный парт номер) по каталогу производителя данного товара. Не оябязательное поле. Для Prompower и Unimat это title в API  -->')
        if title: offer_xml.append(f"<partNumber>{xml_escape(title)}</partNumber>")
            
        # ВЕНДОР
        if is_first_offer: offer_xml.append('<!--  Название производеителя (бренда) товара. Может быть полное или сокращенное название. Не оябязательное поле. Для Prompower нужно указывать Prompower. Для Unimat нужно указывать Unimat  -->')
        offer_xml.append(f"<vendor>{brand}</vendor>")

        # ТН ВЭД
        if is_first_offer: offer_xml.append('<!--  Код ТН ВЭД товара. Не обязательное поле. У Prompower в API это tnved. -->')
        if tnved:
            offer_xml.append(f"<tnvedcode>{xml_escape(tnved)}</tnvedcode>")
        
        # ОПИСАНИЕ И СПЕЦИАЛЬНЫЙ ТЕКСТ ДЛЯ UNIMAT
        if is_first_offer: 
            desc_comment = """<!--  Описание товара. Может быть в формате html. Не обязательное поле. Для Prompower и Unimat это description в API. 
Для Unimat дополнительно добавляется следующий текст:

Модули Unimat на 100% совместимы (взаимозаменяемы) с соответствующими модулями Siemens S7-200, S7-300, S7-1200. Чтобы получить артикул Unimat, нужно у артикула Siemens заменить 6ES7- на UN-, а остальную часть артикула оставить без изменений.

Данный модуль Unimat [тут нужно вставить обозначение Unimat из тега partNumber] соответствует модулю Siemens [тут нужно указать то же обозначение из тега partNumber, только нужно заменить "UN " на "6ES7"].

Модули Unimat поддерживаются на складе в России. 
Кабель Profibus и ProfiNET от Unimat соответствует кабелю Siemens.

Модули UniMAT монтируются в оригинальный ПЛК Siemens, определяются средой разработки как оригинальные модули Siemens без каких-либо дополнительных манипуляций со стороны программиста.

Сценарии использования модулей Unimat: 
+ в новых проектах (с оригинальным CPU Siemens) 
+ в проектах модернизации производства, где установлены ПЛК Siemens S7-200, S7-300 и S7-1200 и требуется их расширение (или замена модулей)
+ станции удалённого (распределённого) ввода-вывода от Unimat (Profibus; Profinet)
  -->"""
            offer_xml.append(desc_comment)
            
        final_desc = description
        if brand == "Unimat" and title:
            siemens_title = title.replace("UN ", "6ES7 ")
            unimat_addition = f"""<br><br>Модули Unimat на 100% совместимы (взаимозаменяемы) с соответствующими модулями Siemens S7-200, S7-300, S7-1200. Чтобы получить артикул Unimat, нужно у артикула Siemens заменить 6ES7- на UN-, а остальную часть артикула оставить без изменений.<br><br>Данный модуль Unimat {title} соответствует модулю Siemens {siemens_title}.<br><br>Модули Unimat поддерживаются на складе в России.<br>Кабель Profibus и ProfiNET от Unimat соответствует кабелю Siemens.<br><br>Модули UniMAT монтируются в оригинальный ПЛК Siemens, определяются средой разработки как оригинальные модули Siemens без каких-либо дополнительных манипуляций со стороны программиста.<br><br>Сценарии использования модулей Unimat:<br>+ в новых проектах (с оригинальным CPU Siemens)<br>+ в проектах модернизации производства, где установлены ПЛК Siemens S7-200, S7-300 и S7-1200 и требуется их расширение (или замена модулей)<br>+ станции удалённого (распределённого) ввода-вывода от Unimat (Profibus; Profinet)"""
            final_desc += unimat_addition
            
        # CDATA не экранируется (оно специально для того и создано)
        if final_desc: offer_xml.append(f"<description><![CDATA[{final_desc}]]></description>")
            
        # ПАРАМЕТРЫ ТОВАРА (Характеристики)
        if is_first_offer: offer_xml.append('<!--  список параметров товара. Максимум 20 параметров для одного товара. -->')
        for prop in prod.get('props', [])[:20]:
            p_name, p_val = prop.get('name', ''), prop.get('value', '')
            # Парсим название и единицу измерения (если она в скобках)
            match = param_regex.match(p_name)
            clean_name = match.group(1).strip() if match else p_name
            unit = match.group(2) if match and match.group(2) else ""
            offer_xml.append(f'<param name="{xml_escape(clean_name)}" unit="{xml_escape(unit)}">{xml_escape(str(p_val))}</param>')
            
        # ДОКУМЕНТАЦИЯ (PDF файлы)
        if is_first_offer: 
            doc_comment = """<!--  Дополнительные файлы для скачивания. Например чертеж товара, файл документации (datasheet), инструкция по использованию. Файлы не должны содержать ссылок и логотипов на сайт поставщика. Допустимый формат файла - pdf, docx.  Не обязательное поле. 
Для Prompower доступные файлы pdf определяются следующим образом: нужно спарсить их названия и адрес со страницы, которая ранее получилась в тегах url. На этих страницах у некоторых товаров есть доступные файлы pdf. В коде страницы они расположены в разделе после названия раздела "Техническая документация и материалы для скачивания" и в следующем разделе после названия раздела "Чертежи, 3D-модели" (в class="v-list-group__items"). Сначала встречается ссылка на файл, например, href="/docs/pulse-nka/LVC_PULSE_Catalog.pdf" (ссылка неполная, поэтому в начале ещё нужно добавить www.prompower.ru), затем, после тега <div class="text-caption col col-10"> встречается название файла, например, "Низковольтная коммутационная аппаратура PROMPOWER. Спецификация продукта". 
Дополнительно нужно добавить файлы pdf, которые отсутствуют на странице конкретного товара, но есть на странице сайта на уровень выше - парсить их нужно по такому же принципу, который описан выше.
Если для какой-либо позиции Prompower после парсинга нет файла с названием "Краткий референс-лист проектов по автоматизации", то нужно добавить такой файл, его адрес https://prompower.ru/docs/referenceList.pdf 
Парсинг с кэшированием (обновляется 1-го числа каждого месяца).

Для позиций Unimat нужно добавлять следующие файлы:
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/HMI%20Catalogue%206-18.pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20120%20Series%20PLC%20(1-2).pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20120%20Series%20PLC%20(61-82(%E6%9B%B4%E6%96%B0).pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%201200%20series%20PLC.pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20200%20Series%20PLC%20(29-60(%E6%9B%B4%E6%96%B0%EF%BC%89).pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UN%20300%20%D0%BC%D0%BE%D0%B4%D1%83%D0%BB%D0%B8%20(3-28).pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/UniMAT%20%D0%9B%D0%B8%D1%81%D1%82%D0%BE%D0%B2%D0%BA%D0%B0.pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/Unimat%20-%202025%20%D0%B1%D1%80%D0%BE%D1%88%D1%8E%D1%80%D0%B0.pdf
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/main/Unimat/%D0%9C%D0%BE%D0%B4%D1%83%D0%BB%D0%B8%20UniMAT%20UN%20300%20%3D%20Siemens%20S7-300%20(100%25%20%D0%B0%D0%BD%D0%B0%D0%BB%D0%BE%D0%B3).pdf
название файлов указано после последнего слэш до ".pdf"

Также для всех товаров Prompower и Unimat добавляется файл (документ со ссылкой на Яндекс Диск с сертификатами ТР ТС):
https://github.com/brilka/feed-from-prompower-for-chipidip/blob/cc58148495a4db33e65ae255e720914c295e4fb0/%D0%A1%D1%81%D1%8B%D0%BB%D0%BA%D0%B0%20%D0%BD%D0%B0%20%D1%81%D0%B5%D1%80%D1%82%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%82%D1%8B%20%D0%A2%D0%A0%20%D0%A2%D0%A1.pdf
  -->"""
            offer_xml.append(doc_comment)

        # Обработка файлов Prompower (Парсинг сайта)
        if brand == "Prompower" and url:
            all_product_docs = []
            
            # 1. Сканируем страницу самого товара
            if need_global_pdf_update or url not in pdf_cache["urls"]:
                product_docs = scrape_docs(url)
                pdf_cache["urls"][url] = product_docs
            else:
                product_docs = pdf_cache["urls"][url]
            all_product_docs.extend(product_docs)
            
            # 2. Сканируем родительскую страницу
            parent_url = "/".join(url.split("/")[:-1])
            if need_global_pdf_update or parent_url not in pdf_cache["urls"]:
                parent_docs = scrape_docs(parent_url)
                pdf_cache["urls"][parent_url] = parent_docs
            else:
                parent_docs = pdf_cache["urls"][parent_url]
                
            # Добавляем те документы родителя, которых нет на странице продукта
            existing_urls = [d['url'] for d in all_product_docs]
            for p_doc in parent_docs:
                if p_doc['url'] not in existing_urls:
                    all_product_docs.append(p_doc)
            
            # 3. Добавляем референс-лист, если его еще нет в списке
            has_ref = False
            for d in all_product_docs:
                if d['name'] == "Краткий референс-лист проектов по автоматизации":
                    has_ref = True
                    break
            if not has_ref:
                all_product_docs.append({
                    "url": "https://prompower.ru/docs/referenceList.pdf",
                    "name": "Краткий референс-лист проектов по автоматизации"
                })
                
            # Генерируем теги docFile
            for doc in all_product_docs: 
                offer_xml.append(f'<docFile url="{xml_escape(doc["url"])}" name="{xml_escape(doc["name"])}"/>')
                
        # Обработка файлов Unimat (Прямые ссылки на GitHub)
        elif brand == "Unimat":
            for pdf_url in UNIMAT_PDFS:
                file_name_encoded = pdf_url.split('/')[-1].replace('.pdf', '')
                file_name = urllib.parse.unquote(file_name_encoded)
                # Меняем blob на raw, чтобы робот мог скачать сам файл, а не страницу гитхаба
                download_url = pdf_url.replace("/blob/main/", "/raw/main/")
                offer_xml.append(f'<docFile url="{xml_escape(download_url)}" name="{xml_escape(file_name)}"/>')

        # ДОБАВЛЕНИЕ: Общий сертификат ТР ТС для всех позиций (Prompower и Unimat)
        common_cert_url = "https://github.com/brilka/feed-from-prompower-for-chipidip/raw/cc58148495a4db33e65ae255e720914c295e4fb0/%D0%A1%D1%81%D1%8B%D0%BB%D0%BA%D0%B0%20%D0%BD%D0%B0%20%D1%81%D0%B5%D1%80%D1%82%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%82%D1%8B%20%D0%A2%D0%A0%20%D0%A2%D0%A1.pdf"
        offer_xml.append(f'<docFile url="{xml_escape(common_cert_url)}" name="Ссылка на сертификаты ТР ТС"/>')
                
        # ГРУППА ТОВАРА
        if is_first_offer: 
            long_comment = """<!--  Код группы товара из каталога Чип и Дип. Если указан - товар будет размещен в данный раздел товара сайта Чип и Дип. Не обязательное поле. Для Prompower и Unimat вот сопоставление кодов и категорий: 
3073;19“ комплектующие;
3073;Аксессуары;
3059;Двери;
2584;Коллаборативные роботы;
2178;Модификационные комплекты для моторов;
2273;Модули расширения ПЛК PMP301;
3054;Монтажные панели;
2926;Моторные дроссели;
2178;Моторы;
2585;Серводвигатели;
2926;Сетевые дроссели;
3073;Соединительные комплекты;
3073;Шасси;
3073;Шкафы электротехнические;
2178;Опции для двигателей;
2273;Аксессуары для ПЛК;
2459;Аксессуары для реле;
2947;Дополнительные контактные приставки;
3073;Заземление;
2954;Опции для преобразователей частоты;
2947;Дополнительные контактные приставки PULSE;
3010;Колодки для реле;
3065;Прокладка кабеля;
3109;MCB (Miniature Circuit Breaker);
1559;Реле общего назначения;
2947;Контакторы PULSE;
3067;Панели основания;
2585;Аксессуары для сервосистем;
2947;Контакторы;
1559;Миниатюрные силовые реле;
2598;Сувенирная продукция;
3624;Реле тонкие;
1266;Кабели для датчиков;
2947;Миниконтакторы;
2947;Миниконтакторы PULSE;
3062;Цоколи;
3184;Климат + Свет;
2939;Блок питания HDR в пластиковом корпусе;
3124;Пластроны;
2939;Блок питания MDR в пластиковом корпусе;
3069;Боковые панели;
3062;Секционирование;
1266;Индуктивные датчики;
3115;Дополнительные контактные приставки для MCB;
2968;Опции для устройств плавного пуска;
2273;Модули расширения ПЛК PMP20/PMP30;
2939;Блок питания NDR в металлическом корпусе;
2930;Автоматы защиты двигателя PULSE;
2744;Фотоэлектрические датчики;
3071;Полки;
3067;Потолочные панели;
2954;Преобразователи частоты PD100;
2954;Преобразователи частоты PD101;
1667;Тормозные резисторы;
2954;Преобразователи частоты PD150;
2622;Панели оператора PH1;
3069;Задние панели;
3256;Промышленные коммутаторы;
2273;Панели оператора PH;
2926;ЭМС фильтры;
3062;Сейсмостойкость;
2926;Дроссели dU/dt;
2968;Устройства плавного пуска P2S 050;
2926;Дроссели для цепей постоянного тока;
2954;Преобразователи частоты PD210;
2954;Преобразователи частоты PD110;
2968;Устройства плавного пуска P2S 100;
2585;Сервоприводы;
1556;Регуляторы мощности;
2273;Программируемые логические контроллеры PMP20;
2954;Преобразователи частоты PD310;
2178;Электродвигатели класс энергоэфф. IE1;
2273;ПЛК PMP301;
3072;Каркасы;
2926;Синус-фильтры;
2954;Внешние тормозные модули для ПЧ;
2954;Преобразователи частоты PD310 IP54;
2273;Промышленный монитор;
2968;Устройства плавного пуска P2S 300;
2273;Промышленный ПК;
2273;ПЛК PMP30;
2273;Панельный ПК;
2273;Кабели и аксессуары;
2273;Модули для ПЛК;
2273;Панели оператора UniMAT;
2273;ПЛК UniMAT;
2585;Серво;
   -->"""
            offer_xml.append(long_comment)
            
        if item_group_id:
            offer_xml.append(f"<itemGroupId>{item_group_id}</itemGroupId>")
            
        if is_first_offer: offer_xml.append('<!--  Единица измерения товара. Допустимые значения (соглано ОКЕИ) -->')
        offer_xml.append("<unitID>шт</unitID>")
        
        # ВЕС ТОВАРА (с переводом кг -> граммы)
        if is_first_offer: offer_xml.append('<!--  Вес товара в граммах. Используется для вычисления тарифов по доставке товара. -->')
        weight = prod.get('weight')
        if brand == "Prompower" and weight:
            try:
                weight_grams = int(float(weight) * 1000)
                offer_xml.append(f"<weight>{weight_grams}</weight>")
            except (ValueError, TypeError):
                offer_xml.append(f"<weight>{xml_escape(str(weight))}</weight>")
                
        # ГАБАРИТЫ ТОВАРА (Ширина, Высота, Глубина)
        item_width = None
        item_height = None
        item_depth = None
        
        if brand == "Prompower":
            for prop in prod.get('props', []):
                p_name = prop.get('name', '').strip().lower()
                p_val = prop.get('value')
                
                # Игнорируем нулевые и пустые значения
                if p_val in [0, 0.0, "0", "", None]:
                    continue
                    
                if p_name in ['ширина (мм)', 'ширина']:
                    if item_width is None: item_width = p_val
                elif p_name in ['высота (мм)', 'высота']:
                    if item_height is None: item_height = p_val
                elif p_name in ['глубина (мм)', 'глубина']:
                    if item_depth is None: item_depth = p_val
                    
        if is_first_offer: offer_xml.append('<!--  Ширина товара, в миллиметрах. В API Prompower находится в props среди остальных записей. У Unimat отсутствуют данные. -->')
        if item_width is not None:
            offer_xml.append(f"<width>{xml_escape(str(item_width))}</width>")
            
        if is_first_offer: offer_xml.append('<!--  Высота товара, в миллиметрах. В API Prompower находится в props среди остальных записей. У Unimat отсутствуют данные. -->')
        if item_height is not None:
            offer_xml.append(f"<height>{xml_escape(str(item_height))}</height>")
            
        if is_first_offer: offer_xml.append('<!--  Глубина товара, в миллиметрах. В API Prompower находится в props среди остальных записей. У Unimat отсутствуют данные. -->')
        if item_depth is not None:
            offer_xml.append(f"<depth>{xml_escape(str(item_depth))}</depth>")
            
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
    all_offers_xml = []
    is_first_offer = True
    
    # Обрабатываем оба бренда
    if prompower_products:
        xml_data, is_first_offer = process_products(prompower_products, "Prompower", categories_dict, pdf_cache, is_first_offer)
        all_offers_xml.extend(xml_data)
    if unimat_products:
        xml_data, is_first_offer = process_products(unimat_products, "Unimat", categories_dict, pdf_cache, is_first_offer)
        all_offers_xml.extend(xml_data)
        
    save_pdf_cache(pdf_cache)
        
    # Формируем итоговый текстовый массив (строки) XML документа
    xml_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<shop>',
        '<name>Prompower и Unimat</name>',
        '<company>Мотрум</company>',
        '<url>https://brilka.github.io/feed-from-prompower-for-chipidip/</url>',
        '<categories>'
    ]
    
    # Добавляем все категории
    for cat_id, data in categories_dict.items():
        parent_attr = f' parentId="{data["parentId"]}"' if data['parentId'] else ''
        xml_lines.append(f'<category id="{cat_id}"{parent_attr}>{xml_escape(data["title"])}</category>')
        
    xml_lines.append('</categories>')
    xml_lines.append('<!--  список товаров к продаже  -->')
    xml_lines.append('<offers>')
    xml_lines.extend(all_offers_xml) # Вставляем список всех товаров (offers)
    xml_lines.append('</offers>')
    xml_lines.append('</shop>')
    
    # СБОРКА ОСНОВНОГО ФАЙЛА
    try:
        with open(XML_FILENAME, "w", encoding="utf-8") as f:
            f.write("\n".join(xml_lines))
        print(f"\nФайл {XML_FILENAME} успешно сгенерирован!")
    except Exception as e:
        print(f"Ошибка сохранения основного файла: {e}")
        
    # СБОРКА АВАРИЙНОГО ФАЙЛА (ZEROwarehouse)
    zero_xml_lines = []
    for line in xml_lines:
        # Регулярным выражением заменяем любое число внутри тега <qty> на 0
        if "<qty>" in line:
            line = re.sub(r'<qty>.*?</qty>', '<qty>0</qty>', line)
        zero_xml_lines.append(line)
        
    try:
        with open(XML_ZERO_FILENAME, "w", encoding="utf-8") as f:
            f.write("\n".join(zero_xml_lines))
        print(f"Файл {XML_ZERO_FILENAME} (аварийный) успешно сгенерирован!")
    except Exception as e:
        print(f"Ошибка сохранения аварийного файла: {e}")

    print(f"Парсинг и формирование завершены за {time.time() - start_time:.2f} секунд.")

if __name__ == "__main__":
    main()
