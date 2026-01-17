import sqlite3  # Библиотека для базы данных (куда сохраняем)
import requests  # Чтобы делать запросы к сайту (как браузер)
from bs4 import BeautifulSoup  # Чтобы разбирать HTML-код страницы (парсинг)
import uuid  # Генерирует уникальный ID для каждой статьи
import time  # Нужно для пауз
import random  # Чтобы паузы были разной длины
from datetime import datetime, timedelta  # Работа с датами (чтобы листать дни назад)

# --- НАСТРОЙКИ ---
DB_NAME = 'articles.db'  # Имя файла базы
TARGET_COUNT = 5000  # Цель: собрать 5000 статей
BASE_URL = 'https://www.ixbt.com'
START_DATE = datetime.now()  # Начинаем с сегодняшнего дня

# Заголовки нужны, чтобы сайт думал, что мы обычный пользователь, а не бот.
# Иначе может выдать ошибку 403 (доступ запрещен).
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
}


# --- 1. РАБОТА С БАЗОЙ ДАННЫХ ---
def init_db():
    conn = sqlite3.connect(DB_NAME)  # Создаем/открываем файл базы
    cursor = conn.cursor()
    # Создаем таблицу, если ее нет.
    # url делаем UNIQUE, чтобы случайно не записать одну статью дважды.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS articles (
        guid TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        url TEXT UNIQUE,
        published_at TEXT,
        comments_count INTEGER,
        created_at_utc TEXT,
        rating REAL
    )
    ''')
    conn.commit()  # Сохраняем структуру
    return conn


def save_article(conn, data):
    try:
        cursor = conn.cursor()
        # Вставляем данные в таблицу
        cursor.execute('''
            INSERT INTO articles (guid, title, description, url, published_at, comments_count, created_at_utc, rating)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            str(uuid.uuid4()),  # Генерируем случайный код статьи
            data['title'],
            data['description'],
            data['url'],
            data['published_at'],
            0,  # Комменты пока не считаем, ставим 0
            datetime.utcnow().isoformat(),  # Время, когда мы скачали статью
            0  # Рейтинг тоже 0
        ))
        conn.commit()  # Записываем изменения
        return True
    except sqlite3.IntegrityError:
        # Если вылетел этот error, значит статья с таким URL уже есть.
        # Просто пропускаем её, возвращаем False.
        return False
    except Exception as e:
        print(f"[Ошибка БД] {e}")  # Если другая ошибка, выводим в консоль
        return False


def get_current_count(conn):
    # Просто считаем, сколько строк сейчас в базе, чтобы знать прогресс
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM articles')
    return cursor.fetchone()[0]


# --- 2. ЧИСТКА ТЕКСТА ---
def clean_text(soup_content):
    if not soup_content: return ""
    # Удаляем всякий мусор: скрипты, стили, картинки, рекламу
    for tag in soup_content(['script', 'style', 'img', 'video', 'iframe', 'figure', 'div.gallery', 'aside']):
        tag.decompose()

    # Вытаскиваем чистый текст
    text = soup_content.get_text(separator='\n')
    # Убираем лишние пробелы и пустые строки
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return '\n'.join(lines)


# --- 3. ПАРСИНГ ОДНОЙ СТАТЬИ ---
def parse_article_page(url, session):
    try:
        # Важно! Делаем небольшую случайную паузу, чтобы не грузить сервер
        # и не словить бан за спам запросами.
        time.sleep(random.uniform(0.2, 0.4))

        # Скачиваем страницу
        response = session.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200: return None  # Если страница битая, пропускаем

        # Делаем из HTML объекта "суп", чтобы искать теги
        soup = BeautifulSoup(response.text, 'html.parser')

        # Ищем заголовок H1
        title_tag = soup.find('h1')
        if not title_tag: return None
        title = title_tag.get_text(strip=True)

        # Ищем блок с текстом.
        # Тут перебор вариантов, потому что на сайте старые и новые новости имеют разную верстку.
        content_div = soup.find('div', itemprop='articleBody')
        if not content_div:
            content_div = soup.find('div', class_='b-article__content')
        if not content_div:
            content_div = soup.find('div', class_='post-content')

        if not content_div: return None

        # Чистим текст функцией выше
        description = clean_text(content_div)

        # Если текста почти нет (меньше 50 символов) — это мусор, пропускаем
        if len(description) < 50: return None

        # Пытаемся найти дату в мета-тегах
        pub_date = datetime.now().strftime('%Y-%m-%d')
        meta_date = soup.find('meta', itemprop='datePublished')
        if meta_date:
            pub_date = meta_date.get('content')

        # Возвращаем готовый словарь с данными
        return {
            'title': title,
            'description': description,
            'url': url,
            'published_at': pub_date
        }
    except Exception as e:
        print(f"Ошибка парсинга {url}: {e}")
        return None


# --- 4. ПОЛУЧЕНИЕ СПИСКА ССЫЛОК ЗА ДЕНЬ ---
def get_links_for_date(date_obj, session):
    # Собираем ссылку на архив, типа: .../news/2024/05/20/
    url = f"{BASE_URL}/news/{date_obj.strftime('%Y/%m/%d')}/"
    try:
        time.sleep(0.5)  # Тоже пауза, вежливость к серверу
        response = session.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200: return []

        soup = BeautifulSoup(response.text, 'html.parser')
        links = set()  # Set (множество) автоматически удаляет дубликаты
        date_str = date_obj.strftime('%Y/%m/%d')

        # Пробегаемся по всем ссылкам на странице
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Проверяем, что ссылка ведет именно на новость и именно за эту дату
            if '/news/' in href and date_str in href and '.html' in href:
                # Если ссылка не полная (без http), дописываем домен
                if href.startswith('http'):
                    full_link = href
                else:
                    full_link = BASE_URL + href if href.startswith('/') else BASE_URL + '/' + href
                links.add(full_link)
        return list(links)
    except:
        return []


# --- ГЛАВНЫЙ БЛОК ЗАПУСКА ---
def main():
    conn = init_db()  # Запускаем БД
    total_saved = get_current_count(conn)  # Смотрим, сколько уже есть
    session = requests.Session()  # Создаем сессию (так быстрее работает сеть)

    current_date = START_DATE  # Стартуем с сегодня

    print(f"--- ЗАПУСК ПАРСЕРА ---")
    print(f"Цель: {TARGET_COUNT} статей")
    print(f"Уже есть: {total_saved}")
    print("-" * 30)

    # Крутим цикл, пока не наберем 5000 статей
    while total_saved < TARGET_COUNT:
        d_str = current_date.strftime('%Y-%m-%d')
        print(f"Сканирую дату: {d_str}...")

        # Получаем все ссылки за этот день
        links = get_links_for_date(current_date, session)

        if not links:
            print(f"   -> Новостей нет, идем дальше.")

        # Проходим по каждой ссылке
        for link in links:
            if total_saved >= TARGET_COUNT: break  # Если хватит, выходим сразу

            # Скачиваем статью
            data = parse_article_page(link, session)

            if data:
                # Пробуем сохранить
                if save_article(conn, data):
                    total_saved += 1
                    # Каждые 10 штук пишем лог, чтобы видеть, что прога не зависла
                    if total_saved % 10 == 0:
                        print(f"   [OK] Всего: {total_saved}. Статья: {data['title'][:40]}...")

        # Самое важное: отматываем день назад (вчера, позавчера...)
        current_date -= timedelta(days=1)

    print("\n" + "=" * 30)
    print(f"ГОТОВО! Собрали {total_saved} статей.")
    conn.close()  # Закрываем базу


if __name__ == '__main__':
    main()