import requests
import time
import psycopg2

# Veritabanı bağlantısı kurma
conn = psycopg2.connect(
    dbname="",
    user="",
    password="",
    host="",
    port=""
)
cur = conn.cursor()


def tum_animeleri_cek():
    page = 1
    requests_per_second = 1  # İstek hızını azalttım
    seconds_per_minute = 59
    total_requests = 0

    while True:
        start_time = time.time()
        url = f"https:={page}"
        response = requests.get(url)
        total_requests += 1

        if response.status_code == 200:
            animeler = response.json()['data']
            if not animeler:  # Eğer animeler listesi boşsa döngüden çık
                break
            for anime in animeler:
                # Tüm anime bilgilerini veritabanına ekle
                kaydet(anime)
            page += 1
        elif response.status_code == 429:  # Rate Limiting hatası
            print("Rate Limiting hatası! Bekleme süresi:", response.headers.get('Retry-After'))
            time.sleep(10)  # Belirli bir süre bekle ve tekrar dene
        else:
            print("Hata:", response.text)
            break

        # Her saniyede belirli sayıda istek gönderdikten sonra bekleme
        elapsed_time = time.time() - start_time
        if total_requests % requests_per_second == 0:
            time.sleep(max(0, 1 - elapsed_time))

        # Her dakikada belirli sayıda istek gönderdikten sonra bekleme
        if total_requests % (requests_per_second * seconds_per_minute) == 0:
            time.sleep(60)


def kaydet(anime):
    # Anime bilgilerini veritabanına ekle
    insert_query = """
    INSERT INTO anime (title, source, score, mal_id, url, aired_from, aired_to, type, episodes, status, duration, rating, synopsis)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Anime bilgilerini veritabanına ekleme
    cur.execute(insert_query, (
        anime['title'],
        anime['source'],
        anime['score'],
        anime['mal_id'],
        anime['url'],
        anime['aired']['from'],
        anime['aired']['to'],
        anime['type'],
        anime['episodes'],
        anime['status'],
        anime['duration'],
        anime['rating'],
        anime['synopsis']
    ))
    conn.commit()


if __name__ == "__main__":
    tum_animeleri_cek()
