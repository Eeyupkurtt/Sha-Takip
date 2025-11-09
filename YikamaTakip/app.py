import sqlite3
import pandas as pd
from flask import Flask, render_template, request, jsonify, g
from datetime import datetime

DATABASE = 'veritabani.db'

app = Flask(__name__)

# Tarayıcı önbelleğini zorla KAPATMA
@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

#Veritabanı Bağlantısı
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

#Veritabanını Başlat
def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Tedarikciler (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ad TEXT NOT NULL UNIQUE
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Plakalar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plaka_no TEXT NOT NULL UNIQUE,
            tedarikci_id INTEGER NOT NULL,
            FOREIGN KEY (tedarikci_id) REFERENCES Tedarikciler (id)
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Yikamacilar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ad TEXT NOT NULL UNIQUE,
            renk TEXT NOT NULL
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS YikamaKayitlari (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plaka_id INTEGER NOT NULL,
            yikamaci_id INTEGER NOT NULL,
            tarih DATE NOT NULL,
            "not" TEXT, 
            FOREIGN KEY (plaka_id) REFERENCES Plakalar (id),
            FOREIGN KEY (yikamaci_id) REFERENCES Yikamacilar (id)
        );
        """)
    
        
        #İLK BAŞTA PLAKA VE TEDARİKÇİLERİ EXCELDEN OTOMATİK EKLEDİĞİM İÇİN BU KOD VARDI ŞUAN db DEN ÇEKİYOR DİREKT

        # try:
        #     df = pd.read_excel('plakalar.xlsx')
        #     if 'Plaka' not in df.columns or 'Tedarikciler' not in df.columns:
        #         print("UYARI: Excel dosyasında 'Plaka' ve 'Tedarikciler' sütunları bulunamadı. Yükleme atlanıyor.")
        #         raise Exception("Eksik Excel sütunları")

        #     df.dropna(subset=['Plaka', 'Tedarikciler'], inplace=True)
        #     df['plaka_no_clean'] = df['Plaka'].astype(str).str.strip().str.upper()
        #     df['tedarikci_clean'] = df['Tedarikciler'].astype(str).str.strip()
        #     df = df[df['plaka_no_clean'] != '']
        #     df = df[df['tedarikci_clean'] != '']

        #     excel_tedarikciler = df['tedarikci_clean'].unique()
        #     tedarikciler_to_insert = [(ad,) for ad in excel_tedarikciler]
        #     cursor.executemany("INSERT OR IGNORE INTO Tedarikciler (ad) VALUES (?)", tedarikciler_to_insert)
            
        #     cursor.execute("SELECT id, ad FROM Tedarikciler")
        #     tedarikci_map = {row['ad']: row['id'] for row in cursor.fetchall()}
            
        #     plaka_listesi = []
        #     for _, row in df.iterrows():
        #         tedarikci_ad = row['tedarikci_clean']
        #         plaka_no = row['plaka_no_clean']
        #         tedarikci_id = tedarikci_map.get(tedarikci_ad)
                
        #         if plaka_no and tedarikci_id:
        #             plaka_listesi.append((plaka_no, tedarikci_id))
        #         else:
        #             print(f"Uyarı: Plaka {plaka_no} veya Tedarikçi {tedarikci_ad} için eşleşme bulunamadı, atlanıyor.")

        #     cursor.executemany("INSERT OR IGNORE INTO Plakalar (plaka_no, tedarikci_id) VALUES (?, ?)", plaka_listesi)
        #     print(f"Excel'den {len(plaka_listesi)} plaka/tedarikçi ilişkisi yüklendi (veya zaten mevcuttu).")

        # except FileNotFoundError:
        #     print("UYARI: 'plakalar.xlsx' dosyası bulunamadı. İlk plakalar yüklenmedi.")
        # except Exception as e:
        #     print(f"Excel okuma hatası: {e}")

        db.commit()

#Ana Sayfa Rotaları
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/arac_yonetimi')
def arac_yonetimi():
    return render_template('arac_yonetimi.html')

@app.route('/yikamaci_yonetimi')
def yikamaci_yonetimi():
    return render_template('yikamaci_yonetimi.html')

@app.route('/istatistikler')
def istatistikler():
    return render_template('istatistikler.html')

@app.route('/yikama')
def yikama():
    return render_template('yikama.html')

#Tedarikçi Yönetimi
@app.route('/api/tedarikciler', methods=['GET'])
def tedarikcileri_al():
    db = get_db()
    tedarikciler = db.execute("SELECT * FROM Tedarikciler ORDER BY ad").fetchall()
    return jsonify([dict(t) for t in tedarikciler])

@app.route('/api/tedarikci_ekle', methods=['POST'])
def tedarikci_ekle():
    data = request.json
    try:
        ad = data['ad'].strip()
        if not ad:
            return jsonify({'success': False, 'error': 'Tedarikçi adı boş olamaz'}), 400
        db = get_db()
        cursor = db.cursor()
        cursor.execute("INSERT INTO Tedarikciler (ad) VALUES (?)", (ad,))
        yeni_id = cursor.lastrowid
        db.commit()
        return jsonify({'success': True, 'yeni_tedarikci': {'id': yeni_id, 'ad': ad}})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Bu isimde bir tedarikçi zaten var'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/tedarikci_sil/<int:id>', methods=['DELETE'])
def tedarikci_sil(id):
    db = get_db()
    try:
        plakalar = db.execute("SELECT 1 FROM Plakalar WHERE tedarikci_id = ?", (id,)).fetchone()
        if plakalar:
            return jsonify({'success': False, 'error': 'Bu tedarikçiye bağlı araçlar var. Önce araçları silin veya değiştirin.'}), 400
        
        db.execute("DELETE FROM Tedarikciler WHERE id = ?", (id,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

#Araç Yönetimi
@app.route('/api/araclar', methods=['GET'])
def aracları_al():
    db = get_db()
    araclar = db.execute("""
        SELECT p.id, p.plaka_no, t.ad AS tedarikci_adi 
        FROM Plakalar p
        JOIN Tedarikciler t ON p.tedarikci_id = t.id
        ORDER BY p.plaka_no
    """).fetchall()
    return jsonify([dict(a) for a in araclar])

@app.route('/api/arac_ekle', methods=['POST'])
def arac_ekle():
    data = request.json
    try:
        plaka_no = data['plaka_no'].upper().strip()
        tedarikci_id = int(data['tedarikci_id'])
        if not plaka_no or not tedarikci_id:
            return jsonify({'success': False, 'error': 'Plaka ve tedarikçi zorunludur'}), 400
        
        db = get_db()
        cursor = db.cursor()
        cursor.execute("INSERT INTO Plakalar (plaka_no, tedarikci_id) VALUES (?, ?)", (plaka_no, tedarikci_id))
        yeni_id = cursor.lastrowid
        db.commit()
        
        yeni_arac = db.execute("""
            SELECT p.id, p.plaka_no, t.ad AS tedarikci_adi 
            FROM Plakalar p
            JOIN Tedarikciler t ON p.tedarikci_id = t.id
            WHERE p.id = ?
        """, (yeni_id,)).fetchone()
        
        return jsonify({'success': True, 'yeni_arac': dict(yeni_arac)})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Bu plaka zaten kayıtlı'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/arac_sil/<int:id>', methods=['DELETE'])
def arac_sil(id):
    db = get_db()
    try:
        db.execute("DELETE FROM YikamaKayitlari WHERE plaka_id = ?", (id,))
        db.execute("DELETE FROM Plakalar WHERE id = ?", (id,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

#Yıkamacı Yönetimi
@app.route('/api/yikamacilar', methods=['GET'])
def yikamacilari_al():
    db = get_db()
    yikamacilar = db.execute("SELECT * FROM Yikamacilar ORDER BY ad").fetchall()
    return jsonify([dict(y) for y in yikamacilar])

@app.route('/api/yikamaci_ekle', methods=['POST'])
def yikamaci_ekle():
    data = request.json
    try:
        ad = data['ad'].strip()
        renk = data['renk']
        if not ad or not renk:
            return jsonify({'success': False, 'error': 'Ad ve renk zorunludur'}), 400
        
        db = get_db()
        cursor = db.cursor()
        cursor.execute("INSERT INTO Yikamacilar (ad, renk) VALUES (?, ?)", (ad, renk))
        yeni_id = cursor.lastrowid
        db.commit()
        return jsonify({'success': True, 'yeni_yikamaci': {'id': yeni_id, 'ad': ad, 'renk': renk}})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'Bu yıkamacı zaten var'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/yikamaci_sil/<int:id>', methods=['DELETE'])
def yikamaci_sil(id):
    db = get_db()
    try:
        kayitlar = db.execute("SELECT 1 FROM YikamaKayitlari WHERE yikamaci_id = ?", (id,)).fetchone()
        if kayitlar:
            return jsonify({'success': False, 'error': 'Bu yıkamacıya ait yıkama kayıtları var. Önce kayıtları silin.'}), 400
        
        db.execute("DELETE FROM Yikamacilar WHERE id = ?", (id,))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

#İstatistikler
@app.route('/api/istatistikler', methods=['GET'])
def api_istatistikler():
    db = get_db()
    tarih_baslangic = request.args.get('tarih_baslangic')
    tarih_bitis = request.args.get('tarih_bitis')
    
    params_filtre = []
    where_filtre = ""
    if tarih_baslangic and tarih_bitis:
        where_filtre = "WHERE yk.tarih BETWEEN ? AND ?"
        params_filtre = [tarih_baslangic, tarih_bitis]

    yikamaci_stats_query = f"""
        SELECT 
            y.ad, y.renk, COUNT(yk.id) AS toplam_yikama
        FROM 
            Yikamacilar y
        LEFT JOIN 
            YikamaKayitlari yk ON y.id = yk.yikamaci_id
        {where_filtre.replace("yk.", "yk.")} 
        GROUP BY y.id
        ORDER BY toplam_yikama DESC;
    """
    yikamaci_stats = db.execute(yikamaci_stats_query, params_filtre).fetchall()

    tedarikci_stats_query = f"""
        SELECT 
            t.ad, COUNT(yk.id) AS toplam_yikama
        FROM 
            Tedarikciler t
        LEFT JOIN 
            Plakalar p ON t.id = p.tedarikci_id
        LEFT JOIN 
            YikamaKayitlari yk ON p.id = yk.plaka_id
        {where_filtre.replace("yk.", "yk.")}
        GROUP BY t.id
        ORDER BY toplam_yikama DESC;
    """
    tedarikci_stats = db.execute(tedarikci_stats_query, params_filtre).fetchall()

    detayli_stats_query = f"""
        SELECT 
            t.ad AS tedarikci_adi, 
            y.ad AS yikamaci_adi, 
            y.renk AS yikamaci_rengi, 
            COUNT(yk.id) AS toplam_yikama
        FROM 
            YikamaKayitlari yk
        JOIN 
            Plakalar p ON yk.plaka_id = p.id
        JOIN 
            Tedarikciler t ON p.tedarikci_id = t.id
        JOIN 
            Yikamacilar y ON yk.yikamaci_id = y.id
        {where_filtre}
        GROUP BY t.id, y.id
        ORDER BY tedarikci_adi, toplam_yikama DESC;
    """
    detayli_stats = db.execute(detayli_stats_query, params_filtre).fetchall()

    return jsonify({
        'yikamaci_stats': [dict(r) for r in yikamaci_stats],
        'tedarikci_stats': [dict(r) for r in tedarikci_stats],
        'detayli_stats': [dict(r) for r in detayli_stats]
    })

@app.route('/api/tedarikci_detayli_rapor', methods=['GET'])
def tedarikci_detayli_rapor():
    db = get_db()
    #Arama için ayrı tarih filtreleri
    tarih_baslangic = request.args.get('tarih_baslangic_detay')
    tarih_bitis = request.args.get('tarih_bitis_detay')
    tedarikci_ad = request.args.get('tedarikci_ad')

    if not tedarikci_ad:
        return jsonify([]) 

    params = [tedarikci_ad]
    where_kosullari = ["t.ad = ?"]

    if tarih_baslangic and tarih_bitis:
        where_kosullari.append("yk.tarih BETWEEN ? AND ?")
        params.extend([tarih_baslangic, tarih_bitis])
    
    where_sorgu = " AND ".join(where_kosullari)

    detay_sorgu = f"""
    SELECT 
        p.plaka_no,
        y.ad AS yikamaci_adi,
        y.renk AS yikamaci_rengi,
        yk.tarih,
        yk."not"
    FROM 
        YikamaKayitlari yk
    JOIN 
        Plakalar p ON yk.plaka_id = p.id
    JOIN 
        Tedarikciler t ON p.tedarikci_id = t.id
    JOIN 
        Yikamacilar y ON yk.yikamaci_id = y.id
    WHERE
        {where_sorgu}
    ORDER BY
        yk.tarih DESC;
    """
    
    kayitlar = db.execute(detay_sorgu, params).fetchall()
    return jsonify([dict(k) for k in kayitlar])


#Yıkama Kayıt
@app.route('/api/yikama_veri_al', methods=['GET'])
def yikama_veri_al():
    db = get_db()
    plakalar = db.execute("SELECT * FROM Plakalar ORDER BY plaka_no").fetchall()
    yikamacilar = db.execute("SELECT * FROM Yikamacilar ORDER BY ad").fetchall()
    return jsonify({
        'plakalar': [dict(p) for p in plakalar],
        'yikamaciler': [dict(y) for y in yikamacilar]
    })

@app.route('/api/yikama_kaydet', methods=['POST'])
def yikama_kaydet():
    data = request.json
    try:
        plaka_no = data['plaka_no'].upper().strip()
        yikamaci_id = int(data['yikamaci_id'])
        tarih = data['tarih']
        not_degeri = data.get('not', None) 
        
        if not plaka_no or not yikamaci_id or not tarih:
            return jsonify({'success': False, 'error': 'Tüm alanlar zorunludur'}), 400

        db = get_db()
        cursor = db.cursor()
        
        cursor.execute("SELECT id FROM Plakalar WHERE plaka_no = ?", (plaka_no,))
        plaka_row = cursor.fetchone()
        
        yeni_plaka_olustu = False
        if plaka_row:
            plaka_id = plaka_row['id']
        else:
            cursor.execute("SELECT id FROM Tedarikciler WHERE ad = 'Bilinmiyor'")
            bilinmiyor_id_row = cursor.fetchone()
            
            if not bilinmiyor_id_row:
                return jsonify({'success': False, 'error': "'Bilinmiyor' tedarikçisi bulunamadı."}), 500

            bilinmiyor_id = bilinmiyor_id_row['id']
            
            cursor.execute("INSERT INTO Plakalar (plaka_no, tedarikci_id) VALUES (?, ?)", (plaka_no, bilinmiyor_id))
            plaka_id = cursor.lastrowid
            yeni_plaka_olustu = True
        
        cursor.execute(
            "INSERT INTO YikamaKayitlari (plaka_id, yikamaci_id, tarih, \"not\") VALUES (?, ?, ?, ?)",
            (plaka_id, yikamaci_id, tarih, not_degeri)
        )
        db.commit()
        
        return jsonify({
            'success': True, 
            'message': 'Kayıt eklendi.',
            'yeni_plaka_olustu': yeni_plaka_olustu,
            'plaka_data': {'id': plaka_id, 'plaka_no': plaka_no}
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/yikama_kayitlari', methods=['GET'])
def yikama_kayitlari_al():
    db = get_db()
    tarih_baslangic = request.args.get('tarih_baslangic')
    tarih_bitis = request.args.get('tarih_bitis')

    params_filtre_tum = []
    where_filtre_tum = ""
    if tarih_baslangic and tarih_bitis:
        where_filtre_tum = "WHERE yk.tarih BETWEEN ? AND ?"
        params_filtre_tum = [tarih_baslangic, tarih_bitis]

    tum_kayitlar_query = f"""
    SELECT 
        yk.id, 
        p.plaka_no, 
        t.ad AS tedarikci_adi,
        y.ad AS yikamaci_adi, 
        y.renk AS yikamaci_rengi, 
        yk.tarih,
        yk."not"
    FROM 
        YikamaKayitlari yk
    JOIN 
        Plakalar p ON yk.plaka_id = p.id
    JOIN 
        Yikamacilar y ON yk.yikamaci_id = y.id
    JOIN
        Tedarikciler t ON p.tedarikci_id = t.id
    {where_filtre_tum}
    ORDER BY 
        yk.tarih DESC, yk.id DESC;
    """
    tum_kayitlar = db.execute(tum_kayitlar_query, params_filtre_tum).fetchall()
    return jsonify([dict(t) for t in tum_kayitlar])

@app.route('/api/yikama_sil/<int:kayit_id>', methods=['DELETE'])
def yikama_sil(kayit_id):
    try:
        db = get_db()
        db.execute("DELETE FROM YikamaKayitlari WHERE id = ?", (kayit_id,))
        db.commit()
        return jsonify({'success': True, 'message': 'Kayıt silindi.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/kirli_araclar', methods=['GET'])
def kirli_araclari_al():
    db = get_db()
    
    query = """
    WITH RankedYikamalar AS (
        SELECT 
            plaka_id, 
            tarih, 
            "not",
            id,
            ROW_NUMBER() OVER(PARTITION BY plaka_id ORDER BY tarih DESC, id DESC) as rn
        FROM 
            YikamaKayitlari
    ),
    SonYikama AS (
        SELECT 
            plaka_id, 
            tarih AS son_tarih,
            "not" AS son_not
        FROM 
            RankedYikamalar
        WHERE 
            rn = 1
    )
    SELECT 
        p.plaka_no,
        t.ad AS tedarikci_adi,
        sy.son_tarih,
        sy.son_not,
        CASE
            WHEN sy.son_tarih IS NULL THEN 'sari'
            WHEN sy.son_tarih <= date('now', '-7 days') THEN 'kirmizi'
            ELSE 'yesil'
        END AS durum,
        CAST(julianday('now') - julianday(sy.son_tarih) AS INTEGER) AS gecen_gun
    FROM 
        Plakalar p
    JOIN
        Tedarikciler t ON p.tedarikci_id = t.id
    LEFT JOIN 
        SonYikama sy ON p.id = sy.plaka_id
    ORDER BY p.plaka_no ASC;
    """
    
    araclar = db.execute(query).fetchall()
    
    gecikmis_var_mi = any(a['durum'] == 'kirmizi' or a['durum'] == 'sari' for a in araclar)
    
    return jsonify({
        'araclar': [dict(a) for a in araclar],
        'gecikmis_var_mi': gecikmis_var_mi
    })


#Başlat
if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5000)