import os
import requests
import time
import re
import random
import string
import json
import threading
import sqlite3
import asyncio
import aiohttp
import hashlib
import base64
from datetime import datetime
from random import choice as cc, randint as rr
from uuid import uuid4
from user_agent import generate_user_agent
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from queue import Queue
from collections import deque

lock = threading.Lock()
hit_ig = 0
iyi_ig = 0
bad_ig = 0
bad_gm = 0
checked_users = []
cache = {}
batch_size = 100
MAX_THREADS = 50
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3

print("\x1b[1;36mQuantex İnsta Tool V4 Başlatılıyor...\x1b[0m")
print("\x1b[1;33mDeveloper: @QuantexKanallar\x1b[0m")
print("\x1b[1;32m" + "="*50 + "\x1b[0m")

token = input("\x1b[1;32mTelegram Bot Token Girin : \x1b[1;33m")
id = input("\x1b[1;32mTelegram Id Girin : \x1b[1;33m")
time.sleep(1.5)
os.system("clear")

@dataclass
class UserData:
    username: str
    email: str
    full_name: str = ""
    follower_count: int = 0
    following_count: int = 0
    user_id: str = ""
    post_count: int = 0
    created_date: str = ""
    is_verified: bool = False
    is_private: bool = False
    biography: str = ""
    profile_pic: str = ""
    hit_time: str = ""

class DatabaseManager:
    def __init__(self, db_path: str = "quantex_instagram.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      username TEXT UNIQUE,
                      email TEXT,
                      full_name TEXT,
                      follower_count INTEGER,
                      following_count INTEGER,
                      user_id TEXT,
                      post_count INTEGER,
                      is_verified INTEGER,
                      is_private INTEGER,
                      biography TEXT,
                      profile_pic TEXT,
                      created_date TEXT,
                      hit_time TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS checked_users
                     (username TEXT PRIMARY KEY,
                      check_time TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS stats
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      date TEXT,
                      total_hits INTEGER,
                      valid_users INTEGER,
                      invalid_users INTEGER,
                      success_rate REAL)''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_username ON users(username)''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_follower ON users(follower_count)''')
        conn.commit()
        conn.close()
    
    def save_user(self, user_data: UserData):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO users 
                     (username, email, full_name, follower_count, following_count, 
                      user_id, post_count, is_verified, is_private, biography, 
                      profile_pic, created_date, hit_time)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (user_data.username, user_data.email, user_data.full_name,
                   user_data.follower_count, user_data.following_count,
                   user_data.user_id, user_data.post_count,
                   1 if user_data.is_verified else 0,
                   1 if user_data.is_private else 0,
                   user_data.biography, user_data.profile_pic,
                   user_data.created_date, user_data.hit_time))
        conn.commit()
        conn.close()
    
    def save_checked_user(self, username: str):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO checked_users (username, check_time) VALUES (?, ?)',
                  (username, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    def is_checked(self, username: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT username FROM checked_users WHERE username = ?', (username,))
        result = c.fetchone()
        conn.close()
        return result is not None
    
    def save_stats(self, total: int, valid: int, invalid: int):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        success_rate = (valid / total * 100) if total > 0 else 0
        c.execute('INSERT INTO stats (date, total_hits, valid_users, invalid_users, success_rate) VALUES (?, ?, ?, ?, ?)',
                  (datetime.now().date().isoformat(), total, valid, invalid, success_rate))
        conn.commit()
        conn.close()
    
    def get_top_users(self, limit: int = 10):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''SELECT username, full_name, follower_count, post_count 
                     FROM users 
                     ORDER BY follower_count DESC 
                     LIMIT ?''', (limit,))
        results = c.fetchall()
        conn.close()
        return results
    
    def get_stats(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM users')
        total_users = c.fetchone()[0]
        c.execute('SELECT AVG(follower_count) FROM users')
        avg_followers = c.fetchone()[0] or 0
        c.execute('SELECT SUM(post_count) FROM users')
        total_posts = c.fetchone()[0] or 0
        conn.close()
        return {
            'total_users': total_users,
            'avg_followers': int(avg_followers),
            'total_posts': total_posts
        }

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.current_index = 0
        self.lock = threading.Lock()
    
    def add_proxy(self, proxy: str):
        self.proxies.append(proxy)
    
    def add_proxies(self, proxies: List[str]):
        self.proxies.extend(proxies)
    
    def get_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxies:
            return None
        with self.lock:
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            return {'http': proxy, 'https': proxy}
    
    def load_from_file(self, filepath: str):
        try:
            with open(filepath, 'r') as f:
                proxies = [line.strip() for line in f if line.strip()]
                self.add_proxies(proxies)
        except FileNotFoundError:
            pass

class SessionPool:
    def __init__(self, pool_size: int = 50):
        self.pool = Queue(maxsize=pool_size)
        self.pool_size = pool_size
        self.headers = {
            'authority': 'www.instagram.com',
            'accept': '*/*',
            'accept-language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
            'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
        }
        self._init_pool()
    
    def _init_pool(self):
        for _ in range(self.pool_size):
            session = requests.Session()
            session.headers.update(self.headers)
            session.cookies.update(self._generate_cookies())
            self.pool.put(session)
    
    def _generate_cookies(self):
        return {
            'csrftoken': self._generate_csrftoken(),
            'mid': f'am4UQAABAAGk{''.join(random.choices(string.ascii_letters + string.digits, k=16))}',
            'ig_did': str(uuid4()),
            'ig_nrcb': '1',
            'datr': ''.join(random.choices(string.ascii_letters + string.digits, k=16)),
            'dpr': '2',
            'wd': '360x683',
        }
    
    def _generate_csrftoken(self):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=32))
    
    def get_session(self):
        return self.pool.get()
    
    def return_session(self, session):
        session.cookies.update(self._generate_cookies())
        self.pool.put(session)

class InstagramAPI:
    def __init__(self, session_pool: SessionPool, proxy_manager: ProxyManager):
        self.session_pool = session_pool
        self.proxy_manager = proxy_manager
        self.base_url = "https://www.instagram.com"
        self.api_url = f"{self.base_url}/api/v1"
        self.graphql_url = f"{self.base_url}/graphql"
        
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        session = self.session_pool.get_session()
        proxy = self.proxy_manager.get_proxy()
        
        if proxy:
            kwargs['proxies'] = proxy
        
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            if response.status_code == 200:
                return response.json() if 'json' in response.headers.get('content-type', '') else response.text
            elif response.status_code == 429:
                time.sleep(random.uniform(2, 5))
                return None
            return None
        except Exception as e:
            return None
        finally:
            self.session_pool.return_session(session)
    
    def get_user_info(self, username: str) -> Optional[UserData]:
        url = f"{self.api_url}/users"
        params = {'username': username}
        
        result = self._make_request('GET', url, params=params)
        if result and 'user' in result:
            user = result['user']
            return UserData(
                username=user.get('username', username),
                email=f"{username}@gmail.com",
                full_name=user.get('full_name', ''),
                follower_count=user.get('follower_count', 0),
                following_count=user.get('following_count', 0),
                user_id=str(user.get('id', '')),
                post_count=user.get('media_count', 0),
                is_verified=user.get('is_verified', False),
                is_private=user.get('is_private', False),
                biography=user.get('biography', ''),
                profile_pic=user.get('profile_pic_url', ''),
                created_date=date(str(user.get('id', 0))),
                hit_time=datetime.now().isoformat()
            )
        return None
    
    def search_users(self, query: str, count: int = 50) -> List[str]:
        url = f"{self.api_url}/web/search/"
        params = {'q': query, 'type': 'user', 'count': count}
        
        result = self._make_request('GET', url, params=params)
        if result and 'users' in result:
            return [user['username'] for user in result['users']]
        return []
    
    def get_friendships_many(self, user_ids: List[str]) -> Dict:
        url = f"{self.api_url}/friendships/show_many/"
        params = {'user_ids': ','.join(user_ids)}
        
        result = self._make_request('GET', url, params=params)
        return result if result else {}
    
    def get_user_by_id(self, user_id: str) -> Optional[UserData]:
        url = f"{self.api_url}/users/{user_id}/info/"
        
        result = self._make_request('GET', url)
        if result and 'user' in result:
            user = result['user']
            return UserData(
                username=user.get('username', ''),
                email=f"{user.get('username', '')}@gmail.com",
                full_name=user.get('full_name', ''),
                follower_count=user.get('follower_count', 0),
                following_count=user.get('following_count', 0),
                user_id=str(user.get('id', '')),
                post_count=user.get('media_count', 0),
                is_verified=user.get('is_verified', False),
                is_private=user.get('is_private', False),
                biography=user.get('biography', ''),
                profile_pic=user.get('profile_pic_url', ''),
                created_date=date(str(user.get('id', 0))),
                hit_time=datetime.now().isoformat()
            )
        return None
    
    def get_user_reels(self, user_id: str, count: int = 12) -> List[Dict]:
        url = f"{self.api_url}/feed/user/{user_id}/reels/"
        params = {'count': count}
        
        result = self._make_request('GET', url, params=params)
        if result and 'reels' in result:
            return result['reels']
        return []
    
    def get_user_stories(self, user_id: str) -> List[Dict]:
        url = f"{self.api_url}/feed/user/{user_id}/story/"
        
        result = self._make_request('GET', url)
        if result and 'reel' in result:
            return result['reel']['items']
        return []

class AsyncInstagramAPI:
    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager
        self.session = None
    
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        self.session = aiohttp.ClientSession(connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_user_info_async(self, username: str) -> Optional[UserData]:
        url = f"https://www.instagram.com/api/v1/users"
        params = {'username': username}
        
        headers = {
            'authority': 'www.instagram.com',
            'accept': '*/*',
            'accept-language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
            'viewport-width': '980',
            'dpr': '2',
        }
        
        try:
            async with self.session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'user' in data:
                        user = data['user']
                        return UserData(
                            username=user.get('username', username),
                            email=f"{username}@gmail.com",
                            full_name=user.get('full_name', ''),
                            follower_count=user.get('follower_count', 0),
                            following_count=user.get('following_count', 0),
                            user_id=str(user.get('id', '')),
                            post_count=user.get('media_count', 0),
                            is_verified=user.get('is_verified', False),
                            is_private=user.get('is_private', False),
                            biography=user.get('biography', ''),
                            profile_pic=user.get('profile_pic_url', ''),
                            created_date=date(str(user.get('id', 0))),
                            hit_time=datetime.now().isoformat()
                        )
        except Exception as e:
            pass
        return None
    
    async def search_users_async(self, query: str, count: int = 50) -> List[str]:
        url = "https://www.instagram.com/api/v1/web/search/"
        params = {'q': query, 'type': 'user', 'count': count}
        
        headers = {
            'authority': 'www.instagram.com',
            'accept': '*/*',
            'accept-language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
            'viewport-width': '980',
            'dpr': '2',
        }
        
        try:
            async with self.session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'users' in data:
                        return [user['username'] for user in data['users']]
        except Exception as e:
            pass
        return []

class BatchProcessor:
    def __init__(self, api: InstagramAPI, async_api: AsyncInstagramAPI):
        self.api = api
        self.async_api = async_api
        self.batch_size = 100
        self.db = DatabaseManager()
        
    def process_batch_sync(self, usernames: List[str]) -> List[Tuple[str, bool, Optional[UserData]]]:
        results = []
        for username in usernames:
            user_data = self.api.get_user_info(username)
            if user_data:
                results.append((username, True, user_data))
            else:
                results.append((username, False, None))
        return results
    
    async def process_batch_async(self, usernames: List[str]) -> List[Tuple[str, bool, Optional[UserData]]]:
        tasks = [self.async_api.get_user_info_async(username) for username in usernames]
        results = await asyncio.gather(*tasks)
        
        processed = []
        for username, user_data in zip(usernames, results):
            if user_data:
                processed.append((username, True, user_data))
            else:
                processed.append((username, False, None))
        return processed
    
    def process_friendships_batch(self, user_ids: List[str]) -> Dict:
        results = {}
        for i in range(0, len(user_ids), self.batch_size):
            batch = user_ids[i:i + self.batch_size]
            friendships = self.api.get_friendships_many(batch)
            if friendships:
                results.update(friendships)
        return results

class StatsManager:
    def __init__(self):
        self.db = DatabaseManager()
        self.start_time = datetime.now()
        self.hits_per_second = deque(maxlen=60)
        self.lock = threading.Lock()
    
    def add_hit(self):
        with self.lock:
            self.hits_per_second.append(datetime.now())
    
    def get_hits_per_second(self) -> float:
        with self.lock:
            if not self.hits_per_second:
                return 0
            now = datetime.now()
            recent = [h for h in self.hits_per_second if (now - h).seconds < 1]
            return len(recent)
    
    def get_uptime(self) -> str:
        delta = datetime.now() - self.start_time
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def get_stats(self) -> Dict:
        db_stats = self.db.get_stats()
        return {
            'uptime': self.get_uptime(),
            'hits_per_second': self.get_hits_per_second(),
            'total_users': db_stats['total_users'],
            'avg_followers': db_stats['avg_followers'],
            'total_posts': db_stats['total_posts']
        }

def date(Id):
    try:
        if int(Id) > 1 and int(Id) < 1279000:
            return "2010"
        elif int(Id) > 1279001 and int(Id) < 17750000:
            return "2011"
        elif int(Id) > 17750001 and int(Id) < 279760000:
            return "2012"
        elif int(Id) > 279760001 and int(Id) < 900990000:
            return "2013"
        elif int(Id) > 900990001 and int(Id) < 1629010000:
            return "2014"
        elif int(Id) > 1900000000 and int(Id) < 2500000000:
            return "2015"
        elif int(Id) > 2500000000 and int(Id) < 3713668786:
            return "2016"
        elif int(Id) > 3713668786 and int(Id) < 5699785217:
            return "2017"
        elif int(Id) > 5699785217 and int(Id) < 8507940634:
            return "2018"
        elif int(Id) > 8507940634 and int(Id) < 21254029834:
            return "2019"
        else:
            return "2020-2023"
    except:
        return "Bilinmiyor"

def username_generator():
    prefixes = ['user', 'insta', 'gram', 'reel', 'story', 'vip', 'pro', 'max', 'top', 'best']
    suffixes = ['official', 'real', 'life', 'world', 'daily', 'hub', 'zone', 'space']
    numbers = ''.join(random.choices(string.digits, k=random.randint(2, 6)))
    prefix = random.choice(prefixes)
    suffix = random.choice(suffixes) if random.random() > 0.5 else ''
    
    if suffix:
        return f"{prefix}{suffix}{numbers}"
    return f"{prefix}{numbers}"

def list_cek_optimized():
    api = InstagramAPI(SessionPool(10), ProxyManager())
    keywords = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 
                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    
    for _ in range(3):
        keyword = random.choice(keywords)
        results = api.search_users(keyword, count=30)
        if results:
            return random.choice(results)
        
        username = username_generator()
        user_info = api.get_user_info(username)
        if user_info:
            return username
    
    return username_generator()

def info(email):
    global hit_ig
    username = email.split('@')[0] if '@' in email else email
    db = DatabaseManager()
    
    if db.is_checked(username):
        return False
    
    api = InstagramAPI(SessionPool(5), ProxyManager())
    user_data = api.get_user_info(username)
    
    if user_data:
        hit_ig += 1
        text = f'''
INSTAGRAM HESAP BULUNDU!
═══════════════════════════
🔹 HIT: {hit_ig}
🔹 İSİM: {user_data.full_name}
🔹 KULLANICI: @{user_data.username}
🔹 EMAIL: {user_data.email}
🔹 TAKİPÇİ: {user_data.follower_count:,}
🔹 TAKİP: {user_data.following_count:,}
🔹 ID: {user_data.user_id}
🔹 POST: {user_data.post_count:,}
🔹 DOĞUM: {user_data.created_date}
🔹 ONAYLI: {"✅" if user_data.is_verified else "❌"}
🔹 GİZLİ: {"🔒" if user_data.is_private else "🌍"}
═══════════════════════════
Developer: @QuantexKanallar'''
        
        db.save_user(user_data)
        db.save_checked_user(username)
        
        with lock:
            with open("Quantex İnsta Hit.txt", "a", encoding='utf-8') as kaydet:
                kaydet.write(text + "\n")
        
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage?chat_id={id}&text={text}", timeout=10)
        except:
            pass
        
        return True
    
    return False

def arayuz(email):
    os.system("clear")
    stats = StatsManager()
    db = DatabaseManager()
    db_stats = db.get_stats()
    
    print("\x1b[1;36m" + "="*60)
    print("  QUANTEX INSTA TOOL V4 - AKTİF")
    print("  Developer: @QuantexKanallar")
    print("="*60 + "\x1b[0m")
    
    print(f'''
\x1b[1;32m 📊 ANALİZ VE İSTATİSTİKLER
\x1b[1;37m─────────────────────────────────────────────
\x1b[1;32m ✅ HIT INSTA: [ {hit_ig} ]
\x1b[1;33m 📈 İYİ INSTA: [ {iyi_ig} ]
\x1b[1;31m ❌ KÖTÜ INSTA: [ {bad_ig} ]
\x1b[1;31m 📧 KÖTÜ GMAIL: [ {bad_gm} ]
\x1b[1;37m─────────────────────────────────────────────
\x1b[1;36m 🔍 SON EMAIL: [ {email} ]
\x1b[1;36m ⏱  UPTIME: [ {stats.get_uptime()} ]
\x1b[1;36m 🚀 HIT/SN: [ {stats.get_hits_per_second():.1f} ]
\x1b[1;36m 📚 TOPLAM KULLANICI: [ {db_stats['total_users']:,} ]
\x1b[1;36m 📊 ORT. TAKİPÇİ: [ {db_stats['avg_followers']:,} ]
\x1b[1;37m─────────────────────────────────────────────''')
    
    top_users = db.get_top_users(5)
    if top_users:
        print("\x1b[1;33m 🏆 EN ÇOK TAKİPÇİSİ OLANLAR")
        print("\x1b[1;37m─────────────────────────────────────────────")
        for i, (username, full_name, followers, posts) in enumerate(top_users, 1):
            print(f"\x1b[1;36m {i}. @{username} - {followers:,} takipçi - {posts:,} gönderi")
    
    print("\x1b[1;37m" + "="*60 + "\x1b[0m")

async def async_batch_processor(usernames: List[str]):
    proxy_manager = ProxyManager()
    async with AsyncInstagramAPI(proxy_manager) as async_api:
        tasks = [async_api.get_user_info_async(username) for username in usernames]
        results = await asyncio.gather(*tasks)
        return results

def batch_main():
    global hit_ig, iyi_ig, bad_ig, bad_gm
    
    db = DatabaseManager()
    stats = StatsManager()
    batch_counter = 0
    pending_users = []
    
    while True:
        try:
            batch_counter += 1
            
            for _ in range(100):
                username = list_cek_optimized()
                if not db.is_checked(username):
                    pending_users.append(username)
                    if len(pending_users) >= 100:
                        break
            
            if pending_users:
                print(f"\x1b[1;33m[*] Batch {batch_counter} işleniyor... ({len(pending_users)} kullanıcı)\x1b[0m")
                
                with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                    futures = {executor.submit(info, f"{username}@gmail.com"): username for username in pending_users}
                    
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            if result:
                                iyi_ig += 1
                                stats.add_hit()
                            else:
                                bad_ig += 1
                        except Exception as e:
                            bad_ig += 1
                
                db.save_stats(len(pending_users), iyi_ig, bad_ig)
                pending_users = []
                
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"\x1b[1;31m[!] Batch hatası: {e}\x1b[0m")
            time.sleep(5)

if __name__ == "__main__":
    print("\x1b[1;32m[*] Quantex İnsta Tool V4 Başlatılıyor...\x1b[0m")
    
    threads = []
    for i in range(5):
        t = threading.Thread(target=batch_main, daemon=True)
        t.start()
        threads.append(t)
        print(f"\x1b[1;32m[*] Thread {i+1} başlatıldı\x1b[0m")
    
    try:
        while True:
            arayuz("Bekleniyor...")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\x1b[1;33m\n[*] Kapatılıyor...\x1b[0m")
        exit(0)