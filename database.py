import os
import json
# Replace psycopg2 with psycopg2-binary
import psycopg2
from psycopg2.extras import DictCursor, Json
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlparse

class Database:
    def __init__(self, db_url: str = None):
        try:
            # Get database URL from environment or use the provided one
            self.db_url = db_url or os.getenv('DATABASE_URL')
            if not self.db_url:
                raise ValueError("Database URL not provided. Set the DATABASE_URL environment variable.")
                
            # Parse the URL for connection parameters
            result = urlparse(self.db_url)
            self.db_params = {
                'dbname': result.path[1:],
                'user': result.username,
                'password': result.password,
                'host': result.hostname,
                'port': result.port
            }
            
            print(f"Connecting to PostgreSQL database: {result.hostname}:{result.port}/{result.path[1:]}")
            
            # Test the connection
            self._test_connection()
            
            # Initialize the database
            self._init_db()
            
        except Exception as e:
            print(f"Error initializing database: {str(e)}")
            raise
    
    def _get_connection(self):
        """Возвращает соединение с базой данных PostgreSQL"""
        try:
            conn = psycopg2.connect(
                dbname=self.db_params['dbname'],
                user=self.db_params['user'],
                password=self.db_params['password'],
                host=self.db_params['host'],
                port=self.db_params['port']
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {str(e)}")
            raise
            
    def _test_connection(self):
        """Проверяет соединение с базой данных"""
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute('SELECT version();')
                version = cur.fetchone()
                print(f"Успешное подключение к PostgreSQL. Версия: {version[0]}")
        except Exception as e:
            print(f"Ошибка при проверке соединения: {str(e)}")
            raise

    def _init_db(self):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Создаем расширение для работы с UUID
                cursor.execute('''
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                ''')
                
                # Players table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS players (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    name VARCHAR(255),
                    balance INTEGER NOT NULL DEFAULT 0,
                    last_claim VARCHAR(50) DEFAULT '0',
                    daily_streak INTEGER NOT NULL DEFAULT 0,
                    last_daily_claim TIMESTAMP,
                    inventory JSONB NOT NULL DEFAULT '[]'::jsonb,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                ''')
                
                # Market table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS market (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    seller_id BIGINT NOT NULL,
                    item_data JSONB NOT NULL,
                    price INTEGER NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (seller_id) REFERENCES players(user_id) ON DELETE CASCADE
                );
                ''')
                
                # Transactions table
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    from_user_id BIGINT,
                    to_user_id BIGINT,
                    amount INTEGER NOT NULL,
                    item_data JSONB,
                    transaction_type VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
                ''')
            
            conn.commit()
    
    # Player methods
    async def get_player(self, user_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM players WHERE user_id = %s', (user_id,))
                row = cursor.fetchone()
                
                if not row:
                    return None
                    
                columns = [desc[0] for desc in cursor.description]
                player = {}
                for i, col in enumerate(columns):
                    if col == 'inventory' and row[i] is not None:
                        player[col] = row[i] if isinstance(row[i], (list, dict)) else json.loads(row[i])
                    else:
                        player[col] = row[i]
                        
                return player
    
    async def create_player(self, user_id: int, username: str = None, name: str = None) -> Dict:
        player = {
            'user_id': user_id,
            'username': username or f'user_{user_id}',
            'name': name or f'User {user_id}',
            'balance': 0,
            'inventory': [],
            'last_claim': '0',
            'daily_streak': 0,
            'last_daily_claim': None
        }
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                INSERT INTO players (user_id, username, name, balance, inventory, last_claim, daily_streak, last_daily_claim)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    player['user_id'],
                    player['username'],
                    player['name'],
                    player['balance'],
                    json.dumps(player['inventory']),
                    player['last_claim'],
                    player['daily_streak'],
                    player['last_daily_claim']
                ))
                conn.commit()
                
        return player
    
    async def update_player(self, user_id: int, updates: Dict) -> bool:
        if not updates:
            return False
            
        set_clause = ', '.join(f"{k} = %s" for k in updates.keys())
        values = list(updates.values())
        
        # Handle JSON serialization for complex fields
        if 'inventory' in updates:
            values[list(updates.keys()).index('inventory')] = json.dumps(updates['inventory'])
        
        values.append(user_id)  # For WHERE clause
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f'UPDATE players SET {set_clause} WHERE user_id = %s',
                    values
                )
                conn.commit()
                return cursor.rowcount > 0
    
    # Market methods
    async def get_market_list(self, page: int = 1, page_size: int = 10, seller_id: int = None) -> List[Dict]:
        offset = (page - 1) * page_size
        query = 'SELECT * FROM market'
        params = []
        
        if seller_id is not None:
            query += ' WHERE seller_id = %s'
            params.append(seller_id)
            
        query += ' ORDER BY created_at DESC LIMIT %s OFFSET %s'
        params.extend([page_size, offset])
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                result = []
                columns = [desc[0] for desc in cursor.description]
                for row in rows:
                    item = {}
                    for i, col in enumerate(columns):
                        if col == 'item_data' and row[i] is not None:
                            item[col] = row[i] if isinstance(row[i], (dict, list)) else json.loads(row[i])
                        else:
                            item[col] = row[i]
                    result.append(item)
                    
                return result
    
    async def get_market_item(self, item_id: str) -> Optional[Dict]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM market WHERE id = %s', (item_id,))
                row = cursor.fetchone()
                
                if not row:
                    return None
                    
                columns = [desc[0] for desc in cursor.description]
                item = {}
                for i, col in enumerate(columns):
                    if col == 'item_data' and row[i] is not None:
                        item[col] = row[i] if isinstance(row[i], (dict, list)) else json.loads(row[i])
                    else:
                        item[col] = row[i]
                        
                return item
    
    async def add_market_item(self, seller_id: int, item_data: Dict, price: int) -> str:
        import uuid
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                INSERT INTO market (seller_id, item_data, price)
                VALUES (%s, %s, %s)
                RETURNING id
                ''', (
                    seller_id,
                    json.dumps(item_data) if isinstance(item_data, (dict, list)) else item_data,
                    price
                ))
                item_id = cursor.fetchone()[0]
                conn.commit()
                
        return str(item_id)
    
    async def remove_market_item(self, item_id: str) -> bool:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM market WHERE id = %s', (item_id,))
                conn.commit()
                return cursor.rowcount > 0
    
    # Transaction methods
    async def record_transaction(self, from_user_id: int, to_user_id: int, amount: int, 
                               item_data: Dict = None, transaction_type: str = 'trade') -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                INSERT INTO transactions (from_user_id, to_user_id, amount, item_data, transaction_type)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                ''', (
                    from_user_id,
                    to_user_id,
                    amount,
                    json.dumps(item_data) if item_data else None,
                    transaction_type
                ))
                transaction_id = cursor.fetchone()[0]
                conn.commit()
                return transaction_id

# Global database instance
db = Database()

# Backward compatibility functions
async def get_player(user_id: int) -> Optional[Dict]:
    return await db.get_player(user_id)

async def create_player(user_id: int, username: str = None, name: str = None) -> Dict:
    return await db.create_player(user_id, username, name)

async def update_player(user_id: int, updates: Dict) -> bool:
    return await db.update_player(user_id, updates)

async def get_market_list() -> List[Dict]:
    return await db.get_market_list()

async def add_market_item(seller_id: int, item_data: Dict, price: int) -> str:
    return await db.add_market_item(seller_id, item_data, price)

async def remove_market_item(item_id: str) -> bool:
    return await db.remove_market_item(item_id)

async def get_market_item(item_id: str) -> Optional[Dict]:
    return await db.get_market_item(item_id)


class Database:
    def __init__(self, db_path: str = 'bot_data.db'):
        # Get database path from environment or use default
        db_url = os.getenv('DATABASE_URL')
        if db_url and db_url.startswith('sqlite:///'):
            self.db_path = db_url.replace('sqlite:///', '')
        else:
            # Use absolute path for the database file
            base_dir = os.path.dirname(os.path.abspath(__file__))
            db_dir = os.path.join(base_dir, 'data')
            
            # Create data directory if it doesn't exist
            os.makedirs(db_dir, exist_ok=True)
            
            self.db_path = os.path.join(db_dir, db_path)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        
        self._init_db()
    
    def _get_connection(self):
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir:  # Only create directory if path is not in current directory
            os.makedirs(db_dir, exist_ok=True)
            
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Players table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                name TEXT,
                balance INTEGER DEFAULT 0,
                last_claim TEXT DEFAULT '0',
                daily_streak INTEGER DEFAULT 0,
                last_daily_claim TEXT,
                inventory TEXT DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Market table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS market (
                id TEXT PRIMARY KEY,
                seller_id INTEGER,
                item_data TEXT,
                price INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (seller_id) REFERENCES players(user_id)
            )
            ''')
            
            # Transactions table (optional, for history)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_user_id INTEGER,
                to_user_id INTEGER,
                amount INTEGER,
                item_data TEXT,
                transaction_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            conn.commit()
    
    # Player methods
    async def get_player(self, user_id: int) -> Optional[Dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM players WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
                
            columns = [desc[0] for desc in cursor.description]
            player = dict(zip(columns, row))
            
            # Parse JSON fields
            if 'inventory' in player and isinstance(player['inventory'], str):
                player['inventory'] = json.loads(player['inventory'])
                
            return player
    
    async def create_player(self, user_id: int, username: str = None, name: str = None) -> Dict:
        player = {
            'user_id': user_id,
            'username': username or f'user_{user_id}',
            'name': name or f'User {user_id}',
            'balance': 0,
            'inventory': [],
            'last_claim': '0',
            'daily_streak': 0,
            'last_daily_claim': None
        }
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO players (user_id, username, name, balance, inventory, last_claim, daily_streak, last_daily_claim)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                player['user_id'],
                player['username'],
                player['name'],
                player['balance'],
                json.dumps(player['inventory']),
                player['last_claim'],
                player['daily_streak'],
                player['last_daily_claim']
            ))
            conn.commit()
            
        return player
    
    async def update_player(self, user_id: int, updates: Dict) -> bool:
        if not updates:
            return False
            
        set_clause = ', '.join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values())
        
        # Handle JSON serialization for complex fields
        if 'inventory' in updates:
            values[list(updates.keys()).index('inventory')] = json.dumps(updates['inventory'])
        
        values.append(user_id)  # For WHERE clause
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f'UPDATE players SET {set_clause} WHERE user_id = ?', values)
            conn.commit()
            return cursor.rowcount > 0
    
    # Market methods
    async def get_market_list(self, page: int = 1, page_size: int = 10, seller_id: int = None) -> List[Dict]:
        offset = (page - 1) * page_size
        query = 'SELECT * FROM market'
        params = []
        
        if seller_id is not None:
            query += ' WHERE seller_id = ?'
            params.append(seller_id)
            
        query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?'
        params.extend([page_size, offset])
        
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                item = dict(row)
                if 'item_data' in item and item['item_data']:
                    item['item_data'] = json.loads(item['item_data'])
                result.append(item)
                
            return result
    
    async def get_market_item(self, item_id: str) -> Optional[Dict]:
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM market WHERE id = ?', (item_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
                
            item = dict(row)
            if 'item_data' in item and item['item_data']:
                item['item_data'] = json.loads(item['item_data'])
                
            return item
    
    async def add_market_item(self, seller_id: int, item_data: Dict, price: int) -> str:
        import uuid
        
        item_id = str(uuid.uuid4())
        item_json = json.dumps(item_data)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO market (id, seller_id, item_data, price)
            VALUES (?, ?, ?, ?)
            ''', (item_id, seller_id, item_json, price))
            conn.commit()
            
        return item_id
    
    async def remove_market_item(self, item_id: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM market WHERE id = ?', (item_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    # Transaction methods
    async def record_transaction(self, from_user_id: int, to_user_id: int, amount: int, 
                               item_data: Dict = None, transaction_type: str = 'trade') -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO transactions (from_user_id, to_user_id, amount, item_data, transaction_type)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                from_user_id,
                to_user_id,
                amount,
                json.dumps(item_data) if item_data else None,
                transaction_type
            ))
            conn.commit()
            return cursor.lastrowid

# Global database instance
db = Database()

# Backward compatibility functions
async def get_player(user_id: int) -> Optional[Dict]:
    return await db.get_player(user_id)

async def create_player(user_id: int, username: str = None, name: str = None) -> Dict:
    return await db.create_player(user_id, username, name)

async def update_player(user_id: int, updates: Dict) -> bool:
    return await db.update_player(user_id, updates)

async def get_market_list() -> List[Dict]:
    return await db.get_market_list()

async def add_market_item(seller_id: int, item_data: Dict, price: int) -> str:
    return await db.add_market_item(seller_id, item_data, price)

async def remove_market_item(item_id: str) -> bool:
    return await db.remove_market_item(item_id)

async def get_market_item(item_id: str) -> Optional[Dict]:
    return await db.get_market_item(item_id)
