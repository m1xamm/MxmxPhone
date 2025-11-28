import os
import json
from firebase_admin import credentials, firestore, initialize_app, get_app, get_apps
from firebase_admin.firestore import SERVER_TIMESTAMP

# Глобальная переменная для хранения экземпляра приложения
firebase_app = None

def init_firebase():
    global firebase_app
    
    # Если приложение уже инициализировано, возвращаем существующий клиент
    if firebase_app is not None:
        return firebase_app
    
    try:
        # Используем локальный файл с учетными данными р
        cred = credentials.Certificate({
            "type": "service_account",
            "project_id": "mxphonebot",
            "private_key_id": "f17730d98273308082f3aa334cd5816595066e52",
            "private_key": """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCZpPqM9AeWXpSe\n6oCuupb460GgH65qiYQENp0DemaS82deuf3AV4lPWG56wY9f0uGAdy7L59ToF3C9\n6g6py/300STvFXJKQjCHT5mKXlW5m3eNcRu/yjLM9Lj9Ez6VVscqncBDLxGDZynY\nlu0azYE5hfLOp3W3SMwL3XpKBFxhqxVrDDriWYMaEQODwdzU9ifnHPRi6E14uLkk\nz6XuoK0xko1UHovmlp4rT+f/ET+csKz9B4jASaQc9BOYz1iLCObvwYFgpsjzF3Pm\nxrMx9yu+TiYi89GyCU4739zOzgbEt2XTzuExQuLHdy5HPIUi5giIUnzcu1TRleo7\nYEmoR1N3AgMBAAECggEAMJYbxzwaNCJ3pCNrCxYlTNT0XUL3gGg5L63njCSGUF0Z\naLqvNEZaPRWtZjNUeAxzVBEcYs3OpZBYDi54cZh5GBSVNefVywbQWtHAr6FyQW4Y\n6ckFaepykx6max1NUqNs+xyEopo9UwzqDjx4AVxQS5UAufn5vzqNkAj7NXHYlueq\nEGhGaU7Vzi/KUbPHToiRUwfTaf+55I9hpek8lNhf/eVOSEjXORAKrTtLZx1uSk/R\nTG/cLtjqkVYfVUatibgGT2NoLYwtNxxT/1ZZFZkhfm/8L7BdCMzk2hIDo9wJBbZt\n2PIGn6r5Tk7Pv9gIZWTmbhZx/EKwR/nSnwpJEZUwBQKBgQDRhYfOJ16XoTyLQgpc\nqNvTSFKBXS6TfM9BaWtu7K+68EhZQhmnCb+9vWQP3WVOiPu8wmjA1xP0gYKjET15\ntUq4ulvk1+TuzswtNOfzKp1gu1y6oF6Q+kxzr5lmk1gdA4AnyAm76n/6vut2cQdU\nic3bxe1IaKntZpPeoMZSsXghewKBgQC7ukEpSNiNvaBOERVsSw9R7Rw0iHgXuU3C\n8WRYPCDZ93SCNIfTFRbqWFd9HO++9B668wqiskhwKuvs1ytb7iaVbv58TQQaBoe0\nEXfff1jEQPpLMZfRwlS1KEO/dBUaA1VTgpJ2WyROcES9YZDGUu2sBX1Y1dFMzWzs\npNyBpcefNQKBgCYxzGb1YFYN64aLXG41zhT/CyNQBEyYpQOMnywSc5qFcPrshNah\nfVWub85AktY1PIbVfdkhnB6neVQWsXk9Zki0mEnoXXB3PFtFWL9IVnYq0aWn1HVj\nW4p/SVycoaRwXe1ilvutrPTd1vi5dBeiI2fb9fyML+X6HByqfFzYw0h/AoGADsoG\nliUKAmic92l7IZPsOg0O+siBhYTwrlnsCNN71xAamqNey+9OZdnd0ppz/Lwoq5u4\nC8c107hd68Orw8tION+Mpug+WXqIOFRj+DSFHrjrvv7CMiE4ISlx5ORVQT5f+3s+\n5Jobix5nG/BSwn0IAlRQYq49lepdGWnDf6M6zb0CgYB1suNUCm21QqQdK9m9rqjG\nsGbkv5EuTJj8aeAiVhnXogNvjPetSRsW+pHgNfxXPlKEVZv6kshNJ6fYU/8wMXgp\n+YKKyNAbTCNdoHULcKXWJfNjOYXCsLVIt9Vw61MWHNv58Mu9/QVktSsW/SwNaoGL\nytmuHBXLjCpq4+OLVF7dPw==\n-----END PRIVATE KEY-----\n""",
            "client_email": "firebase-adminsdk-fbsvc@mxphonebot.iam.gserviceaccount.com",
            "client_id": "107182062953867814929",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40mxphonebot.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        })
        
        # Проверяем, есть ли уже инициализированные приложения
        if not get_apps():
            firebase_app = initialize_app(cred)
            print("✅ Успешное подключение к Firebase")
        else:
            firebase_app = get_app()
            print("ℹ️ Используется существующее подключение к Firebase")
            
        return firestore.client()
    except Exception as e:
        print(f"❌ Ошибка при подключении к Firebase: {e}")
        import traceback
        traceback.print_exc()
        raise

# Initialize Firebase
db = init_firebase()

class FirebaseDB:
    def __init__(self):
        self.db = db
        self.players_ref = self.db.collection('players')
        self.market_ref = self.db.collection('market')
        self.data_ref = self.db.collection('app_data').document('main')
    
    async def get_player(self, user_id: int):
        doc = self.players_ref.document(str(user_id)).get()
        return doc.to_dict() if doc.exists else None
    
    async def update_player(self, user_id: int, data: dict):
        self.players_ref.document(str(user_id)).set(data, merge=True)
    
    async def get_all_players(self):
        docs = self.players_ref.stream()
        return {int(doc.id): doc.to_dict() for doc in docs}
    
    async def get_market_list(self):
        docs = self.market_ref.stream()
        return [doc.to_dict() for doc in docs]
    
    async def add_market_item(self, item: dict):
        self.market_ref.document(item['id']).set(item)
    
    async def remove_market_item(self, item_id: str):
        self.market_ref.document(item_id).delete()
    
    async def get_market_item(self, item_id: str):
        doc = self.market_ref.document(item_id).get()
        return doc.to_dict() if doc.exists else None
    
    async def get_app_data(self):
        doc = self.data_ref.get()
        return doc.to_dict() if doc.exists else {}
    
    async def update_app_data(self, data: dict):
        self.data_ref.set(data, merge=True)

# Initialize FirebaseDB instance
firebase_db = FirebaseDB()
