import os
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "I'm alive"

@app.route('/ping')
def ping():
    return "pong"

def run():
    port = int(os.environ.get("PORT", 9693))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run, daemon=True)
    t.start()
