import json

# Problem: Centralize system configuration (e.g., logging level, strategy parameters).
# Expectations:
# Implement a Singleton Config class.
# Load settings from config.json.
# Ensure all modules access the same instance.

class Config:

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self): #idk if we need this
        if not Config._initialized:
            self.config = self.load_config()
            Config._initialized = True

    def load_config(self, filepath: str = 'inputs/config.json'):
        with open(filepath, 'r') as f:
            return json.load(f)