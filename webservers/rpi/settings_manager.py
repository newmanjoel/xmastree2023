class SettingsManager:
    _instance = None

    def __init__(self, *args, **kwargs):
        self.settings = {}

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(SettingsManager, cls).__new__(cls)
            # cls._instance.settings = {}
        return cls._instance

    def set_setting(self, key, value):
        self.settings[key] = value

    def get_setting(self, key):
        return self.settings.get(key, None)
