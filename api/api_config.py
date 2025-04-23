import configparser
import os

_config = configparser.ConfigParser()
_ini_path = os.path.join(os.path.dirname(__file__), "config.ini")
_config.read(_ini_path)

# Fetch values and expose them as globals
# SAVE_SETTINGS
CREATE_BATCH_SIZE = int(
    _config.get("SAVE_SETTINGS", "CREATE_BATCH_SIZE", fallback="10")
)

# JWT_SETTINGS
SECRET_KEY = str(
    _config.get(
        "JWT_SETTINGS",
        "secret_key",
        fallback="5b4bb4e6fe7862a28986e67b7087f0a61385f28e32ce9284295a3ce2781afc97",
    )
)
ACCESS_TOKEN_EXPIRE_MINUTES = int(
    _config.get("JWT_SETTINGS", "ACCESS_TOKEN_EXPIRE_MINUTES", fallback=30)
)
REFRESH_TOKEN_EXPIRE_MINUTES = int(
    _config.get("JWT_SETTINGS", "REFRESH_TOKEN_EXPIRE_MINUTES", fallback=30 * 5)
)
algorithm = str(_config.get("JWT_SETTINGS", "algorithm", fallback="HS256"))
