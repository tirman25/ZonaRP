import os
from dotenv import load_dotenv

load_dotenv()

# Bot settings
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BOT_NAME = os.getenv("BOT_NAME", "ZonaRP_bot")
PROXY_URL = os.getenv("PROXY_URL", "")

# Admin IDs
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]

# Database
DATABASE_PATH = "data/rpbot.db"

# Limits
MIN_NICKNAME_LENGTH = 5
MAX_NICKNAME_LENGTH = 15
MIN_COMMAND_NAME_LENGTH = 2
MAX_COMMAND_NAME_LENGTH = 30
MAX_DESCRIPTION_LENGTH = 200

# Report reasons
REPORT_REASONS = [
    "Оскорбление",
    "Не соответствует описанию",
    "Спам",
    "Неприемлемый контент",
    "Другое"
]
