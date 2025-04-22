# config.py
import os
from dotenv import load_dotenv
from aiogram.fsm.storage.memory import MemoryStorage # Keep for local testing default
from aiogram.fsm.storage.redis import RedisStorage, DefaultKeyBuilder  # Example for production

load_dotenv() # Load environment variables from .env file

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable not set!")

# --- Bot Version (Optional) ---
BOT_VERSION = "1.1.0" # Example version

# --- !!! REPLACE THESE WITH YOUR ACTUAL IDs !!! ---
ADMIN_USER_IDS = [5295996054] # Your Telegram User ID(s) - Ensure this is INT
# Ensure these are integers for private chats, or strings like "@YourChannel" for public
MAIN_CHANNEL_ID = "@TheSquonkBotChannel" # Or -100... ID
MAIN_GROUP_ID = "@TheSquonkBotChat" # Or -100... ID
# IMPORTANT: Bot MUST be an ADMIN in MAIN_CHANNEL_ID and MAIN_GROUP_ID to post notifications!

# --- Bot Settings ---
VOTE_COOLDOWN_HOURS = 4
TRENDING_WINDOW_HOURS = 24 # How far back to look for votes for /trending and rank
SAVE_INTERVAL_SECONDS = 60 # How often to save data files

# --- File Paths ---
DATA_DIR = "data"
IMAGES_DIR = "images"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)

GROUP_CONFIG_FILE = os.path.join(DATA_DIR, "group_configs.json")
TOKEN_CACHE_FILE = os.path.join(DATA_DIR, "token_cache.json") # Will store market data now
USER_VOTES_FILE = os.path.join(DATA_DIR, "user_votes.json")
VOTES_LOG_FILE = os.path.join(DATA_DIR, "votes_log.json")
PINNED_SCOREBOARD_DATA_FILE = os.path.join(DATA_DIR, "pinned_messages.json")
MINIGAME_COOLDOWNS_FILE = os.path.join(DATA_DIR, "minigame_cooldowns.json")
USER_FREE_VOTES_FILE = os.path.join(DATA_DIR, "user_free_votes.json")
MARKET_DATA_STATE_FILE = os.path.join(DATA_DIR, "market_data_state.json") # Stores last notified multipliers etc.

# --- Scoreboard Settings ---
SCOREBOARD_UPDATE_INTERVAL_MINUTES = 10 # How often the scoreboard task runs
SCOREBOARD_UPDATE_DELAY_SECONDS = 0.7 # <<< ADDED: Delay between updating multiple scoreboards in one cycle
SCOREBOARD_TITLE = "🔥 Live Trending Tokens 🔥"
SCOREBOARD_TOP_N = 10 # Tokens on vote-based scoreboard

# --- Notification Settings ---
VOTE_IMAGE_FILENAMES = ["squonk_vote1.png", "squonk_vote2.png"]
VOTE_PROMPT_IMAGE = "vote_prompt.png" # <<< ADDED: Image for the /vote command reply in group
PROMO_URL = "https://squonk.meme"

GAINERS_POST_IMAGE = "gainers.png"         # Image for /trending or biggest gainers post
LEADERBOARD_ENTRY_IMAGE = "leaderboard.png" # Image for when a token enters the leaderboard
PUMPING_NOTIF_IMAGE = "pumping.png"       # Image for when a token hits a pump threshold

# --- Minigame Settings ---
MINIGAME_COOLDOWN_HOURS = 4
FREE_VOTE_REWARD_COUNT = 1
MINIGAME_WIN_VALUES = {"🎳": 6, "🎯": 6} # Dice value needed to win for each emoji

# --- Market Data & Related Notification Settings ---
MARKET_DATA_UPDATE_INTERVAL_MINUTES = 5 # How often to run the market data fetch cycle (BEWARE RATE LIMITS!)
MARKET_API_DELAY_SECONDS = 1.1 # <<< ADDED: Delay between API calls within the market data cycle
DEXSCREENER_API_URL = "https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
# Alternative API: Birdeye (might need API key)
# BIRDEYE_API_URL = "https://public-api.birdeye.so/public/price?address={contract_address}"
# BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY") # Add to .env if using Birdeye

FETCH_HOLDERS_ENABLED = False # << TOGGLE: Set to True to *attempt* fetching holders (adds complexity/delay)
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com") # Needed if FETCH_HOLDERS_ENABLED

# Notification Thresholds
PUMPING_MULTIPLIERS = [2, 5, 10, 25, 50, 100] # Notify when MC reaches X times initial MC
MIN_MCAP_FOR_PUMP_NOTIF = 1000 # Optional: Don't send pumping alerts for very low MC tokens

# Cooldowns for specific notification types (to prevent spamming MAIN channels)
PUMPING_NOTIF_COOLDOWN_MINUTES = 60 # Min time between "Pumping" notifications for the *same token*
LEADERBOARD_ENTRY_NOTIF_COOLDOWN_MINUTES = 120 # Min time between "Entered Leaderboard" notifications for the *same token*

ENABLE_BIGGEST_GAINERS_POST = True # <<< ADDED TOGGLE: Set to False to disable this post entirely
BIGGEST_GAINERS_POST_INTERVAL_MINUTES = 6 * 60 # How often to post the "Biggest Gainers" list (e.g., every 6 hours)
BIGGEST_GAINERS_MIN_MCAP = 5000 # <<< ADDED: Minimum Market Cap to be included in gainers list
BIGGEST_GAINERS_MIN_CHANGE_PCT = 50 # Only include tokens with > X% change in gainers list
BIGGEST_GAINERS_COUNT = 10 # <<< ADDED: Max number of tokens to show in gainers list

# --- Storage ---
# For Development/Testing:

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
# Use DefaultKeyBuilder for bot-specific keys if running multiple bots on one Redis
storage = RedisStorage.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}/0", key_builder=DefaultKeyBuilder(with_bot_id=True))