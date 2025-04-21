import asyncio
import json
import logging
import os
import random # For random image selection & potentially minigames
import typing
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import aiofiles
import aiohttp # For making HTTP requests
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode, ChatType, ChatMemberStatus # <<< IMPORT ChatMemberStatus
from aiogram.filters import Command, CommandStart, StateFilter, BaseFilter, ChatMemberUpdatedFilter, IS_ADMIN, IS_MEMBER
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
# Storage is now imported and defined in config.py
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatMemberUpdated, FSInputFile, ChatMemberAdministrator, User # Added User
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramNetworkError, TelegramForbiddenError, TelegramRetryAfter # Added RetryAfter
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage # Import for type checking warning
from aiogram.utils.markdown import hlink, hcode, hbold, hitalic # Import more markdown helpers


# --- Configuration ---
try:
    import config
    storage = config.storage # Get storage from config
except ImportError:
    print("Error: config.py not found. Please create it based on the example.")
    exit()
except AttributeError:
    print("Error: Storage not defined in config.py. Please check your config.")
    exit()

# --- Logging ---
# Basic config is set in __main__, configure logger instance here
# --- FIX: Move logger definition up ---
logger = logging.getLogger(__name__)

# Check for essential config variables early
if not hasattr(config, 'BOT_TOKEN') or not config.BOT_TOKEN:
     # Logger might not be fully configured yet if basicConfig fails, use print
     print("CRITICAL ERROR: BOT_TOKEN not found or empty in config.py!")
     exit() # Exit if no token
if not hasattr(config, 'ADMIN_USER_IDS') or not config.ADMIN_USER_IDS:
     # Use logger now as basicConfig should be okay
     logger.warning("ADMIN_USER_IDS not set in config.py. Some admin functions might be restricted.")
# Add checks for other critical variables if needed


# --- Data Structures --- (Keep as is)
group_configs: typing.Dict[str, typing.Dict] = {} # { "group_id_str": { "token_key": "...", "config": {...} } }
token_cache: typing.Dict[str, typing.Dict] = {}   # { "contract_chain": {"symbol": "...", "name": "...", "group_link": "...", "market_cap_usd": ..., ... } }
user_votes: typing.Dict[str, typing.Dict[str, str]] = {} # Regular vote cooldowns: { user_id_str: { token_key: iso_timestamp } }
votes_log: typing.List[typing.Dict] = []          # [ {"user_id": ..., "user_name": ..., "contract_chain": ..., "timestamp": ..., "is_free_vote": bool} ]
pinned_messages: typing.Dict[str, int] = {} # For Scoreboard: { "chat_id_str": message_id }
minigame_cooldowns: typing.Dict[str, str] = {} # { user_id_str: iso_timestamp_last_played }
user_free_votes: typing.Dict[str, typing.Dict[str, int]] = {} # { user_id_str: { token_key: count } }
market_data_state: typing.Dict[str, typing.Dict] = {} # { token_key: { "last_notified_multiplier": X, "last_pump_notif_ts": ISO, "last_entry_notif_ts": ISO, "previous_vote_rank": Y }, "_internal": {"last_gainers_post_iso": ISO} }
last_gainers_post_time: typing.Optional[datetime] = None # Track last gainers post time (loaded from market_data_state)

# --- Bot Setup --- (Keep as is)
bot = Bot(
    token=config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=storage)
router = Router()
http_session: typing.Optional[aiohttp.ClientSession] = None

# --- Data Persistence --- (Keep as is)
async def load_data():
    global group_configs, token_cache, user_votes, votes_log, pinned_messages, minigame_cooldowns, user_free_votes, market_data_state, last_gainers_post_time
    os.makedirs(config.DATA_DIR, exist_ok=True)
    data_files = [
        (config.GROUP_CONFIG_FILE, "group_configs", {}),
        (config.TOKEN_CACHE_FILE, "token_cache", {}),
        (config.USER_VOTES_FILE, "user_votes", {}),
        (config.VOTES_LOG_FILE, "votes_log", []),
        (config.PINNED_SCOREBOARD_DATA_FILE, "pinned_messages", {}),
        (config.MINIGAME_COOLDOWNS_FILE, "minigame_cooldowns", {}),
        (config.USER_FREE_VOTES_FILE, "user_free_votes", {}),
        (config.MARKET_DATA_STATE_FILE, "market_data_state", {}), # Load market state
    ]
    for path, var_name, default_value in data_files:
        default_type = type(default_value)
        try:
            async with aiofiles.open(path, mode='r', encoding='utf-8') as f:
                content = await f.read()
                if not content: # Handle empty file case
                    logger.warning(f"Data file {path} is empty. Starting with default {default_type.__name__}.")
                    globals()[var_name] = default_value
                    continue
                data = json.loads(content)

                if isinstance(data, default_type):
                    globals()[var_name] = data
                    # Log length for lists/dicts, just 'dict' for dicts
                    data_size = len(data) if hasattr(data, '__len__') else 'dict'
                    logger.info(f"Loaded data from {path} ({data_size} items)")
                else:
                    logger.error(f"Invalid data type loaded from {path}. Expected {default_type}, got {type(data)}. Starting empty.")
                    globals()[var_name] = default_value
        except FileNotFoundError:
            logger.warning(f"Data file not found: {path}. Starting with empty {default_type.__name__}.")
            globals()[var_name] = default_value
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {path}: {e}. Starting empty {default_type.__name__}.")
            globals()[var_name] = default_value
        except Exception as e:
            logger.exception(f"Failed to load data from {path}: {e}")
            globals()[var_name] = default_value

    # Load last gainers post time from market_data_state
    state_data = market_data_state.get("_internal", {})
    last_gainers_iso = state_data.get("last_gainers_post_iso")
    if last_gainers_iso:
        try:
            loaded_time = datetime.fromisoformat(last_gainers_iso)
            # Ensure timezone aware
            if loaded_time.tzinfo is None:
                last_gainers_post_time = loaded_time.replace(tzinfo=timezone.utc)
            else:
                 last_gainers_post_time = loaded_time.astimezone(timezone.utc) # Convert to UTC if different TZ
            logger.info(f"Loaded last gainers post time: {last_gainers_post_time}")
        except ValueError:
            logger.error(f"Could not parse stored last_gainers_post_iso '{last_gainers_iso}' from market_data_state.")
            last_gainers_post_time = None
        except Exception as e:
             logger.exception(f"Error processing stored last_gainers_post_iso: {e}")
             last_gainers_post_time = None

async def save_single_file(path: str, data_structure: typing.Any):
    """Atomically saves a single data structure to a JSON file."""
    temp_path = f"{path}.tmp"
    try:
        async with aiofiles.open(temp_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(data_structure, indent=2, ensure_ascii=False))
        # Atomically replace the old file with the new one
        os.replace(temp_path, path)
        logger.debug(f"Data saved successfully to {path}")
    except Exception as e:
        logger.exception(f"Failed to save data to {path}: {e}")
        # Clean up temp file on error if it exists
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                logger.info(f"Removed temporary save file {temp_path} after error.")
            except OSError as ose:
                logger.error(f"Error removing temporary save file {temp_path} after exception: {ose}")
        # Re-raise the exception so asyncio.gather catches it
        raise

async def save_data():
    global group_configs, token_cache, user_votes, votes_log, pinned_messages, minigame_cooldowns, user_free_votes, market_data_state, last_gainers_post_time
    os.makedirs(config.DATA_DIR, exist_ok=True)

    # Save last gainers post time into market_data_state
    try:
        if last_gainers_post_time:
            if "_internal" not in market_data_state: market_data_state["_internal"] = {}
            market_data_state["_internal"]["last_gainers_post_iso"] = last_gainers_post_time.isoformat()
        elif "_internal" in market_data_state and "last_gainers_post_iso" in market_data_state["_internal"]:
            # Clean up if time is None but key exists
            logger.debug("last_gainers_post_time is None, removing from market_data_state.")
            del market_data_state["_internal"]["last_gainers_post_iso"]
            if not market_data_state["_internal"]: # Remove internal if empty
                logger.debug("Removing empty _internal dict from market_data_state.")
                del market_data_state["_internal"]
    except Exception as e:
         logger.exception(f"Error processing last_gainers_post_time for saving: {e}")

    data_to_save = {
        config.GROUP_CONFIG_FILE: group_configs,
        config.TOKEN_CACHE_FILE: token_cache,
        config.USER_VOTES_FILE: user_votes,
        config.VOTES_LOG_FILE: votes_log,
        config.PINNED_SCOREBOARD_DATA_FILE: pinned_messages,
        config.MINIGAME_COOLDOWNS_FILE: minigame_cooldowns,
        config.USER_FREE_VOTES_FILE: user_free_votes,
        config.MARKET_DATA_STATE_FILE: market_data_state,
    }
    logger.debug(f"Preparing to save data for {len(data_to_save)} files.")
    save_tasks = []
    for path, data_structure in data_to_save.items():
        # Launch each save operation concurrently
        save_tasks.append(asyncio.create_task(save_single_file(path, data_structure), name=f"SaveTask_{os.path.basename(path)}"))

    # Wait for all save operations to complete
    results = await asyncio.gather(*save_tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if i < len(save_tasks): # Ensure index is valid
            task_name = save_tasks[i].get_name()
            if isinstance(result, Exception):
                logger.error(f"Error in background save task '{task_name}': {result}")
            else:
                logger.debug(f"Background save task '{task_name}' completed successfully.")
        else:
             logger.error(f"Result index {i} out of bounds for save tasks (length {len(save_tasks)})")


async def periodic_save():
    while True:
        await asyncio.sleep(config.SAVE_INTERVAL_SECONDS)
        logger.debug(f"Initiating periodic save (interval: {config.SAVE_INTERVAL_SECONDS}s)")
        try:
            await save_data()
        except Exception as e:
            logger.exception(f"Error during periodic save operation: {e}")


# --- Helper Functions --- (Keep as is, relies on fixed is_user_admin)
def get_token_key(contract: str, chain: str) -> str:
    """Generates a unique key for a token based on contract and chain."""
    # Ensure lowercase for consistency
    return f"{contract.lower()}_{chain.lower()}"

async def is_user_admin(chat_id: typing.Union[int, str], user_id: int) -> bool:
    """Checks if a user is an admin or creator in a specific chat."""
    try:
        target_chat_id = chat_id
        if isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
             target_chat_id = int(chat_id)
        elif isinstance(chat_id, str) and chat_id.startswith('@'):
             target_chat_id = chat_id # Use username string directly
        elif not isinstance(chat_id, int):
             logger.warning(f"Admin check received potentially invalid chat ID type: {chat_id} ({type(chat_id)}). Trying anyway.")
             # Let aiogram handle it, might work for usernames depending on version/context

        logger.debug(f"Checking admin status for user {user_id} in chat {target_chat_id}")
        member = await bot.get_chat_member(target_chat_id, user_id)
        # --- Use ChatMemberStatus ---
        is_admin_or_creator = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
        logger.debug(f"User {user_id} status in chat {target_chat_id}: {member.status}. Is admin/creator: {is_admin_or_creator}")
        return is_admin_or_creator
    except (TelegramAPIError, TelegramBadRequest, ValueError) as e:
        err_msg = str(e).lower()
        if isinstance(e, (ValueError, TelegramBadRequest)) and ("chat not found" in err_msg or "peer_id_invalid" in err_msg):
            logger.warning(f"Admin check failed for user {user_id} in chat {chat_id}: Chat not found or invalid ID.")
        elif isinstance(e, (ValueError, TelegramBadRequest)) and ("user not found" in err_msg):
             logger.warning(f"Admin check failed for user {user_id} in chat {chat_id}: User not found.")
        elif isinstance(e, TelegramForbiddenError) or "not enough rights" in err_msg or "bot is not a participant" in err_msg:
             logger.warning(f"Admin check failed for user {user_id} in chat {chat_id}: Bot lacks permissions or is not in the chat.")
        else:
             logger.error(f"Error checking admin status for user {user_id} in chat {chat_id}: {type(e).__name__} - {e}")
        return False
    except Exception as e:
        logger.exception(f"Unexpected error checking admin status for user {user_id} in {chat_id}: {e}")
        return False

async def check_main_membership(user_id: int) -> typing.Tuple[bool, bool]:
    """Checks if user is member of main channel and group. Returns (in_channel, in_group)."""
    async def check_chat_membership(chat_id_config: typing.Union[int, str], user_id: int, chat_name: str) -> bool:
        if not chat_id_config:
            logger.warning(f"{chat_name} ID not set in config. Skipping membership check.")
            return True # Assume membership if not configured to check

        chat_id = None
        try:
            if isinstance(chat_id_config, str) and chat_id_config.lstrip('-').isdigit():
                chat_id = int(chat_id_config)
            elif isinstance(chat_id_config, int):
                chat_id = chat_id_config
            elif isinstance(chat_id_config, str) and chat_id_config.startswith('@'):
                chat_id = chat_id_config # Use username string
            else:
                logger.error(f"Invalid chat ID format for {chat_name}: {chat_id_config}. Skipping check.")
                return False

            logger.debug(f"Checking membership for user {user_id} in {chat_name} ({chat_id})")
            member = await bot.get_chat_member(chat_id, user_id)
            logger.debug(f"User {user_id} status in {chat_name}: {member.status}")
            # --- Use ChatMemberStatus Enum ---
            is_member = member.status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]
            logger.debug(f"Is member of {chat_name}: {is_member}")
            return is_member

        except (TelegramAPIError, TelegramBadRequest, ValueError) as e:
            err_msg = str(e).lower()
            if "chat not found" in err_msg or "user not found" in err_msg or "peer_id_invalid" in err_msg:
                 logger.warning(f"Could not check {chat_name} ({chat_id}) membership for {user_id}: Chat/User not found or invalid ID.")
            elif "member status is unknown" in err_msg:
                 logger.warning(f"Could not determine {chat_name} ({chat_id}) membership for {user_id}: Status unknown (maybe bot needs admin rights?).")
            elif "bot is not a member" in err_msg or "bot is not a participant" in err_msg:
                 logger.warning(f"Could not check {chat_name} ({chat_id}) membership: Bot is not a member of the chat.")
            else:
                 logger.error(f"Could not check {chat_name} ({chat_id}) membership for {user_id}: {type(e).__name__} - {e}")
        except Exception as e:
            logger.exception(f"Unexpected error checking {chat_name} ({chat_id}) membership for {user_id}: {e}")
        return False # Default to False on error

    in_channel = await check_chat_membership(config.MAIN_CHANNEL_ID, user_id, "Main Channel")
    in_group = await check_chat_membership(config.MAIN_GROUP_ID, user_id, "Main Group")
    logger.info(f"Membership check result for user {user_id}: In Channel={in_channel}, In Group={in_group}")
    return in_channel, in_group

def get_cooldown_remaining(user_id: int, token_key: str) -> typing.Optional[timedelta]:
    """Calculates remaining REGULAR vote cooldown time for a user and token. Cleans up expired entries."""
    user_id_str = str(user_id)
    user_token_votes = user_votes.get(user_id_str, {})

    if token_key in user_token_votes:
        try:
            last_vote_time_str = user_token_votes[token_key]
            last_vote_time = datetime.fromisoformat(last_vote_time_str)
            # Ensure timezone aware comparison
            if last_vote_time.tzinfo is None:
                 last_vote_time = last_vote_time.replace(tzinfo=timezone.utc) # Assume UTC

            cooldown_end = last_vote_time + timedelta(hours=config.VOTE_COOLDOWN_HOURS)
            now = datetime.now(timezone.utc) # Use timezone aware now

            if now < cooldown_end:
                 remaining = cooldown_end - now
                 logger.debug(f"Cooldown check for user {user_id_str}, token {token_key}: Remaining {remaining}")
                 return remaining
            else:
                 # Cooldown has expired, clean up the entry
                 logger.debug(f"Cooldown expired for user {user_id_str}, token {token_key}. Cleaning up.")
                 # Use try-except for deletion safety in case of race conditions
                 try:
                     del user_votes[user_id_str][token_key]
                     if not user_votes[user_id_str]: # Remove user dict if empty
                         del user_votes[user_id_str]
                 except KeyError:
                      logger.warning(f"KeyError during cooldown cleanup for {user_id_str}/{token_key}. Already removed?")
                 # Defer saving to the caller or periodic save
                 return None
        except ValueError:
             logger.error(f"Invalid timestamp format for user {user_id_str}, token {token_key}: '{user_token_votes[token_key]}'. Removing entry.")
             # Clean up invalid entry
             try:
                 del user_votes[user_id_str][token_key]
                 if not user_votes[user_id_str]: del user_votes[user_id_str]
             except KeyError: pass
             return None
        except Exception as e:
            logger.exception(f"Error calculating cooldown for user {user_id_str}, token {token_key}: {e}")
            return None # Treat error as cooldown finished to avoid blocking user
    else:
        logger.debug(f"No cooldown entry found for user {user_id_str}, token {token_key}")
        return None # No cooldown entry found

def format_timedelta(delta: typing.Optional[timedelta]) -> str:
    """Formats timedelta into H hours M minutes S seconds."""
    if delta is None: return "N/A"
    total_seconds = int(delta.total_seconds());
    if total_seconds <= 0: return "0s"
    hours, remainder = divmod(total_seconds, 3600); minutes, seconds = divmod(remainder, 60); parts = []
    if hours > 0: parts.append(f"{hours}h")
    if minutes > 0: parts.append(f"{minutes}m")
    # Show seconds only if total duration is less than a minute OR if H/M are zero but S > 0
    if seconds > 0 and total_seconds < 60 : parts.append(f"{seconds}s")
    elif seconds > 0 and hours == 0 and minutes == 0: parts.append(f"{seconds}s")

    return " ".join(parts) if parts else "0s" # Return "0s" if calculation ends up empty

def get_trending_tokens(window_hours: int) -> typing.List[typing.Tuple[str, int]]:
    """Calculates trending tokens based on votes in the last window_hours."""
    now = datetime.now(timezone.utc); cutoff_time = now - timedelta(hours=window_hours)
    recent_votes_by_token = defaultdict(int); malformed_vote_count = 0; outdated_vote_count = 0
    # Iterate over a copy of the list items for safety during potential modifications (though not modifying here)
    current_votes_log = list(votes_log)
    logger.debug(f"Calculating trending tokens from {len(current_votes_log)} log entries, window: {window_hours}h")

    for vote in current_votes_log:
        try:
            # Basic structure check
            if not isinstance(vote, dict) or 'timestamp' not in vote or 'contract_chain' not in vote:
                malformed_vote_count += 1; continue

            timestamp_str = vote.get('timestamp') # Use get for safety
            token_key = vote.get('contract_chain')

            if not timestamp_str or not token_key: # Check for empty values
                 malformed_vote_count += 1; continue

            vote_time = datetime.fromisoformat(timestamp_str)

            # Ensure timezone aware comparison
            if vote_time.tzinfo is None:
                 vote_time = vote_time.replace(tzinfo=timezone.utc) # Assume UTC if naive

            if vote_time >= cutoff_time:
                 recent_votes_by_token[token_key] += 1
            else:
                 outdated_vote_count += 1 # Count votes older than the window

        except (ValueError, TypeError) as e:
             logger.warning(f"Malformed vote entry encountered during trending calc: {vote}, Error: {e}")
             malformed_vote_count += 1; continue
        except Exception as e:
             logger.exception(f"Unexpected error processing vote entry during trending calc: {vote}, Error: {e}")
             malformed_vote_count += 1; continue

    if malformed_vote_count > 0: logger.warning(f"Found {malformed_vote_count} malformed entries in votes_log during trending calculation.")
    logger.debug(f"Trending calculation: {len(recent_votes_by_token)} tokens within window, {outdated_vote_count} outdated votes.")

    # Sort tokens by vote count in descending order
    return sorted(recent_votes_by_token.items(), key=lambda item: item[1], reverse=True)

async def get_token_display_info(token_key: str, include_symbol=True) -> str:
    """Gets a display string like 'Token Name ($SYMBOL)' or falls back."""
    if not token_key or not isinstance(token_key, str):
         logger.warning(f"Invalid token_key type or empty passed to get_token_display_info: {token_key}"); return "Unknown Token"

    info = token_cache.get(token_key, {}); name = info.get('name'); symbol = info.get('symbol')
    display_parts = []
    if name: display_parts.append(name)
    # Use parentheses for symbol for better readability
    if symbol and include_symbol: display_parts.append(f"(${symbol})")

    # Return combined name and symbol if available
    if display_parts: return " ".join(display_parts)

    # Fallback: Try parsing from the key itself if name/symbol missing
    logger.debug(f"Name/Symbol not found in cache for {token_key}, parsing key for display.")
    try:
        parts = token_key.split('_');
        if len(parts) >= 2:
            contract = parts[0]; chain = parts[-1]; # Use last part as chain
            # Shorten contract address if long
            contract_display = f"{contract[:6]}...{contract[-4:]}" if len(contract) > 10 else contract
            return f"{contract_display} ({chain.upper()})" # e.g., 0x123...abc (ETH)
        else:
            # If key doesn't conform to expected format, return key itself as fallback
            logger.warning(f"Could not parse token key '{token_key}' into contract/chain parts.")
            return token_key
    except Exception as e:
         logger.warning(f"Error parsing token key '{token_key}' for display fallback: {e}");
         return token_key # Return key as final fallback on error

async def fetch_token_data_dexscreener(session: aiohttp.ClientSession, contract_address: str) -> typing.Optional[typing.Dict]:
    """Fetches token data from DexScreener API."""
    if not contract_address:
        logger.warning("fetch_token_data_dexscreener called with empty contract_address.")
        return None
    # Ensure URL uses https
    url = config.DEXSCREENER_API_URL.format(contract_address=contract_address).replace("http://", "https://")
    # Use bot version in User-Agent if available
    bot_version = getattr(config, 'BOT_VERSION', '1.0')
    headers = {'User-Agent': f'CryptoPulseBot/{bot_version}'}
    logger.debug(f"Fetching DexScreener data for {contract_address} from {url}")
    try:
        async with session.get(url, headers=headers, timeout=15) as response:
            response_text = await response.text() # Read response text once for logging/parsing
            logger.debug(f"DexScreener response status for {contract_address}: {response.status}")

            if response.status == 200:
                try:
                    data = json.loads(response_text) # Use json.loads after reading text
                except json.JSONDecodeError as e:
                     logger.error(f"JSON Decode Error DexScreener for {contract_address}: {e}. Response (start): {response_text[:500]}")
                     return None

                if data and data.get("pairs") and isinstance(data["pairs"], list) and len(data["pairs"]) > 0:
                    # Simplification: Use the first pair returned.
                    pair_data = data["pairs"][0]
                    logger.debug(f"Using first pair data for {contract_address}: {pair_data.get('pairAddress')}")

                    # Extract data safely using .get() with defaults
                    price_usd_str = pair_data.get("priceUsd")
                    price_native_str = pair_data.get("priceNative")
                    market_cap_str = pair_data.get("fdv") # Using Fully Diluted Valuation as market cap
                    liquidity_usd_str = pair_data.get("liquidity", {}).get("usd")
                    price_change_h24_str = pair_data.get("priceChange", {}).get("h24")
                    volume_h24_str = pair_data.get("volume", {}).get("h24")

                    # Convert to float, handling potential errors or None values
                    try:
                        price_usd = float(price_usd_str) if price_usd_str is not None else 0.0
                        price_native = float(price_native_str) if price_native_str is not None else 0.0
                        market_cap_usd = float(market_cap_str) if market_cap_str is not None else 0.0
                        liquidity_usd = float(liquidity_usd_str) if liquidity_usd_str is not None else 0.0
                        price_change_h24 = float(price_change_h24_str) if price_change_h24_str is not None else 0.0
                        volume_h24 = float(volume_h24_str) if volume_h24_str is not None else 0.0
                    except (ValueError, TypeError) as e:
                         logger.error(f"Error converting DexScreener numeric data for {contract_address}: {e}. Raw Data: Price={price_usd_str}, MC={market_cap_str}, Liq={liquidity_usd_str}, Change={price_change_h24_str}, Vol={volume_h24_str}")
                         return None # Treat conversion errors as critical failure for this fetch

                    token_info = {
                        "price_usd": price_usd,
                        "price_native": price_native,
                        "market_cap_usd": market_cap_usd,
                        "liquidity_usd": liquidity_usd,
                        "price_change_h24": price_change_h24,
                        "volume_h24": volume_h24,
                        "dex_id": pair_data.get("dexId"),
                        "pair_address": pair_data.get("pairAddress"),
                        "url": pair_data.get("url"), # DexScreener URL for the pair
                        "holders": None, # Holders count not typically available here
                        "fetch_timestamp": datetime.now(timezone.utc).isoformat() # Add timestamp of fetch
                    }

                    # Basic sanity check: Market cap > 0 is usually required for useful data
                    if token_info["market_cap_usd"] > 0:
                         logger.debug(f"Successfully processed DexScreener data for {contract_address}")
                         return token_info
                    else:
                         logger.warning(f"DexScreener {contract_address}: Market Cap is zero or negative ({token_info['market_cap_usd']}). Pair Addr: {token_info.get('pair_address')}")
                         # Return data even with 0 MCAP, downstream logic handles it.
                         return token_info
                else:
                    logger.warning(f"No valid pairs found in DexScreener response for {contract_address}. Response (start): {response_text[:500]}")
                    return None # No pairs found or invalid structure
            elif response.status == 404:
                 logger.warning(f"Token {contract_address} not found on DexScreener (404).")
                 return None
            elif response.status == 429:
                 logger.error(f"DexScreener API rate limit hit (429) for {contract_address}!")
                 raise ConnectionError("DexScreener Rate Limit Hit") # Raise specific error
            else:
                 logger.error(f"Error fetching from DexScreener for {contract_address}. Status: {response.status}, Response: {response_text[:500]}")
                 return None
    except aiohttp.ClientConnectorError as e:
        logger.error(f"Network connection error (ClientConnectorError) fetching DexScreener for {contract_address}: {e}")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout error fetching DexScreener for {contract_address}")
        return None
    except ConnectionError: # Re-raise the specific rate limit error if caught above
        raise
    except Exception as e:
        logger.exception(f"Unexpected error fetching DexScreener data for {contract_address}: {e}")
        return None

def get_minigame_cooldown_remaining(user_id: int) -> typing.Optional[timedelta]:
    """Calculates remaining global minigame cooldown time for a user. Cleans up expired."""
    user_id_str = str(user_id)
    if user_id_str in minigame_cooldowns:
        try:
            last_play_time_str = minigame_cooldowns[user_id_str];
            last_play_time = datetime.fromisoformat(last_play_time_str)
            if last_play_time.tzinfo is None: last_play_time = last_play_time.replace(tzinfo=timezone.utc) # Assume UTC
            cooldown_end = last_play_time + timedelta(hours=config.MINIGAME_COOLDOWN_HOURS);
            now = datetime.now(timezone.utc)
            if now < cooldown_end:
                return cooldown_end - now
            else:
                 # Cooldown expired, clean up
                 logger.debug(f"Minigame cooldown expired for user {user_id_str}. Cleaning up.")
                 try:
                     del minigame_cooldowns[user_id_str]
                 except KeyError: pass # Already gone
                 # Defer save
                 return None
        except ValueError:
             logger.error(f"Invalid minigame cooldown timestamp format for user {user_id_str}: '{minigame_cooldowns[user_id_str]}'. Removing entry.");
             try:
                 del minigame_cooldowns[user_id_str] # Clean up invalid entry
             except KeyError: pass
             return None
        except Exception as e:
             logger.exception(f"Error calculating minigame cooldown for user {user_id_str}: {e}")
             return None # Treat error as cooldown finished
    return None # No cooldown entry found

async def generate_scoreboard_text(include_market_data=False) -> str:
    """Generates the formatted text for the pinned scoreboard message."""
    logger.debug("Generating scoreboard text...")
    trending_tokens_by_votes = get_trending_tokens(config.TRENDING_WINDOW_HOURS)
    top_n = config.SCOREBOARD_TOP_N

    title = config.SCOREBOARD_TITLE or "🏆 Trending Tokens" # Default title
    lines = [f"<b>{title}</b> (Votes Last {config.TRENDING_WINDOW_HOURS}h)\n"]

    if not trending_tokens_by_votes:
        lines.append("<i>No tokens currently trending by votes.</i>")
    else:
        rank_counter = 0
        logger.debug(f"Processing {len(trending_tokens_by_votes)} trending tokens for scoreboard (limit {top_n}).")
        for token_key, vote_count in trending_tokens_by_votes:
             if rank_counter >= top_n: break # Stop after reaching top_n

             rank = rank_counter + 1
             emoji = ""
             if rank == 1: emoji = "🥇"
             elif rank == 2: emoji = "🥈"
             elif rank == 3: emoji = "🥉"
             else: emoji = f"{rank}." # Use rank number for others

             try:
                 token_display = await get_token_display_info(token_key)
             except Exception as e:
                 logger.error(f"Error getting display info for {token_key} in scoreboard: {e}")
                 token_display = f"Error ({token_key[:6]}...)"

             mcap_str = ""
             price_change_str = ""
             # Optionally include market data if flag is set and data available
             if include_market_data and token_key in token_cache:
                token_data = token_cache[token_key]
                # Format Market Cap
                mcap = token_data.get('market_cap_usd')
                if mcap is not None and mcap > 0:
                    mcap_str = f" | MC: ${mcap:,.0f}" # Format with commas

                # Format 24h Price Change
                price_change = token_data.get('price_change_h24')
                if price_change is not None:
                    change_emoji = "📈" if price_change >= 0 else "📉"
                    # Use + sign for positive changes
                    change_sign = "+" if price_change >= 0 else ""
                    price_change_str = f" {change_emoji}{change_sign}{price_change:.1f}%"

             # Combine parts for the line
             lines.append(f"{emoji} {token_display} - <b>{vote_count}</b> votes{mcap_str}{price_change_str}")
             rank_counter += 1

        if len(trending_tokens_by_votes) > top_n:
             lines.append(f"\n<i>...and {len(trending_tokens_by_votes) - top_n} more! Use /trending to see more.</i>")

    now_utc = datetime.now(timezone.utc)
    # Format timestamp clearly including Timezone
    timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
    lines.append(f"\n🕒 <i>Last updated: {timestamp}</i>")
    logger.debug("Scoreboard text generation complete.")
    return "\n".join(lines)

# --- FSM States --- (Keep as is)
class SetupStates(StatesGroup):
    waiting_for_action = State()
    waiting_for_contract = State()
    waiting_for_chain = State()
    waiting_for_name = State()
    waiting_for_symbol = State()
    waiting_for_group_link = State()
    configuring_notifications = State()
    waiting_for_threshold = State()
    waiting_for_media = State()

# --- Command Handlers --- (Keep as is, relies on fixed helpers)
@router.message(CommandStart())
async def handle_start(message: Message, state: FSMContext):
    """Handles the /start command, including deep links for setup."""
    await state.clear(); # Clear any previous state for this user
    user_id = message.from_user.id;
    args = message.text.split() if message.text else []
    logger.info(f"Received /start command from user {user_id}. Args: {args}")

    # --- Deep Link for Setup ---
    if len(args) > 1 and args[1].startswith("setup_"):
        logger.info(f"Processing setup deep link: {args[1]}")
        try:
            # Extract group ID from the deep link argument
            group_id_str = args[1].split("_", 1)[1]; # Use split with maxsplit=1
            group_id = int(group_id_str)
            logger.info(f"Attempting setup via deep link for group {group_id} from user {user_id}")

            # Verify user is admin in the target group
            if not await is_user_admin(group_id, user_id):
                group_title = f"Group ID {group_id}"; # Fallback title
                try:
                    chat = await bot.get_chat(group_id);
                    if chat.title: group_title = chat.title
                except Exception as e: logger.warning(f"Could not get chat title for {group_id} during setup start check: {e}")
                await message.answer(f"❌ Sorry, you must be an administrator in the group '{group_title}' (ID: {group_id}) to initiate the setup process using this link.");
                logger.warning(f"User {user_id} tried setup deep link for group {group_id} but is not admin.")
                return

            # Start FSM for setup
            await state.set_state(SetupStates.waiting_for_action)
            # Pre-populate state data with group ID and defaults
            await state.update_data(
                group_id=group_id,
                token_contract=None, chain=None, token_name=None, token_symbol=None, group_link=None,
                notifications_enabled=True, # Default to on
                notification_threshold=1    # Default to 1
            )
            logger.info(f"User {user_id} is admin of {group_id}, starting setup FSM.")
            await message.answer(f"🚀 Welcome, Admin for Group ID {group_id}!\nLet's configure the bot for your token.", reply_markup=await create_setup_menu(state));

        except (IndexError, ValueError) as e:
            logger.warning(f"Invalid setup deep link format received: {args[1]}. Error: {e}")
            await message.answer("❌ Invalid setup link format. Please ensure the link was copied correctly.")
        except Exception as e:
            logger.exception(f"Error processing setup deep link for user {user_id}, argument {args[1]}: {e}")
            await message.answer("An unexpected error occurred while processing the setup link.")
        return # Stop further processing if it was a setup link

    # --- Standard /start message ---
    logger.info(f"Sending standard welcome message to user {user_id}")
    bot_info = await bot.get_me();
    keyboard = InlineKeyboardBuilder()

    async def add_join_button_start(chat_id_config: typing.Union[int, str], text: str, chat_name_log: str):
        """Safely adds a join button if chat_id is valid and link can be created."""
        url = None;
        if not chat_id_config:
             logger.debug(f"Join button for '{text}' skipped: Chat ID not configured.")
             return
        try:
            chat_id_for_link = None # Use numeric ID or username for links
            invite_link_chat_id = None # Use numeric ID for invite link creation

            if isinstance(chat_id_config, str) and chat_id_config.startswith("@"):
                url = f"https://t.me/{chat_id_config.lstrip('@')}" # Direct link for public username/channel
                chat_id_for_link = chat_id_config
            elif isinstance(chat_id_config, int):
                chat_id_for_link = chat_id_config
                invite_link_chat_id = chat_id_config
            elif isinstance(chat_id_config, str) and chat_id_config.lstrip('-').isdigit():
                 numeric_id = int(chat_id_config)
                 chat_id_for_link = numeric_id
                 invite_link_chat_id = numeric_id
            else:
                 logger.error(f"Invalid chat ID format in config for '{text}': {chat_id_config}")
                 return

            # Try creating invite link if it's a group/private channel ID
            if invite_link_chat_id and not url:
                 logger.debug(f"Attempting to create invite link for {chat_name_log} ({invite_link_chat_id})")
                 try:
                     # Create a short-lived, single-use invite link
                     link_obj = await bot.create_chat_invite_link(invite_link_chat_id, member_limit=1)
                     url = link_obj.invite_link
                     logger.debug(f"Created invite link for {chat_name_log}: {url}")
                 except Exception as e:
                     logger.warning(f"Could not create invite link for {chat_name_log} ({invite_link_chat_id}) in /start: {type(e).__name__} - {e}. Using fallback link.")
                     # Fallback - generic link to bot, user needs to find group manually if invite fails
                     url = f"https://t.me/{(await bot.get_me()).username}?start=request_join_{chat_id_for_link}"

            if url:
                 keyboard.button(text=text, url=url)
                 logger.debug(f"Added join button: {text} -> {url}")
            else: logger.warning(f"Could not generate a valid URL for the join button: {text} ({chat_id_for_link})")
        except Exception as e: logger.exception(f"Error adding join button for {chat_id_config}: {e}")

    # Add buttons for main channel and group from config
    await add_join_button_start(config.MAIN_CHANNEL_ID, "➡️ Join Main Channel", "Main Channel")
    await add_join_button_start(config.MAIN_GROUP_ID, "➡️ Join Main Group", "Main Group");
    keyboard.adjust(1) # Arrange buttons in single column

    # Send welcome message
    await message.answer(
        f"👋 Welcome to <b>{bot_info.full_name}</b>!\n\n"
        f"I help crypto communities track votes and token performance.\n\n"
        f"<b>Key Commands:</b>\n"
        f"🔹 /vote - Use this <i>inside a configured group</i> to vote for its token.\n"
        f"🔹 /trending - See the top tokens globally based on recent votes.\n"
        f"🔹 /mycooldowns - Check when you can vote again (regular votes).\n"
        f"🔹 /myfreevotes - See your balance of free votes won from minigames.\n"
        f"🔹 /bowl or /darts - Play minigames <i>in a group</i> for a chance to win free votes! (Cooldown: {config.MINIGAME_COOLDOWN_HOURS}h)\n"
        f"🔹 /help - Show detailed command list.\n\n"
        f"🛠️ <b>For Group Admins:</b>\n"
        f"1. Add me to your token's group.\n"
        f"2. Grant me Admin rights (Send Messages, Pin Messages are useful!).\n"
        f"3. Type /setup in your group to start the configuration via DM.\n\n"
        f"🚨 <b>Requirement for Voting:</b> You usually need to be a member of our main channel and group to cast votes. Please join using the links below!",
        reply_markup=keyboard.as_markup() if keyboard.buttons else None, # Only add markup if buttons exist
        disable_web_page_preview=True
    )

@router.message(Command("help"))
async def handle_help(message: Message):
    """Displays the help message with all commands."""
    # Get bot info for dynamic command examples if needed
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    bot_version_str = f"v{config.BOT_VERSION}" if hasattr(config, 'BOT_VERSION') else 'N/A'

    help_text = (
        f"<b>🤖 {bot_info.full_name} Help ({bot_version_str})</b>\n\n"
        f"Here's a list of available commands:\n\n"

        f"--- <b>👤 User Commands</b> ---\n"
        f"▪️ /start - Shows the welcome message & main community links.\n"
        f"▪️ /vote - Use <i>inside a configured group</i> to cast your vote. Uses a free vote first if available.\n"
        f"▪️ /trending - Displays the top tokens by votes (last {config.TRENDING_WINDOW_HOURS}h).\n"
        f"▪️ /mycooldowns - Check your remaining regular vote cooldowns per token.\n"
        f"▪️ /myfreevotes - Check your balance of free votes earned from minigames.\n"
        f"▪️ /bowl or /darts - Play minigames <i>in a configured group</i> to win free votes! (Cooldown: {config.MINIGAME_COOLDOWN_HOURS}h).\n"
        f"▪️ /help - Shows this help message.\n\n"

        f"--- <b>🛠️ Group Admin Commands</b> (Use in your group) ---\n"
        f"▪️ /setup - Starts the interactive bot configuration via Direct Message (DM).\n"
        f"▪️ /viewconfig - Shows the current bot settings for this group.\n"
        f"▪️ /pinscoreboard - Pins a live-updating scoreboard message (Bot needs 'Pin Messages' permission).\n"
        f"▪️ /resettoken - Removes the bot's config for this group & attempts to unpin the scoreboard.\n"
        f"▪️ /togglenotifications - Turns vote notification messages in this group ON or OFF.\n"
        f"▪️ /setvotethreshold <code>[number]</code> - Sets how many votes trigger a notification (e.g., <code>/setvotethreshold 5</code>).\n\n"

        f"🚨 <b>Voting Requirement:</b> You usually need to join the main project channel & group (links via /start) to vote.\n\n"
        f"<i>If you encounter issues, please contact the bot administrators or the main project group.</i>"
    )
    await message.answer(help_text, disable_web_page_preview=True)


@router.message(Command("setup"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_setup_command_in_group(message: Message):
    """Handles the /setup command issued within a group chat."""
    user_id = message.from_user.id; chat_id = message.chat.id;
    user_display = f"@{message.from_user.username}" if message.from_user.username else f"User ID {user_id}"
    logger.info(f"Received /setup command in group {chat_id} from user {user_id} ({user_display})")

    # 1. Check if the user invoking the command is an admin
    if not await is_user_admin(chat_id, user_id): # Uses fixed helper
        logger.warning(f"User {user_id} tried /setup in group {chat_id} but is not admin.")
        try: await message.delete() # Delete the command message if possible
        except TelegramAPIError: pass # Ignore if delete fails
        await message.answer(f"❌ {user_display}, only administrators of this group can initiate the bot setup.", delete_after=20); # Send temporary error message
        return

    # 2. Check bot status and basic recommend permissions
    try:
        me = await bot.get_me();
        logger.debug(f"Checking bot's ({me.id}) status in chat {chat_id}")
        bot_member = await bot.get_chat_member(chat_id, me.id)
        logger.debug(f"Bot status in chat {chat_id}: {bot_member.status}")

        # Check if bot is admin and warn if not (for full features)
        if bot_member.status != ChatMemberStatus.ADMINISTRATOR: # Use Enum
             logger.warning(f"Bot is not full admin in group {chat_id} (status: {bot_member.status}). Setup initiated, but some features might fail later.")
             # Send a non-blocking warning
             await message.reply("⚠️ The bot isn't a full administrator in this group. Setup can proceed, but features like pinning the scoreboard might require granting full admin rights later.", disable_notification=True)
        else:
             # If bot IS admin, log its specific rights for debugging (optional)
             if isinstance(bot_member, ChatMemberAdministrator):
                 admin_rights_summary = {k: v for k, v in bot_member.model_dump().items() if k.startswith('can_') and v is True}
                 logger.debug(f"Bot admin rights in {chat_id}: {admin_rights_summary}")

    except TelegramForbiddenError as e:
         logger.error(f"Bot is likely forbidden (kicked or no rights) in group {chat_id}: {e}")
         await message.reply("❌ Cannot start setup. The bot might have been kicked or lacks basic permissions to operate in this group.");
         return
    except TelegramAPIError as e:
        logger.error(f"Could not verify bot's own status/permissions in chat {chat_id}: {e}");
        await message.reply("⚠️ Couldn't verify the bot's status in this group. Please ensure it's added correctly as a member (admin recommended).");
        return
    except Exception as e:
         logger.exception(f"Unexpected error checking bot status in {chat_id}: {e}")
         await message.reply("An unexpected error occurred while checking bot status.")
         return

    # 3. Send the deep link button to start setup in DM
    bot_username = (await bot.get_me()).username;
    # Ensure chat_id is passed correctly in the start parameter
    setup_link = f"https://t.me/{bot_username}?start=setup_{chat_id}";
    logger.info(f"Sending setup DM link to user {user_id} for group {chat_id}: {setup_link}")
    keyboard = InlineKeyboardBuilder().button(text="➡️ Configure Bot in Private Message", url=setup_link)
    try:
        reply_msg = await message.reply(
            f"Hi {user_display}! Click the button below to configure the bot's settings for this group in a private chat with me.",
            reply_markup=keyboard.as_markup()
        )
        logger.info(f"Setup prompt sent successfully to group {chat_id}, reply message ID: {reply_msg.message_id}")

        # 4. Clean up messages after a delay (optional) - Run as background task
        cleanup_delay = 90 # seconds
        logger.debug(f"Scheduling cleanup of setup messages in {chat_id} after {cleanup_delay}s")
        async def cleanup_messages():
             await asyncio.sleep(cleanup_delay)
             try:
                 await message.delete() # Delete original /setup command
                 logger.debug(f"Deleted original /setup command message {message.message_id} in {chat_id}")
             except TelegramAPIError as e: logger.debug(f"Could not delete setup cmd {message.message_id} in {chat_id}: {e}")
             try:
                 await reply_msg.delete() # Delete the bot's reply with the button
                 logger.debug(f"Deleted setup prompt reply message {reply_msg.message_id} in {chat_id}")
             except TelegramAPIError as e: logger.debug(f"Could not delete setup prompt {reply_msg.message_id} in {chat_id}: {e}")
        asyncio.create_task(cleanup_messages(), name=f"CleanupSetupMsg_{chat_id}")

    except TelegramForbiddenError as e:
         logger.error(f"Failed to send setup prompt reply to group {chat_id} - Bot forbidden: {e}")
         # Try sending error message directly to user if reply fails?
         try: await bot.send_message(user_id, "❌ I couldn't send the setup link to the group. Please check if I have permission to send messages there.")
         except Exception: pass
    except Exception as e:
        logger.exception(f"Error sending setup prompt reply in chat {chat_id}: {e}");
        # Inform admin that something went wrong
        await message.answer("An error occurred while trying to start the setup process. Please try again or check bot logs.")


# --- Setup FSM Handlers (DM Only) --- (Assume these are correct from previous version)
async def create_setup_menu(state: FSMContext) -> InlineKeyboardMarkup:
    """Generates the dynamic inline keyboard for the setup menu."""
    data = await state.get_data(); builder = InlineKeyboardBuilder()
    logger.debug(f"Generating setup menu. Current state data: {data}")

    def format_button_text(label, value, required=False, default="Not Set", is_media=False):
        """Formats button text showing current value or requirement status."""
        status_text = default
        if is_media:
            status_text = "✅ Set" if value else "🖼️ Optional"
        elif value:
            # Truncate long values like contract addresses
            value_str = str(value)
            status_text = value_str[:10] + "..." + value_str[-6:] if len(value_str) > 20 else value_str
        elif required:
            status_text = "❗️Required"
        return f"{label} [{status_text}]"

    # Build buttons row by row
    builder.row(InlineKeyboardButton(text=format_button_text("1. Token Contract", data.get('token_contract'), required=True), callback_data="setup_set_contract"))
    builder.row(InlineKeyboardButton(text=format_button_text("2. Blockchain", data.get('chain'), required=True), callback_data="setup_set_chain"))
    builder.row(InlineKeyboardButton(text=format_button_text("3. Token Name", data.get('token_name'), required=True), callback_data="setup_set_name"))
    builder.row(InlineKeyboardButton(text=format_button_text("4. Token Symbol", data.get('token_symbol'), required=True), callback_data="setup_set_symbol"))
    builder.row(InlineKeyboardButton(text=format_button_text("5. Group TG Link", data.get('group_link'), required=False, default="Optional"), callback_data="setup_set_group_link"))

    # Notification settings button
    notif_enabled = data.get('notifications_enabled', True)
    notif_threshold = data.get('notification_threshold', 1)
    notif_status_icon = '✅ On' if notif_enabled else '❌ Off'
    builder.row(InlineKeyboardButton(text=f"6. Vote Notifs [{notif_status_icon} | Threshold: {notif_threshold}]", callback_data="setup_configure_notifs"))

    # --- NEW: Media Button ---
    builder.row(InlineKeyboardButton(text=format_button_text("7. Token Media", data.get('media_file_id'), is_media=True), callback_data="setup_set_media"))

    # Check if minimum required fields are set to enable Test/Finish
    can_test_or_finish = all([data.get('token_contract'), data.get('chain'), data.get('token_name'), data.get('token_symbol')])
    logger.debug(f"Can test/finish setup: {can_test_or_finish}")

    test_button_text = "🧪 Test Vote Notif" if can_test_or_finish else "🧪 Test (Set Token First)";
    finish_button_text = "💾 Finish Setup" if can_test_or_finish else "💾 Finish (Set Token First)"

    # Test and Finish buttons
    builder.row(
        InlineKeyboardButton(text=test_button_text, callback_data="setup_test_notif"),
        InlineKeyboardButton(text=finish_button_text, callback_data="setup_finish")
    )

    # Cancel button
    builder.row(InlineKeyboardButton(text="❌ Cancel Setup", callback_data="setup_cancel"))

    return builder.as_markup()

@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_set_media")
async def prompt_for_media(callback: CallbackQuery, state: FSMContext):
    """Prompts user to send media."""
    logger.debug(f"User {callback.from_user.id} requested to set token media.")
    await callback.answer()
    await state.set_state(SetupStates.waiting_for_media)

    # Add Skip/Remove button
    builder = InlineKeyboardBuilder()
    builder.button(text="🗑️ Skip / Remove Media", callback_data="setup_skip_remove_media")
    builder.button(text="< Back to Menu", callback_data="setup_media_back_to_menu")
    builder.adjust(1)

    await callback.message.edit_text(
        "🖼️ Please send <b>one</b> image (JPG, PNG) or GIF to be used for this token's notifications.\n"
        "<i>(Keep file size reasonable, under 5MB recommended)</i>",
        reply_markup=builder.as_markup()
    )

@router.message(StateFilter(SetupStates.waiting_for_media), F.photo | F.animation | F.document)
async def process_media(message: Message, state: FSMContext):
    """Processes the media sent by the user."""
    file_id = None
    file_type = "Unknown"

    if message.photo:
        # Get the largest photo version
        file_id = message.photo[-1].file_id
        file_type = "Photo"
        logger.info(f"User {message.from_user.id} sent photo with file_id: {file_id}")
    elif message.animation:
        file_id = message.animation.file_id
        file_type = "Animation (GIF)"
        logger.info(f"User {message.from_user.id} sent animation with file_id: {file_id}")
    elif message.document:
        # Check mime type for allowed document types
        if message.document.mime_type in ["image/jpeg", "image/png", "image/gif"]:
            file_id = message.document.file_id
            file_type = f"Document ({message.document.mime_type})"
            logger.info(f"User {message.from_user.id} sent document ({file_type}) with file_id: {file_id}")
        else:
            logger.warning(f"User {message.from_user.id} sent unsupported document type: {message.document.mime_type}")
            await message.reply("⚠️ Unsupported file type. Please send a JPG, PNG, or GIF.")
            # Re-prompt (optional, or just let them try again)
            # await message.answer("🖼️ Reply again with the desired image or GIF:")
            return # Stay in state

    if file_id:
        await state.update_data(media_file_id=file_id)
        await state.set_state(SetupStates.waiting_for_action)
        # Clean up the prompt message if possible (we stored its ID when prompting)
        state_data = await state.get_data()
        prompt_msg_id = state_data.get("media_prompt_message_id") # Assuming we stored it
        if prompt_msg_id:
            try: await bot.delete_message(message.chat.id, prompt_msg_id)
            except: pass # Ignore errors deleting old prompt

        # Delete the user's media message
        try: await message.delete()
        except: pass

        # Confirm and show menu
        await bot.send_message( # Send new message instead of replying to deleted one
            chat_id=message.chat.id,
            text=f"✅ Token media ({file_type}) set successfully!",
            reply_markup=await create_setup_menu(state)
        )

    else:
        # Should not happen if filters work, but as a fallback
        await message.reply("⚠️ Could not process the media. Please try sending it again.")

# Handler for non-media messages in the waiting_for_media state
@router.message(StateFilter(SetupStates.waiting_for_media))
async def process_media_incorrect_type(message: Message, state: FSMContext):
    """Handles incorrect message types when expecting media."""
    await message.reply("⚠️ Please send an image (JPG, PNG) or a GIF, or use the 'Skip/Remove' button.")

@router.callback_query(StateFilter(SetupStates.waiting_for_media), F.data == "setup_skip_remove_media")
async def skip_remove_media(callback: CallbackQuery, state: FSMContext):
    """Handles skipping or removing the token media."""
    logger.info(f"User {callback.from_user.id} skipped/removed token media.")
    await callback.answer("Media skipped/removed.")
    await state.update_data(media_file_id=None) # Set to None
    await state.set_state(SetupStates.waiting_for_action)
    await callback.message.edit_text("✅ Token media skipped/removed.", reply_markup=await create_setup_menu(state))

@router.callback_query(StateFilter(SetupStates.waiting_for_media), F.data == "setup_media_back_to_menu")
async def media_back_to_menu(callback: CallbackQuery, state: FSMContext):
     """Handles the back button from media selection."""
     logger.debug("User clicked Back from media setup.")
     await callback.answer();
     await state.set_state(SetupStates.waiting_for_action);
     await callback.message.edit_text("⚙️ Setup Menu:", reply_markup=await create_setup_menu(state))


@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_set_contract")
async def prompt_for_contract(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SetupStates.waiting_for_contract)
    await callback.message.edit_text(
        "📝 Please reply with the token's <b>full Contract Address</b> (e.g., <code>0x...</code> or Solana address).",
        reply_markup=None # Remove keyboard while waiting for reply
    )
    await callback.answer()

@router.message(StateFilter(SetupStates.waiting_for_contract), F.text)
async def process_contract(message: Message, state: FSMContext):
    contract_address = message.text.strip()
    # Basic validation (length might vary significantly between chains)
    # Let's check it's not empty and has a reasonable length, e.g., > 20 chars for most chains
    # Add more specific checks per chain later if needed
    if not contract_address or len(contract_address) < 20:
        await message.reply("⚠️ That doesn't look like a valid contract address. Please paste the full address again.");
        # Re-prompt without editing the previous prompt message
        await message.answer("📝 Reply again with the Contract Address:")
        return # Stay in the waiting_for_contract state

    logger.info(f"User {message.from_user.id} provided contract: {contract_address}")
    await state.update_data(token_contract=contract_address)
    await state.set_state(SetupStates.waiting_for_action) # Go back to menu
    # Confirm and show menu again
    await message.answer(f"✅ Contract address set:\n<code>{contract_address}</code>", reply_markup=await create_setup_menu(state));
    try: await message.delete() # Delete the user's contract address reply for privacy/cleanliness
    except TelegramAPIError: pass

@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_set_chain")
async def prompt_for_chain(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    builder = InlineKeyboardBuilder()
    # Add common chains - Adjust as needed
    builder.button(text="SOL (Solana)", callback_data="setchain_sol");
    builder.button(text="ETH (Ethereum)", callback_data="setchain_eth");
    builder.button(text="BSC (BNB Chain)", callback_data="setchain_bsc");
    builder.button(text="Base", callback_data="setchain_base");
    builder.button(text="ARB (Arbitrum)", callback_data="setchain_arb");
    # Add other chains if required: POLYGON, AVAX, etc.
    # builder.button(text="Other", callback_data="setchain_other"); # Optional: handle manual input?
    builder.button(text="< Back to Menu", callback_data="setup_back_to_menu");
    builder.adjust(2, 2, 1) # Adjust layout: 2 buttons per row, last row has 1
    await state.set_state(SetupStates.waiting_for_chain)
    await callback.message.edit_text("🔗 Please select the blockchain where this token exists:", reply_markup=builder.as_markup())

@router.callback_query(StateFilter(SetupStates.waiting_for_chain), F.data.startswith("setchain_"))
async def process_chain_selection(callback: CallbackQuery, state: FSMContext):
    await callback.answer();
    try:
        chain_code = callback.data.split("_", 1)[1]
        # Check for back button first - Ensure correct callback data name
        if chain_code == "back": # This depends on the callback_data set for the back button
            # Assuming back button callback is 'setup_back_to_menu' as defined below
            logger.warning("Incorrect callback 'setchain_back' received, should be 'setup_back_to_menu'. Handling anyway.")
            await state.set_state(SetupStates.waiting_for_action)
            await callback.message.edit_text("⚙️ Setup Menu:", reply_markup=await create_setup_menu(state))
            return

        # If it's a valid chain selection
        chain = chain_code.lower() # Store as lowercase code
        logger.info(f"User {callback.from_user.id} selected chain: {chain.upper()}")
        await state.update_data(chain=chain)
        await state.set_state(SetupStates.waiting_for_action) # Back to main menu
        await callback.message.edit_text(f"✅ Blockchain set: <b>{chain.upper()}</b>", reply_markup=await create_setup_menu(state))

    except IndexError:
        logger.error(f"Error parsing chain selection callback data: {callback.data}")
        await callback.message.edit_text("❌ An error occurred processing your selection. Please try again.", reply_markup=await create_setup_menu(state));
        await state.set_state(SetupStates.waiting_for_action) # Go back to menu on error


@router.callback_query(StateFilter(SetupStates.waiting_for_chain), F.data == "setup_back_to_menu")
async def chain_back_to_menu(callback: CallbackQuery, state: FSMContext):
     """Handles the back button from chain selection."""
     logger.debug("User clicked Back from chain selection.")
     await callback.answer();
     await state.set_state(SetupStates.waiting_for_action);
     await callback.message.edit_text("⚙️ Setup Menu:", reply_markup=await create_setup_menu(state))


@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_set_name")
async def prompt_for_name(callback: CallbackQuery, state: FSMContext):
    await callback.answer();
    await state.set_state(SetupStates.waiting_for_name);
    await callback.message.edit_text("📝 Please reply with the <b>Token Name</b> (e.g., My Awesome Token):", reply_markup=None)

@router.message(StateFilter(SetupStates.waiting_for_name), F.text)
async def process_name(message: Message, state: FSMContext):
    name = message.text.strip();
    # Basic validation
    if not name or len(name) > 64:
        await message.reply("⚠️ Invalid name. Please provide a name between 1 and 64 characters.");
        await message.answer("📝 Reply again with the Token Name:")
        return # Stay in state

    logger.info(f"User {message.from_user.id} provided token name: {name}")
    await state.update_data(token_name=name);
    await state.set_state(SetupStates.waiting_for_action)
    await message.answer(f"✅ Token name set: <b>{name}</b>", reply_markup=await create_setup_menu(state));
    try: await message.delete() # Delete user's reply
    except TelegramAPIError: pass


@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_set_symbol")
async def prompt_for_symbol(callback: CallbackQuery, state: FSMContext):
    await callback.answer();
    await state.set_state(SetupStates.waiting_for_symbol);
    await callback.message.edit_text("📝 Please reply with the <b>Token Symbol</b> (e.g., MAT, PEPE):", reply_markup=None)

@router.message(StateFilter(SetupStates.waiting_for_symbol), F.text)
async def process_symbol(message: Message, state: FSMContext):
    symbol = message.text.strip().upper(); # Convert to uppercase
    # Validation: alphanumeric, reasonable length
    if not symbol or not symbol.isalnum() or not (2 <= len(symbol) <= 10):
        await message.reply("⚠️ Invalid symbol. Please use 2-10 letters and numbers (e.g., ETH, SOL, PEPE1).");
        await message.answer("📝 Reply again with the Token Symbol:")
        return # Stay in state

    logger.info(f"User {message.from_user.id} provided token symbol: {symbol}")
    await state.update_data(token_symbol=symbol);
    await state.set_state(SetupStates.waiting_for_action)
    await message.answer(f"✅ Token symbol set: <b>${symbol}</b>", reply_markup=await create_setup_menu(state));
    try: await message.delete() # Delete user's reply
    except TelegramAPIError: pass


@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_set_group_link")
async def prompt_for_group_link(callback: CallbackQuery, state: FSMContext):
    await callback.answer();
    await state.set_state(SetupStates.waiting_for_group_link);
    await callback.message.edit_text(
        "🔗 Please reply with the <b>full Telegram link</b> to the token's main group chat (e.g., <code>https://t.me/mytokengroup</code>).\n"
        "This link might be shown in some bot messages.\n\n"
        "Reply with 'SKIP' if you don't want to set one.",
        reply_markup=None,
        disable_web_page_preview=True
    )

@router.message(StateFilter(SetupStates.waiting_for_group_link), F.text)
async def process_group_link(message: Message, state: FSMContext):
    link_text = message.text.strip()

    if link_text.upper() == 'SKIP':
        logger.info(f"User {message.from_user.id} skipped setting group link.")
        await state.update_data(group_link=None); # Explicitly set to None
        await state.set_state(SetupStates.waiting_for_action)
        await message.answer("✅ Group link skipped.", reply_markup=await create_setup_menu(state));
        try: await message.delete()
        except TelegramAPIError: pass
        return # Exit function

    # Validate link format (basic check)
    if not link_text.startswith(("https://t.me/", "http://t.me/")):
        await message.reply("⚠️ Invalid format. Please provide the full Telegram link starting with <code>https://t.me/</code> or reply 'SKIP'.");
        await message.answer("🔗 Reply again with the Group Link or 'SKIP':")
        return # Stay in state

    # Link format looks okay
    logger.info(f"User {message.from_user.id} provided group link: {link_text}")
    await state.update_data(group_link=link_text);
    await state.set_state(SetupStates.waiting_for_action)
    await message.answer(f"✅ Group Link set:\n<code>{link_text}</code>", reply_markup=await create_setup_menu(state), disable_web_page_preview=True);
    try: await message.delete() # Delete user's reply
    except TelegramAPIError: pass


@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_configure_notifs")
async def configure_notifications(callback: CallbackQuery, state: FSMContext):
    """Handles entry into the notification configuration sub-menu."""
    logger.debug(f"User {callback.from_user.id} entered notification config menu.")
    await callback.answer();
    data = await state.get_data();
    builder = InlineKeyboardBuilder()

    current_status = data.get('notifications_enabled', True); # Default true
    toggle_text = "❌ Turn Off Notifications" if current_status else "✅ Turn On Notifications"
    builder.button(text=toggle_text, callback_data="notif_toggle");

    threshold = data.get('notification_threshold', 1) # Default 1
    builder.button(text=f"📊 Set Threshold (Current: {threshold})", callback_data="notif_set_threshold");

    builder.button(text="< Back to Main Menu", callback_data="setup_back_to_menu");
    builder.adjust(1) # One button per row

    await state.set_state(SetupStates.configuring_notifications)

    # Edit the existing menu message
    try:
        await callback.message.edit_text(
            "⚙️ Configure vote notification settings for the group:",
            reply_markup=builder.as_markup()
        )
    except TelegramAPIError as e:
         # Handle cases where the original message might be gone or uneditable
         logger.warning(f"Error editing message in configure_notifications: {e}. Sending new message instead.")
         user_id = callback.from_user.id
         try:
             # Try sending a new message if editing fails
             await bot.send_message(user_id, "⚙️ Configure vote notification settings:", reply_markup=builder.as_markup())
         except Exception as send_e:
             logger.error(f"Failed to send new notification config menu to user {user_id} after edit failed: {send_e}")


@router.callback_query(StateFilter(SetupStates.configuring_notifications), F.data == "notif_toggle")
async def toggle_notifications(callback: CallbackQuery, state: FSMContext):
    """Toggles the notification enabled status."""
    data = await state.get_data();
    current_status = data.get('notifications_enabled', True);
    new_status = not current_status
    await state.update_data(notifications_enabled=new_status);

    status_text = "ON" if new_status else "OFF";
    logger.info(f"User {callback.from_user.id} toggled notifications to {status_text}")
    await callback.answer(f"Notifications turned {status_text}")

    # Re-render the notification config menu with updated status
    await configure_notifications(callback, state) # Call the function that edits the message


@router.callback_query(StateFilter(SetupStates.configuring_notifications), F.data == "notif_set_threshold")
async def prompt_for_threshold(callback: CallbackQuery, state: FSMContext):
    """Prompts the user to enter the notification threshold."""
    logger.debug(f"User {callback.from_user.id} requested to set notification threshold.")
    await callback.answer();
    await state.set_state(SetupStates.waiting_for_threshold)
    # Store the message ID of the menu so we can delete it later if needed
    await state.update_data(prompt_message_id=callback.message.message_id)
    await callback.message.edit_text(
        "📊 Enter the vote threshold for notifications.\n"
        "This determines how often vote notifications are posted in the group.\n"
        "For example:\n"
        "  • <code>1</code> notifies on every single vote.\n"
        "  • <code>5</code> notifies on the 1st vote, then the 5th, 10th, 15th, etc.\n\n"
        "<b>Please reply with a whole number (1 or greater):</b>",
        reply_markup=None # Remove keyboard while waiting for input
        )


@router.message(StateFilter(SetupStates.waiting_for_threshold), F.text)
async def process_threshold(message: Message, state: FSMContext):
    """Processes the user's threshold input."""
    state_data = await state.get_data();
    prompt_message_id = state_data.get("prompt_message_id") # Get original prompt ID

    try:
        threshold = int(message.text.strip());
        if threshold < 1:
            raise ValueError("Threshold must be 1 or greater.")

        logger.info(f"User {message.from_user.id} set notification threshold to {threshold}")
        await state.update_data(notification_threshold=threshold, prompt_message_id=None) # Clear prompt ID
        await state.set_state(SetupStates.configuring_notifications) # Go back to notif menu

        # Send confirmation message
        confirmation_msg = await message.answer(f"✅ Notification threshold set to: <b>{threshold}</b>.")

        # Try to delete the original prompt message ("Enter the vote threshold...")
        if prompt_message_id:
             try:
                 await bot.delete_message(chat_id=message.chat.id, message_id=prompt_message_id)
                 logger.debug(f"Deleted threshold prompt message {prompt_message_id}")
             except TelegramAPIError as e:
                 logger.warning(f"Could not delete threshold prompt message {prompt_message_id}: {e}")

        # Try to delete the user's reply (the number they sent)
        try:
            await message.delete()
            logger.debug(f"Deleted user threshold input message {message.message_id}")
        except TelegramAPIError as e:
            logger.warning(f"Could not delete user threshold input message {message.message_id}: {e}")

        # Re-render the notification config menu by sending a *new* message
        # It's generally better practice than editing the confirmation message.
        temp_state_data = await state.get_data() # Get updated state data
        builder = InlineKeyboardBuilder()
        current_status = temp_state_data.get('notifications_enabled', True)
        toggle_text = "❌ Turn Off Notifications" if current_status else "✅ Turn On Notifications"
        builder.button(text=toggle_text, callback_data="notif_toggle")
        updated_threshold = temp_state_data.get('notification_threshold', 1)
        builder.button(text=f"📊 Set Threshold (Current: {updated_threshold})", callback_data="notif_set_threshold")
        builder.button(text="< Back to Main Menu", callback_data="setup_back_to_menu")
        builder.adjust(1)
        await bot.send_message(message.chat.id, "⚙️ Configure vote notification settings:", reply_markup=builder.as_markup())
        logger.debug("Sent updated notification config menu.")

    except ValueError as e:
        logger.warning(f"Invalid threshold input from user {message.from_user.id}: '{message.text}' - Error: {e}")
        await message.reply("⚠️ Invalid input. Please enter a whole number that is 1 or greater.")
        # Re-prompt (don't edit, just send another message asking again)
        await message.answer("📊 Reply again with the notification threshold (e.g., 1, 5, 10):")
        # User stays in waiting_for_threshold state


@router.callback_query(StateFilter(SetupStates.configuring_notifications), F.data == "setup_back_to_menu")
async def notifs_back_to_menu(callback: CallbackQuery, state: FSMContext):
     """Handles returning from notification config to main setup menu."""
     logger.debug(f"User {callback.from_user.id} clicked Back from notification config.")
     await callback.answer();
     await state.set_state(SetupStates.waiting_for_action);
     await callback.message.edit_text("⚙️ Setup Menu:", reply_markup=await create_setup_menu(state))


@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_test_notif")
async def test_notification(callback: CallbackQuery, state: FSMContext):
    """Sends a test notification to the configured group."""
    data = await state.get_data();
    group_id = data.get('group_id');
    token_contract = data.get('token_contract');
    chain = data.get('chain');
    name = data.get('token_name');
    symbol = data.get('token_symbol')

    # Ensure required data is present before testing
    if not all([group_id, token_contract, chain, name, symbol]):
        logger.warning(f"User {callback.from_user.id} tried to test notification but required fields are missing.")
        await callback.answer("❗️ Please set Contract, Chain, Name, and Symbol first before testing!", show_alert=True);
        # Refresh menu to potentially show missing fields
        try:
            await callback.message.edit_reply_markup(reply_markup=await create_setup_menu(state));
        except TelegramAPIError as e:
             logger.warning(f"Failed to edit setup menu markup during test notification pre-check: {e}")
        return

    logger.info(f"User {callback.from_user.id} initiated test notification for group {group_id}")
    await callback.answer("⏳ Sending test notification to the group...");

    # Generate token key and temporary cache entry for display info
    token_key = get_token_key(token_contract, chain);
    # Use existing cache if available, otherwise create temp entry
    if token_key not in token_cache:
         logger.debug(f"Creating temporary cache entry for {token_key} for test notification display.")
         token_cache[token_key] = {'name': name, 'symbol': symbol} # Temp entry for display

    token_display = await get_token_display_info(token_key)

    test_caption = (
        f"🧪 <b>Test Vote Notification!</b> 🧪\n\n"
        f"This is a test message to verify the bot can send notifications for <b>{token_display}</b> in this group (ID: {group_id}).\n\n"
        f"If you see this, the basic sending mechanism seems okay!\n"
        f"(No actual vote was recorded. Settings look okay if you see this!)"
    );

    # Prepare vote button (using a special callback data for test)
    vote_button_builder = InlineKeyboardBuilder();
    # Test button callback no longer processed, but keep suffix for clarity
    vote_button_builder.button(text="🚀 Vote Now! (Test Button)", callback_data=f"vote_{token_key}_test");
    # Add promo link if configured
    if hasattr(config, 'PROMO_URL') and config.PROMO_URL:
        vote_button_builder.button(text="Check us out!", url=config.PROMO_URL);
    vote_button_builder.adjust(1 if not (hasattr(config, 'PROMO_URL') and config.PROMO_URL) else 2); # Adjust layout
    notif_markup = vote_button_builder.as_markup()

    # Select random image if configured
    selected_image_path = None
    if hasattr(config, 'VOTE_IMAGE_FILENAMES') and config.VOTE_IMAGE_FILENAMES and isinstance(config.VOTE_IMAGE_FILENAMES, list) and len(config.VOTE_IMAGE_FILENAMES) > 0:
        try:
            image_filename = random.choice(config.VOTE_IMAGE_FILENAMES);
            potential_path = os.path.join(config.IMAGES_DIR, image_filename);
            if os.path.isfile(potential_path):
                selected_image_path = potential_path;
                logger.debug(f"Selected image for test notification: {selected_image_path}")
            else: logger.warning(f"Test Notification: Configured image file not found at path: {potential_path}")
        except IndexError: # Handle empty list case
            logger.warning("Test Notification: VOTE_IMAGE_FILENAMES list is configured but empty.")
        except Exception as e:
            logger.error(f"Test Notification: Error selecting image: {e}")

    # --- Try sending the message/photo ---
    try:
        if selected_image_path:
            # --- Try sending photo ---
            logger.debug(f"Attempting to send test photo {selected_image_path} to group {group_id}")
            try:
                photo_input = FSInputFile(selected_image_path);
                await bot.send_photo(chat_id=group_id, photo=photo_input, caption=test_caption, reply_markup=notif_markup)
                logger.info(f"Test photo notification sent successfully to group {group_id}")
            except FileNotFoundError:
                 logger.error(f"Test Notification Error: Image file {selected_image_path} not found during send attempt. Sending text fallback.")
                 await bot.send_message(group_id, test_caption, reply_markup=notif_markup)
            except (TelegramForbiddenError, TelegramBadRequest) as tg_err:
                err_msg = str(tg_err).lower()
                if "have no rights to send photos" in err_msg or "not enough rights" in err_msg:
                     logger.error(f"Test Notification Error sending photo to group {group_id}: Bot lacks permission to send photos. Sending text fallback. Error: {tg_err}")
                     await bot.send_message(group_id, test_caption, reply_markup=notif_markup) # Fallback to text
                elif "chat not found" in err_msg:
                     logger.error(f"Test Notification Error sending photo to group {group_id}: Chat not found. Error: {tg_err}")
                     await callback.message.answer(f"❌ Error sending test photo: Chat ID {group_id} not found.")
                     return
                else:
                     # Other Telegram errors during photo send
                     logger.error(f"Test Notification Telegram error sending photo to group {group_id}: {tg_err}")
                     await callback.message.answer(f"❌ Error sending test photo to group: {tg_err.message}\nCheck bot permissions (Send Media).")
                     return # Stop if photo send fails critically
            except Exception as photo_e:
                 logger.exception(f"Unexpected error sending test photo to group {group_id}: {photo_e}")
                 # Attempt fallback to text on unexpected error during photo send
                 try:
                     await bot.send_message(group_id, test_caption, reply_markup=notif_markup)
                     logger.info("Sent text fallback after unexpected photo error.")
                 except Exception as fallback_e:
                      logger.error(f"Failed to send text fallback after photo error for group {group_id}: {fallback_e}")
                      await callback.message.answer("❌ An unexpected error occurred sending the test photo, and the text fallback also failed.")
                      return
        else:
            # --- Send text message ---
            logger.debug(f"Attempting to send test text notification to group {group_id}")
            await bot.send_message(group_id, test_caption, reply_markup=notif_markup)
            logger.info(f"Test text notification sent successfully to group {group_id}")

        # --- If send succeeded (photo or text) ---
        await callback.message.answer("✅ Test message sent to the group successfully!")

    # --- Catch broader errors related to sending to the group ---
    except (TelegramForbiddenError, TelegramBadRequest) as e:
         error_message = f"❌ Failed to send test message to Group ID {group_id}.\nError: {e.message}\n\n"
         err_msg_lower = str(e).lower()
         if "chat not found" in err_msg_lower: error_message += "Check if the Group ID is correct and the bot is still in the group."
         elif "bot was kicked" in err_msg_lower or "bot is not a member" in err_msg_lower: error_message += "The bot was kicked from the group or is not a member."
         elif "have no rights to send messages" in err_msg_lower: error_message += "The bot lacks permission to send messages in the group."
         else: error_message += "Please double-check the Group ID and the bot's permissions (like Send Messages) in that group."
         logger.error(f"Failed sending test message (text/fallback) to group {group_id}: {e}")
         await callback.message.answer(error_message, disable_web_page_preview=True)
    except (TelegramNetworkError, TelegramAPIError) as e: # Catch other network/API errors
        logger.error(f"Network/API Error sending test message to group {group_id}: {e}");
        error_message = f"❌ Failed to send test message due to a network or API error: {e.message}\nPlease try again later.";
        await callback.message.answer(error_message, disable_web_page_preview=True)
    except Exception as e:
        # Catch any other unexpected errors during the process
        logger.exception(f"Unexpected error sending test notification to group {group_id}");
        await callback.message.answer("❌ An unexpected error occurred while sending the test message.")


@router.callback_query(StateFilter(SetupStates.waiting_for_action), F.data == "setup_finish")
async def finish_setup(callback: CallbackQuery, state: FSMContext):
    """Finalizes the setup process, saves config, and clears state."""
    logger.info(f"User {callback.from_user.id} initiated finish setup.")
    data = await state.get_data();
    group_id = data.get('group_id');
    token_contract = data.get('token_contract');
    chain = data.get('chain');
    name = data.get('token_name');
    symbol = data.get('token_symbol');
    group_link = data.get('group_link'); # Optional
    media_file_id = data.get('media_file_id') # <<< GET MEDIA FILE ID
    notifications_enabled = data.get('notifications_enabled', True)
    notification_threshold = data.get('notification_threshold', 1)

    # --- Validate required fields ---
    required_fields = {'Group ID': group_id, 'Token Contract': token_contract, 'Blockchain': chain, 'Token Name': name, 'Token Symbol': symbol}
    missing_fields = [k for k, v in required_fields.items() if not v]
    if missing_fields:
        missing_str = ', '.join(missing_fields)
        logger.warning(f"User {callback.from_user.id} tried to finish setup but missing fields: {missing_str}")
        await callback.answer(f"❗️ Cannot finish: Required info missing ({missing_str}).", show_alert=True);
        try:
            await callback.message.edit_reply_markup(reply_markup=await create_setup_menu(state));
        except TelegramAPIError as e:
             logger.warning(f"Failed to edit setup menu markup during finish setup pre-check: {e}")
        return

    await callback.answer("💾 Saving configuration...", show_alert=False);
    logger.info(f"Finishing setup for group {group_id}. Data: {data}")

    # --- Prepare data for saving ---
    group_id_str = str(group_id);
    token_key = get_token_key(token_contract, chain)

    # Update token cache (create or update)
    if token_key not in token_cache: token_cache[token_key] = {}
    token_cache[token_key].update({
        'name': name,
        'symbol': symbol,
        'group_link': group_link,
        'media_file_id': media_file_id, # <<< SAVE MEDIA FILE ID
        'market_cap_usd': None, 'price_usd': None, 'first_mcap_usd': None,
        'first_mcap_timestamp': None, 'fetch_timestamp': None,
        'price_change_h24': None, 'volume_h24': None, 'url': None,
    })
    logger.debug(f"Updated token_cache for {token_key}: {token_cache[token_key]}")

    # Reset market data state
    if token_key in market_data_state:
        logger.debug(f"Resetting market_data_state for {token_key}")
        del market_data_state[token_key]

    # Prepare group config entry
    group_config_entry = {
        "token_key": token_key,
        "config": {
            "admin_user_id": callback.from_user.id,
            "notifications_enabled": notifications_enabled,
            "notification_threshold": notification_threshold,
            "group_link": group_link,
            "setup_complete": True,
            "is_active": True,
            "updated_at": datetime.now(timezone.utc).isoformat()
            # Media file ID is stored in token_cache now, not group config
        }
    }
    group_configs[group_id_str] = group_config_entry;
    logger.debug(f"Updated group_configs for {group_id_str}: {group_config_entry}")

    # --- Save data and clear state ---
    await save_data();
    await state.clear()
    logger.info(f"Configuration saved and FSM state cleared for user {callback.from_user.id}")

    # --- Confirmation Message ---
    token_display = await get_token_display_info(token_key);
    group_title = f"Group ID {group_id}"
    try:
        chat = await bot.get_chat(group_id);
        if chat.title: group_title = chat.title
    except Exception as e:
        logger.warning(f"Could not get chat title for {group_id} in finish_setup confirmation: {e}")

    await callback.message.edit_text(
        f"✅🎉 <b>Setup Complete!</b> 🎉✅\n\n"
        f"The bot is now configured for token <b>{token_display}</b> in the group '<b>{group_title}</b>'.\n"
        f"{'🖼️ Token media has been set.' if media_file_id else ''}\n" # Mention if media was set
        f"🔹 Users can now use /vote in the group.\n"
        f"🔹 Market data fetching will begin shortly.\n"
        f"🔹 Use /viewconfig in the group anytime to see the settings.\n"
        f"🔹 Consider using /pinscoreboard in the group.",
        reply_markup=None,
        disable_web_page_preview=True
    )

@router.callback_query(StateFilter("*"), F.data == "setup_cancel")
async def cancel_setup(callback: CallbackQuery, state: FSMContext):
    """Cancels the setup process at any stage."""
    logger.info(f"User {callback.from_user.id} cancelled setup. Current state: {await state.get_state()}")
    await callback.answer("Setup cancelled.", show_alert=False);
    current_state = await state.get_state()
    if current_state is None:
        # Already finished or no state, edit message minimally
        logger.debug("Setup cancel called but no active state found.")
        try: await callback.message.edit_text("Setup process ended.")
        except TelegramAPIError: pass # Ignore if message already gone
        return

    # Clear state and edit message
    await state.clear();
    try:
        await callback.message.edit_text("❌ Setup cancelled. No changes were saved.", reply_markup=None)
        logger.info(f"Setup cancelled and state cleared for user {callback.from_user.id}")
    except TelegramAPIError as e:
         logger.warning(f"Could not edit message on setup cancel: {e}") # Ignore if message already gone

# --- <<< MODIFIED VOTING FLOW >>> ---

async def process_vote_action(user_id: int, user_name: str, token_key: str) -> typing.Tuple[str, bool]:
    """
    Handles the core logic of processing a vote attempt.
    Checks membership, cooldowns, free votes, logs vote, saves data.
    Returns a tuple: (result_message: str, vote_succeeded: bool).
    Does NOT send messages directly to user/group anymore (except maybe group notifs).
    """
    user_id_str = str(user_id);
    safe_user_name = user_name.replace('<', '&lt;').replace('>', '&gt;');
    logger.info(f"Processing vote action logic for user {user_id} ({safe_user_name}), token {token_key}")
    try:
        token_display = await get_token_display_info(token_key)
    except Exception as e:
        logger.error(f"Failed to get token display info for {token_key} during vote logic: {e}")
        token_display = f"Token ({token_key[:6]}...)" # Fallback display

    # 1. Check Main Channel/Group Membership
    logger.debug(f"Checking main channel/group membership for user {user_id}")
    in_channel, in_group = await check_main_membership(user_id)
    if not in_channel or not in_group:
        missing_reqs = []
        if not in_channel: missing_reqs.append("main channel")
        if not in_group: missing_reqs.append("main group")
        reason = ' and '.join(missing_reqs)
        logger.warning(f"Vote logic failed for user {user_id}: Not member of {reason}.")
        # Return failure message and False status
        return (f"⚠️ Vote Failed! You must be a member of the {reason} to vote.", False) # Markup handled elsewhere now

    # 2. Check for Free Votes FIRST
    used_free_vote = False
    data_changed = False # Flag if save is needed
    logger.debug(f"Checking free votes for user {user_id_str}, token {token_key}")
    if user_id_str in user_free_votes and token_key in user_free_votes[user_id_str] and user_free_votes[user_id_str][token_key] > 0:
        user_free_votes[user_id_str][token_key] -= 1;
        remaining_free = user_free_votes[user_id_str][token_key];
        used_free_vote = True;
        data_changed = True
        logger.info(f"User {user_id} used a free vote for {token_key}. Remaining free for this token: {remaining_free}")
        # Clean up if count reaches zero
        if user_free_votes[user_id_str][token_key] <= 0:
             del user_free_votes[user_id_str][token_key];
             logger.debug(f"Removed free vote entry for token {token_key} for user {user_id_str} as count is zero.")
        # Clean up user entry if no more free votes for any token
        if not user_free_votes[user_id_str]:
             del user_free_votes[user_id_str];
             logger.debug(f"Removed user entry {user_id_str} from user_free_votes as it's empty.")
    else:
        # 3. Check Regular Vote Cooldown (only if no free vote was used)
        logger.debug(f"No free votes found/used for {user_id_str}/{token_key}. Checking regular cooldown.")
        cooldown = get_cooldown_remaining(user_id, token_key) # This helper now cleans expired entries
        if cooldown:
            # Cooldown active, prevent vote
            logger.info(f"Vote logic denied for user {user_id} on {token_key} due to active cooldown: {format_timedelta(cooldown)} remaining.")
            return (f"⏳ Cooldown active! You recently voted for {token_display}.\nTry again in <b>{format_timedelta(cooldown)}</b>.", False)
        else:
             # No free vote used and no cooldown active -> Proceed with regular vote
             logger.debug(f"No active cooldown for user {user_id}, token {token_key}. Proceeding with regular vote.")
             # If get_cooldown_remaining returned None, it might have cleaned up data, check if save needed
             # Check the main dict directly as helper modifies it
             if user_id_str in user_votes and token_key not in user_votes.get(user_id_str, {}):
                 data_changed = True # Cooldown was cleaned up

    # 4. Record the Vote (Regular or Free)
    now = datetime.now(timezone.utc); now_iso = now.isoformat()
    # Add regular vote cooldown timestamp *only* if it wasn't a free vote
    if not used_free_vote:
        if user_id_str not in user_votes: user_votes[user_id_str] = {};
        user_votes[user_id_str][token_key] = now_iso # Set cooldown timestamp
        logger.info(f"Recorded regular vote for user {user_id}, token {token_key}. Cooldown started.")
        data_changed = True
    else:
        logger.info(f"Recording free vote for user {user_id}, token {token_key}.")

    # Log the vote action
    log_entry = {
        "user_id": user_id,
        "user_name": user_name, # Store original username if available
        "contract_chain": token_key,
        "timestamp": now_iso,
        "is_free_vote": used_free_vote
    };
    votes_log.append(log_entry)
    data_changed = True # Vote log always changes
    logger.debug(f"Appended vote log entry: {log_entry}")

    # Save data changes IF any occurred (cooldown added/cleaned, free vote used, log updated)
    if data_changed:
        logger.debug("Data changed, saving vote action state.")
        await save_data()
    else:
         logger.debug("No data changes detected after vote processing.")

    # 5. Send Notification to Associated Group(s) if needed (Optional based on design)
    # DECISION: Keep group notifications for now, triggered by successful vote.
    await send_vote_notification_to_groups(user_id, safe_user_name, token_key, used_free_vote, token_display)

    # 6. Format and return success message
    success_message = f"✅ Vote cast successfully for <b>{token_display}</b>!\n\n"
    if used_free_vote:
        # Re-fetch remaining count for accuracy after decrement
        remaining_free = user_free_votes.get(user_id_str, {}).get(token_key, 0);
        success_message = f"✅ Free vote used for <b>{token_display}</b>!\n"
        success_message += f"You have {remaining_free} free vote(s) left for this token.\n\n"
        success_message += f"Your regular vote cooldown for {token_display} is unaffected."
        logger.info(f"Vote success message (free vote) generated for user {user_id}")
    else:
        # If regular vote, mention the cooldown period
        success_message += f"You can cast your next regular vote for this token in {config.VOTE_COOLDOWN_HOURS} hours."
        logger.info(f"Vote success message (regular vote) generated for user {user_id}")

    return (success_message, True) # Return success message and True status


async def send_vote_notification_to_groups(user_id: int, safe_user_name: str, token_key: str, used_free_vote: bool, token_display: str):
    """Sends the actual notification to configured groups after a successful vote. (Uses token media if set)"""
    associated_groups_configs = []
    logger.debug(f"Finding associated groups for notification for token {token_key}")
    for gid_str, g_data in group_configs.items():
        if g_data.get("token_key") == token_key and g_data.get("config", {}).get("setup_complete") and g_data.get("config", {}).get("is_active"):
             try:
                 group_id_int = int(gid_str)
                 group_conf = g_data.get("config")
                 if group_conf: associated_groups_configs.append((group_id_int, group_conf))
             except ValueError: logger.warning(f"Invalid group ID string {gid_str} in group_configs for notification.")

    if not associated_groups_configs:
        logger.debug(f"No active, configured groups found for token {token_key} to notify.")
        return

    logger.info(f"Need to potentially send notifications to {len(associated_groups_configs)} groups for vote on {token_key}")
    trending_data = get_trending_tokens(config.TRENDING_WINDOW_HOURS);
    votes_in_window = 0; rank_str = "N/A";
    for i, (tkey, vcount) in enumerate(trending_data):
        if tkey == token_key: votes_in_window = vcount; rank_str = f"#{i + 1}"; break
    logger.debug(f"Trending stats for {token_key} for notification: Votes={votes_in_window}, Rank={rank_str}")

    # Construct notification message
    notif_caption = (
        f"🚀 <a href='tg://user?id={user_id}'>{safe_user_name}</a> just voted for <b>{token_display}</b>!\n\n"
        f"📈 Votes (Last {config.TRENDING_WINDOW_HOURS}h): {votes_in_window}\n"
        f"🏆 Global Rank: {rank_str}\n\n"
        f"{'✨ Free vote used! ✨' if used_free_vote else 'Use /vote to cast your vote!'}" # Modified CTA slightly
    )

    # Prepare buttons (promo only)
    promo_button_builder = InlineKeyboardBuilder();
    promo_added = False
    if hasattr(config, 'PROMO_URL') and config.PROMO_URL:
        promo_button_builder.button(text="Check us out!", url=config.PROMO_URL);
        promo_added = True
    notif_markup = promo_button_builder.as_markup() if promo_added else None

    # --- Determine Media to Send ---
    media_to_send = None # Will be file_id or FSInputFile path
    media_is_file_id = False

    # 1. Check for Token-Specific Media (set during setup)
    token_specific_media_id = token_cache.get(token_key, {}).get('media_file_id')
    if token_specific_media_id:
        media_to_send = token_specific_media_id
        media_is_file_id = True
        logger.debug(f"Using token-specific media (file_id: {media_to_send}) for vote notification.")
    else:
        # 2. Fallback to Random Image from Config
        if hasattr(config, 'VOTE_IMAGE_FILENAMES') and config.VOTE_IMAGE_FILENAMES and isinstance(config.VOTE_IMAGE_FILENAMES, list) and len(config.VOTE_IMAGE_FILENAMES) > 0:
            try:
                image_filename = random.choice(config.VOTE_IMAGE_FILENAMES);
                potential_path = os.path.join(config.IMAGES_DIR, image_filename);
                if os.path.isfile(potential_path):
                    media_to_send = potential_path # Use path for FSInputFile
                    media_is_file_id = False
                    logger.debug(f"Using random fallback image ({media_to_send}) for vote notification.")
                else: logger.warning(f"Vote Notification: Configured random image file not found: {potential_path}")
            except IndexError: logger.warning("Vote Notification: VOTE_IMAGE_FILENAMES list is empty.")
            except Exception as e: logger.error(f"Vote Notification: Error selecting random image: {e}")

    # --- Send to Groups ---
    for group_id, group_conf in associated_groups_configs:
        logger.debug(f"Checking notification settings for group {group_id}")
        if group_conf.get("notifications_enabled", True):
            threshold = group_conf.get("notification_threshold", 1);
            should_send = (votes_in_window == 1) or (votes_in_window > 0 and votes_in_window % threshold == 0)
            logger.debug(f"Group {group_id}: Notifs Enabled=True, Threshold={threshold}, Votes={votes_in_window}, Should Send={should_send}")

            if should_send:
                logger.info(f"Attempting to send vote notification to group {group_id} (threshold met).")
                try:
                    if media_to_send:
                        photo_input = media_to_send if media_is_file_id else FSInputFile(media_to_send)
                        logger.debug(f"Sending photo/animation notification to {group_id} (Media: {media_to_send}, IsFileID: {media_is_file_id})")
                        try:
                            await bot.send_photo(chat_id=group_id, photo=photo_input, caption=notif_caption, reply_markup=notif_markup)
                            logger.info(f"Media notification sent successfully to group {group_id}")
                        except (TelegramForbiddenError, TelegramBadRequest) as tg_err:
                            err_msg = str(tg_err).lower()
                            if "kicked" in err_msg or "not a member" in err_msg or "chat not found" in err_msg or "bot was blocked" in err_msg:
                                logger.warning(f"Bot kicked/removed/chat deleted for group {group_id} during photo send. Disabling group config. Error: {tg_err}")
                                if str(group_id) in group_configs: group_configs[str(group_id)]["config"]["is_active"] = False; await save_data()
                            elif "rights to send photos" in err_msg or "wrong file identifier" in err_msg:
                                logger.warning(f"Bot lacks photo permission or invalid file_id '{media_to_send}' in group {group_id}. Sending text fallback. Error: {tg_err}")
                                await bot.send_message(group_id, notif_caption, reply_markup=notif_markup, disable_web_page_preview=True)
                            else: logger.warning(f"Failed sending vote photo to group {group_id}: {tg_err}")
                        except FileNotFoundError: # Only relevant if using FSInputFile
                             logger.error(f"Vote Notify Img path {media_to_send} missing when sending to {group_id}. Sending text fallback.")
                             await bot.send_message(group_id, notif_caption, reply_markup=notif_markup, disable_web_page_preview=True)
                        except Exception as e:
                            logger.exception(f"Unexpected error sending vote photo/animation to {group_id}: {e}")
                            try: await bot.send_message(group_id, notif_caption, reply_markup=notif_markup, disable_web_page_preview=True)
                            except Exception: pass # Ignore fallback error
                    else:
                        # Send text only if no media configured or found
                        logger.debug(f"Sending text notification to {group_id} (no media)")
                        await bot.send_message(group_id, notif_caption, reply_markup=notif_markup, disable_web_page_preview=True)
                        logger.info(f"Text notification sent successfully to group {group_id}")
                except Exception as e: # Catch broader errors during the send logic
                    logger.exception(f"Outer error sending vote notification to group {group_id}: {e}")
        else:
            logger.debug(f"Notifications are disabled for group {group_id}. Skipping.")


@router.message(Command("vote"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_vote_in_group(message: Message):
    """Handles the /vote command: Sends a button reply in the group."""
    group_id_str = str(message.chat.id);
    user = message.from_user;
    user_name = user.username if user.username else user.first_name;
    logger.info(f"Received /vote command in group {group_id_str} from user {user.id} ({user_name})")

    # Check if group is configured and active
    group_data = group_configs.get(group_id_str)
    if not group_data or not group_data.get("config", {}).get("setup_complete") or not group_data.get("config", {}).get("is_active"):
        logger.warning(f"Vote command in unconfigured/inactive group {group_id_str} by user {user.id}")
        await message.reply("This group isn't configured for voting yet, or the configuration is inactive."); return

    token_key = group_data.get("token_key");
    if not token_key:
        logger.error(f"Configuration error in group {group_id_str}: Missing token_key.");
        await message.reply("Configuration error. Please contact an admin."); return

    try:
        token_display = await get_token_display_info(token_key)
    except Exception as e:
        logger.error(f"Failed to get token display info for {token_key} in /vote command: {e}")
        token_display = "this token" # Fallback display

    # Create the button to initiate the DM confirmation
    builder = InlineKeyboardBuilder()
    # Use a unique prefix for this type of button
    builder.button(text=f"➡️ Click here to Vote for {token_display}", callback_data=f"vote_request_{token_key}")

    await message.reply(
        f"🗳️ Ready to vote for {hbold(token_display)}?\nClick the button below to confirm your vote in a private message.",
        reply_markup=builder.as_markup()
    )

@router.message(Command("vote"), F.chat.type == ChatType.PRIVATE)
async def handle_vote_in_dm(message: Message):
    """Informs users they cannot initiate vote in DM."""
    logger.info(f"User {message.from_user.id} tried to use /vote in DM.")
    await message.reply("Please use the /vote command inside the specific token's Telegram group first.")

@router.callback_query(F.data.startswith("vote_request_"))
async def handle_vote_request_button(callback: CallbackQuery):
    """Handles the button clicked in the group, sends DM confirmation."""
    user_id = callback.from_user.id
    logger.info(f"User {user_id} clicked vote request button: {callback.data}")
    try:
        token_key = callback.data.split("vote_request_", 1)[1]
        if not token_key: raise ValueError("Token key missing")
    except (IndexError, ValueError) as e:
        logger.error(f"Failed to parse token_key from vote_request callback: {callback.data}, Error: {e}")
        await callback.answer("Error: Could not identify the token from this button.", show_alert=True)
        return

    try:
        token_display = await get_token_display_info(token_key)
    except Exception as e:
        logger.error(f"Failed to get token display info for {token_key} in vote_request handler: {e}")
        token_display = "this token"

    # Create the confirmation button for DM
    builder = InlineKeyboardBuilder()
    # Use a different prefix for the confirmation button
    builder.button(text=f"✅ Confirm Vote for {token_display}", callback_data=f"vote_confirm_{token_key}")

    # Try sending the confirmation message to the user's DM
    try:
        await bot.send_message(
            chat_id=user_id,
            text=f"Please confirm your vote for <b>{token_display}</b> by clicking the button below:",
            reply_markup=builder.as_markup()
        )
        # Answer the group callback silently to remove the loading spinner
        await callback.answer()
        logger.info(f"Sent vote confirmation DM to user {user_id} for token {token_key}")
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        err_msg = str(e).lower()
        if "blocked" in err_msg or "deactivated" in err_msg or "bot was blocked" in err_msg or "chat not found" in err_msg:
            logger.warning(f"Cannot send vote confirmation DM to user {user_id}: Bot blocked or chat not found.")
            await callback.answer("Could not send confirmation DM. Have you started a chat with me and not blocked me?", show_alert=True)
        else:
            logger.error(f"Error sending vote confirmation DM to user {user_id}: {e}")
            await callback.answer("An error occurred trying to send the confirmation message.", show_alert=True)
    except Exception as e:
        logger.exception(f"Unexpected error sending vote confirmation DM to user {user_id}: {e}")
        await callback.answer("An unexpected error occurred.", show_alert=True)


@router.callback_query(F.data.startswith("vote_confirm_"))
async def handle_vote_confirm_button(callback: CallbackQuery):
    """Handles the confirmation button clicked in DM."""
    user = callback.from_user
    user_name = user.username if user.username else user.first_name
    message = callback.message # The message in the DM
    logger.info(f"User {user.id} ({user_name}) clicked vote confirmation button: {callback.data}")

    try:
        token_key = callback.data.split("vote_confirm_", 1)[1]
        if not token_key: raise ValueError("Token key missing")
    except (IndexError, ValueError) as e:
        logger.error(f"Failed to parse token_key from vote_confirm callback: {callback.data}, Error: {e}")
        await callback.answer("Error: Could not identify the token from this button.", show_alert=True)
        # Try to edit the message to show error
        try: await message.edit_text("❌ Error identifying token. Please try voting again from the group.")
        except: pass
        return

    # Process the vote action logic (checks cooldowns, logs vote, saves)
    try:
        result_message, vote_succeeded = await process_vote_action(user.id, user_name, token_key)
    except Exception as e:
         logger.exception(f"Unexpected error during process_vote_action for user {user.id}, token {token_key} (confirm step): {e}")
         await callback.answer("An unexpected error occurred while processing your vote.", show_alert=True)
         try: await message.edit_text("❌ An unexpected error occurred processing the vote.", reply_markup=None)
         except: pass
         return

    # Show result to user (edit the DM message)
    try:
        await message.edit_text(result_message, reply_markup=None, disable_web_page_preview=True) # Remove button after processing
        # Answer callback silently if edit succeeds
        await callback.answer()
        logger.info(f"Vote processed for user {user.id}, token {token_key}. Success: {vote_succeeded}. Result msg sent to DM.")
    except Exception as e:
        logger.error(f"Failed to edit DM message or answer callback after vote confirmation for user {user.id}: {e}")
        # If editing fails, still try to answer the callback with the primary message
        await callback.answer(result_message.split('\n')[0], show_alert=True)

    # If vote succeeded, send the follow-up minigame prompt
    if vote_succeeded:
        try:
            await bot.send_message(
                chat_id=user.id,
                text=f"✨ Want to earn another free vote for this token?\n"
                     f"Try your luck with /bowl or /darts in the group! (Cooldown: {config.MINIGAME_COOLDOWN_HOURS}h)"
            )
            logger.info(f"Sent minigame prompt DM to user {user.id}")
        except Exception as e:
            logger.warning(f"Failed to send minigame prompt DM to user {user.id}: {e}")

# Remove or comment out the old button handler if no longer needed
# @router.callback_query(F.data.startswith("vote_"))
# async def handle_vote_button(callback: CallbackQuery):
#    # ... (old logic) ...
#    logger.warning(f"Old vote_ handler triggered for {callback.data}. This might be deprecated.")
#    await callback.answer("This button might be outdated. Please use /vote in the group.", show_alert=True)


# --- Cooldown/Free Vote Check Commands --- (Keep as is)
@router.message(Command("mycooldowns"))
async def handle_mycooldowns(message: Message):
    """Shows the user their active regular vote cooldowns."""
    user_id = message.from_user.id; user_id_str = str(user_id);
    logger.info(f"User {user_id} requested /mycooldowns")
    user_vote_data = user_votes.get(user_id_str, {});
    active_cooldowns = {}; # Store {token_key: timedelta}
    ready_to_vote_keys = [] # Store keys where cooldown just expired
    needs_save = False # Flag if cleanup occurs within get_cooldown_remaining

    # Iterate over a copy of keys for safe deletion if needed within the loop/helper
    current_tokens = list(user_vote_data.keys())
    logger.debug(f"Checking cooldowns for user {user_id} on tokens: {current_tokens}")

    for token_key in current_tokens:
        # get_cooldown_remaining now handles cleanup of expired entries
        cooldown = get_cooldown_remaining(user_id, token_key);
        if cooldown:
            active_cooldowns[token_key] = cooldown
        else:
            # Check if the key was actually removed by the helper
            # Check the main dict directly as helper modifies it
            if user_id_str in user_votes and token_key not in user_votes[user_id_str]:
                 logger.debug(f"Cooldown for {token_key} confirmed expired and cleaned up for user {user_id}.")
                 ready_to_vote_keys.append(token_key)
                 needs_save = True # Mark that data structure was modified by helper
            elif token_key not in user_vote_data: # If key wasn't even there initially
                 pass # Should not happen if iterating over keys, but safe check
            else: # Cooldown is None, but key still exists (error in helper?)
                 logger.warning(f"get_cooldown_remaining returned None for {token_key} user {user_id}, but key still exists in user_votes. Attempting manual cleanup.")
                 try:
                     del user_votes[user_id_str][token_key]
                     if not user_votes[user_id_str]: del user_votes[user_id_str]
                     needs_save = True
                 except KeyError: pass # Ignore if already gone

    # Further cleanup: if user entry is now empty after loop, remove it
    if user_id_str in user_votes and not user_votes[user_id_str]:
        logger.debug(f"Removing empty cooldown dict for user {user_id}.")
        del user_votes[user_id_str]
        needs_save = True

    if needs_save:
        logger.info("Saving cooldown data after cleanup.")
        await save_data() # Save if any cooldowns were cleared

    # --- Construct Reply ---
    if not active_cooldowns and not ready_to_vote_keys:
        await message.reply("You have no active regular vote cooldowns."); return

    reply_lines = ["<b>🕒 Your REGULAR Vote Cooldowns:</b>"]
    if active_cooldowns:
        # Sort by remaining time (shortest first)
        sorted_cooldowns = sorted(active_cooldowns.items(), key=lambda item: item[1])
        for token_key, cooldown_time in sorted_cooldowns:
            try:
                token_display = await get_token_display_info(token_key);
                reply_lines.append(f"⏳ {token_display}: <b>{format_timedelta(cooldown_time)}</b> left")
            except Exception as e:
                 logger.error(f"Error getting token display {token_key} for mycooldowns: {e}")
                 reply_lines.append(f"⏳ Token ({token_key[:6]}...): <b>{format_timedelta(cooldown_time)}</b> left")
    else:
        reply_lines.append("\n✅ All tracked regular cooldowns have expired!")

    # Optionally mention tokens that just became available if any were cleaned up
    if ready_to_vote_keys:
         reply_lines.append("\n✅ You can now vote again for:")
         # Sort ready tokens alphabetically
         ready_to_vote_keys.sort()
         for token_key in ready_to_vote_keys[:5]: # Show a few
             try:
                 token_display = await get_token_display_info(token_key)
                 reply_lines.append(f"   - {token_display}")
             except Exception: reply_lines.append(f"   - Token ({token_key[:6]}...)")
         if len(ready_to_vote_keys) > 5: reply_lines.append("   - ... and more!")

    await message.reply("\n".join(reply_lines))

@router.message(Command("myfreevotes"))
async def handle_myfreevotes(message: Message):
    """Shows the user's balance of free votes."""
    user_id = message.from_user.id; user_id_str = str(user_id);
    logger.info(f"User {user_id} requested /myfreevotes")
    free_vote_data = user_free_votes.get(user_id_str, {})
    needs_save = False # Flag for cleanup

    if not free_vote_data:
        await message.reply(f"You currently have no free votes. Play /bowl or /darts in a configured group for a chance to win {config.FREE_VOTE_REWARD_COUNT}!"); return

    reply_lines = ["<b>🎟️ Your Free Votes Balance:</b>"]
    total_free_votes = 0;
    active_token_votes = [] # Store tuples of (display_name, count)

    # Iterate safely and clean up zero counts
    for token_key, count in list(free_vote_data.items()):
        if count is None or count <= 0:
            logger.debug(f"Cleaning up zero/invalid count free vote entry for user {user_id}, token {token_key}")
            try:
                 del user_free_votes[user_id_str][token_key]
                 needs_save = True
            except KeyError: pass # Already gone
            continue # Skip to next token

        # Valid count > 0
        try:
            token_display = await get_token_display_info(token_key);
            active_token_votes.append((token_display, count))
            total_free_votes += count
        except Exception as e:
            logger.error(f"Error getting display info for token {token_key} in myfreevotes: {e}")
            # Still count it, but use key as display fallback
            active_token_votes.append((f"Token ({token_key})", count))
            total_free_votes += count

    # Check if user dict is now empty after cleanup
    if user_id_str in user_free_votes and not user_free_votes[user_id_str]:
         logger.debug(f"Removing empty free vote dict for user {user_id}.")
         del user_free_votes[user_id_str]
         needs_save = True

    if needs_save:
         logger.info("Saving free vote data after cleanup.")
         await save_data()

    # Construct reply based on remaining votes
    if not active_token_votes: # If all entries were cleaned up or initially empty
        await message.reply(f"You currently have no active free votes. Play /bowl or /darts in a configured group for a chance to win {config.FREE_VOTE_REWARD_COUNT}!"); return

    # Sort alphabetically by token display name for consistent order
    active_token_votes.sort(key=lambda x: x[0])

    for token_display, count in active_token_votes:
        reply_lines.append(f"✨ {token_display}: <b>{count}</b>")

    reply_lines.append(f"\nTotal Free Votes: <b>{total_free_votes}</b>.");
    reply_lines.append("\n<i>Use the /vote command in the respective group to automatically use a free vote first.</i>");
    await message.reply("\n".join(reply_lines))


# --- Trending Command --- (Keep as is)
@router.message(Command("trending"))
async def handle_trending(message: Message):
    """Displays the globally trending tokens based on recent votes."""
    logger.info(f"User {message.from_user.id} requested /trending")
    try:
        trending_tokens = get_trending_tokens(config.TRENDING_WINDOW_HOURS)
    except Exception as e:
        logger.exception("Error getting trending tokens:")
        await message.answer("An error occurred while calculating trending tokens.")
        return

    if not trending_tokens:
        await message.answer(f"🤷 No tokens seem to be trending based on votes in the last {config.TRENDING_WINDOW_HOURS} hours."); return

    display_limit = min(len(trending_tokens), config.SCOREBOARD_TOP_N) # Use scoreboard limit for consistency
    reply_text = f"🔥 <b>Top {display_limit} Trending Tokens by Votes ({config.TRENDING_WINDOW_HOURS}h Window):</b>\n\n"

    processed_count = 0
    for token_key, vote_count in trending_tokens:
        if processed_count >= display_limit: break
        rank = processed_count + 1;
        try:
            token_display = await get_token_display_info(token_key);
        except Exception as e:
            logger.error(f"Error getting display info for {token_key} in /trending: {e}")
            token_display = f"Error ({token_key[:6]}...)"

        mcap_str = ""
        price_change_str = ""

        # Attempt to add market data if available in cache
        if token_key in token_cache:
            token_data = token_cache[token_key]
            mcap = token_data.get('market_cap_usd')
            if mcap is not None and mcap > 0:
                 mcap_str = f" | MC: ${mcap:,.0f}"
            price_change = token_data.get('price_change_h24')
            if price_change is not None:
                 change_emoji = "📈" if price_change >= 0 else "📉"
                 change_sign = "+" if price_change >= 0 else ""
                 price_change_str = f" {change_emoji}{change_sign}{price_change:.1f}%"

        reply_text += f"{rank}. {token_display} - <b>{vote_count}</b> votes{mcap_str}{price_change_str}\n"
        processed_count += 1

    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
    reply_text += f"\n<i>Vote data retrieved: {timestamp}</i>"
    await message.answer(reply_text, disable_web_page_preview=True)


# --- Admin Commands --- (Keep as is)
async def _admin_command_check(message: Message) -> bool:
    """Helper check for admin commands executed in group chats."""
    if message.chat.type == ChatType.PRIVATE:
        await message.reply("This command must be executed within the group chat it applies to."); return False
    # Check if the invoker is an admin
    logger.debug(f"Checking admin status for user {message.from_user.id} in chat {message.chat.id} for command {message.text}")
    is_admin = await is_user_admin(message.chat.id, message.from_user.id) # Uses the fixed version
    if not is_admin:
        user_display = f"@{message.from_user.username}" if message.from_user.username else f"User {message.from_user.id}"
        logger.warning(f"User {user_display} ({message.from_user.id}) tried admin command {message.text} in {message.chat.id} but is not admin.")
        try: await message.delete() # Try to delete unauthorized command
        except TelegramAPIError: pass
        # Send temporary error message
        await message.answer(f"❌ {user_display}, you need to be a group administrator to use this command.", delete_after=20)
        return False
    logger.debug(f"Admin check passed for user {message.from_user.id} in chat {message.chat.id}")
    return True

@router.message(Command("viewconfig"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_viewconfig(message: Message):
    """Displays the current bot configuration for the group."""
    logger.info(f"User {message.from_user.id} requested /viewconfig in chat {message.chat.id}")
    if not await _admin_command_check(message): return

    group_id_str = str(message.chat.id);
    config_data = group_configs.get(group_id_str)

    if not config_data or not config_data.get('config', {}).get('setup_complete'):
        logger.info(f"viewconfig requested in group {group_id_str} but setup is not complete.")
        await message.reply("The bot setup has not been completed for this group yet. Use /setup first."); return

    token_key = config_data.get("token_key");
    conf = config_data.get("config", {});

    token_display = "N/A (Error!)"
    media_status = "Not Set" # <<< Default Media Status
    if token_key:
        try: token_display = await get_token_display_info(token_key)
        except Exception as e: logger.error(f"Error getting token display for {token_key} in viewconfig: {e}")
        # Check token_cache for media_file_id
        if token_cache.get(token_key, {}).get('media_file_id'):
            media_status = "✅ Set" # <<< Update status if found

    group_link = conf.get("group_link") or "Not Set"
    admin_user_id = conf.get('admin_user_id', 'N/A')
    admin_display = f"<a href='tg://user?id={admin_user_id}'>User {admin_user_id}</a>" if isinstance(admin_user_id, int) else 'N/A'

    updated_at_str = conf.get('updated_at', 'N/A')
    updated_display = updated_at_str
    try:
        if updated_at_str != 'N/A':
            dt_obj = datetime.fromisoformat(updated_at_str)
            if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            updated_display = dt_obj.strftime("%Y-%m-%d %H:%M:%S %Z")
    except ValueError: updated_display = updated_at_str

    lines = [f"⚙️ <b>Current Bot Configuration for this Group:</b>",
             "------------------------------------",
             f"🏷️ <b>Token:</b> {token_display}",
             f"   <i>(Internal Key: {hcode(token_key or 'N/A')})</i>",
             f"🖼️ <b>Token Media:</b> {media_status}", # <<< Show Media Status
             f"🔗 <b>Group Link:</b> {hlink('Link', group_link) if group_link != 'Not Set' else 'Not Set'}",
             f"🔔 <b>Vote Notifications:</b> {'✅ On' if conf.get('notifications_enabled', True) else '❌ Off'}",
             f"📊 <b>Notification Threshold:</b> {conf.get('notification_threshold', 1)} vote(s)",
             f"🛠️ <b>Setup By Admin:</b> {admin_display}",
             f"⏰ <b>Config Last Updated:</b> {updated_display}",
             f"🚦 <b>Bot Status for Group:</b> {'✅ Active' if conf.get('is_active', True) else '❌ Inactive'}",
             "------------------------------------"]
    await message.reply("\n".join(lines), disable_web_page_preview=True)


@router.message(Command("pinscoreboard"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_pin_scoreboard(message: Message):
    """Pins the live scoreboard message."""
    logger.info(f"User {message.from_user.id} requested /pinscoreboard in chat {message.chat.id}")
    if not await _admin_command_check(message): return

    chat_id = message.chat.id; chat_id_str = str(chat_id); user_id = message.from_user.id

    # 1. Check Bot's Pin Permission
    try:
        me = await bot.get_me();
        logger.debug(f"Checking bot ({me.id}) pin permission in chat {chat_id}")
        bot_member = await bot.get_chat_member(chat_id, me.id);

        # Check if the bot is an admin first
        if bot_member.status != ChatMemberStatus.ADMINISTRATOR: # Use Enum
             await message.reply("❌ The bot needs to be an administrator with the 'Pin Messages' permission to use this command.");
             logger.warning(f"Pin scoreboard failed: Bot is not admin in chat {chat_id} (status: {bot_member.status})")
             return

        # If admin, check the specific permission
        if isinstance(bot_member, ChatMemberAdministrator):
             if not bot_member.can_pin_messages:
                  await message.reply("❌ The bot is an admin, but lacks the specific 'Pin Messages' permission. Please grant it in group settings.");
                  logger.warning(f"Pin scoreboard failed: Bot is admin but lacks can_pin_messages right in {chat_id}")
                  return
             else:
                  logger.debug(f"Bot has can_pin_messages permission in {chat_id}.")
        else:
             # Should not happen if status is admin, but as a fallback
             logger.error(f"Bot status is admin but type is not ChatMemberAdministrator ({type(bot_member)}), cannot verify pin permission.")
             await message.reply("⚠️ Could not reliably verify the bot's pin permission. Please ensure it's an admin with the right enabled.")
             return

    except TelegramAPIError as e:
        logger.error(f"Could not verify bot permissions for pinning in chat {chat_id}: {e}");
        await message.reply("⚠️ Couldn't verify the bot's permissions. Please ensure it's an admin with 'Pin Messages' rights."); return
    except Exception as e:
         logger.exception(f"Unexpected error checking pin permission in {chat_id}: {e}")
         await message.reply("An unexpected error occurred while checking permissions.")
         return

    # 2. Check if already pinned by this bot
    if chat_id_str in pinned_messages:
        logger.info(f"Scoreboard already tracked as pinned (msg {pinned_messages[chat_id_str]}) in chat {chat_id_str}. Informing user.")
        await message.reply(f"ℹ️ It seems a scoreboard message (ID: {pinned_messages[chat_id_str]}) is already tracked as pinned by me in this chat. Use /resettoken first if you want to replace it, or unpin the old one manually."); return

    # 3. Send the initial scoreboard message
    try:
        logger.debug(f"Generating scoreboard text for initial pin in {chat_id}")
        scoreboard_text = await generate_scoreboard_text(include_market_data=True) # Generate content
        sent_message = await bot.send_message(
             chat_id=chat_id,
             text=scoreboard_text,
             disable_web_page_preview=True
        );
        message_id = sent_message.message_id
        logger.info(f"Scoreboard message {message_id} sent to chat {chat_id} for pinning.")
    except Exception as e:
        logger.exception(f"Failed to send initial scoreboard message to chat {chat_id}");
        await message.reply("❌ An error occurred while trying to send the scoreboard message."); return

    # 4. Pin the sent message
    try:
        logger.info(f"Attempting to pin message {message_id} in chat {chat_id}")
        await bot.pin_chat_message(
             chat_id=chat_id,
             message_id=message_id,
             disable_notification=True # Pin silently
        );
        logger.info(f"Scoreboard message {message_id} successfully pinned in chat {chat_id} by user {user_id}.")
    except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError) as e:
        logger.error(f"Failed to pin scoreboard message {message_id} in chat {chat_id}: {type(e).__name__} - {e}");
        # Try to delete the message we just sent if pinning failed
        try: await bot.delete_message(chat_id, message_id)
        except Exception: logger.warning(f"Could not delete unpinnable scoreboard message {message_id} in {chat_id}")
        await message.reply(f"❌ Failed to pin the scoreboard message (ID: {message_id}). Error: {e.message}. Please check the bot's 'Pin Messages' permission again. I have deleted the message I sent.");
        return # Stop if pin failed
    except Exception as e:
        logger.exception(f"Unexpected error pinning scoreboard message {message_id} in chat {chat_id}: {e}")
        await message.reply("❌ An unexpected error occurred while trying to pin the scoreboard.")
        return # Stop on unexpected error

    # 5. Pinning succeeded: Save state and confirm
    pinned_messages[chat_id_str] = message_id;
    await save_data();
    await message.reply(f"✅ Scoreboard message pinned successfully! It will be updated automatically every {config.SCOREBOARD_UPDATE_INTERVAL_MINUTES} minutes.")

@router.message(Command("unpin"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_unpin_scoreboard(message: Message):
    """Unpins the tracked scoreboard message."""
    logger.info(f"User {message.from_user.id} requested /unpin in chat {message.chat.id}")
    if not await _admin_command_check(message): return

    chat_id = message.chat.id
    chat_id_str = str(chat_id)

    if chat_id_str not in pinned_messages:
        logger.info(f"/unpin command in {chat_id}: No scoreboard message tracked.")
        await message.reply("ℹ️ I don't have a record of a pinned scoreboard message in this chat.")
        return

    pinned_msg_id = pinned_messages[chat_id_str]
    logger.info(f"Attempting to unpin tracked message {pinned_msg_id} in chat {chat_id}")

    # Check bot permission
    try:
        me = await bot.get_me()
        bot_member = await bot.get_chat_member(chat_id, me.id)
        if bot_member.status == ChatMemberStatus.ADMINISTRATOR and isinstance(bot_member, ChatMemberAdministrator):
             if not bot_member.can_pin_messages: # Need same perm for unpin
                  await message.reply("❌ The bot needs the 'Pin Messages' permission to unpin messages.")
                  logger.warning(f"Unpin failed: Bot lacks can_pin_messages permission in {chat_id}")
                  return
        elif bot_member.status != ChatMemberStatus.ADMINISTRATOR:
             await message.reply("❌ The bot needs to be an administrator with 'Pin Messages' permission.")
             logger.warning(f"Unpin failed: Bot is not admin in {chat_id}")
             return
    except Exception as e:
        logger.exception(f"Error checking bot permission for unpinning in {chat_id}: {e}")
        await message.reply("⚠️ Could not verify bot permissions. Ensure it's an admin with pin rights.")
        return

    # Attempt to unpin
    unpinned_successfully = False
    try:
        await bot.unpin_chat_message(chat_id=chat_id, message_id=pinned_msg_id)
        unpinned_successfully = True
        logger.info(f"Successfully unpinned message {pinned_msg_id} in chat {chat_id}")
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        err_msg = str(e).lower()
        if "message to unpin not found" in err_msg or "message not found" in err_msg:
            logger.warning(f"Message {pinned_msg_id} to unpin not found in chat {chat_id} (already unpinned/deleted?).")
            unpinned_successfully = True # Consider it done if not found
        elif "not enough rights" in err_msg or "can't manage pinned messages" in err_msg:
             logger.warning(f"Bot lacked permission to unpin {pinned_msg_id} in {chat_id} despite earlier check?")
        else:
             logger.error(f"Failed to unpin message {pinned_msg_id} in {chat_id}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error unpinning message {pinned_msg_id} in {chat_id}: {e}")

    # Remove from tracking if successful or message gone
    if unpinned_successfully:
        del pinned_messages[chat_id_str]
        await save_data()
        await message.reply("✅ Tracked scoreboard message has been unpinned.")
    else:
        await message.reply("❌ Failed to unpin the tracked scoreboard message. Please check bot permissions or unpin manually.")


@router.message(Command("resettoken"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_resettoken(message: Message):
    """Resets the bot's configuration for the current group."""
    logger.info(f"User {message.from_user.id} requested /resettoken in chat {message.chat.id}")
    if not await _admin_command_check(message): return

    group_id_str = str(message.chat.id)
    user_display = f"@{message.from_user.username}" if message.from_user.username else f"User {message.from_user.id}"

    if group_id_str in group_configs:
        logger.info(f"Resetting token config for group {group_id_str} initiated by {user_display}")
        # Store details before deleting
        token_key_to_remove = group_configs[group_id_str].get("token_key")
        pinned_msg_id = pinned_messages.get(group_id_str) # Get pinned ID before deleting config

        # Delete group config first
        del group_configs[group_id_str]
        logger.debug(f"Removed group config for {group_id_str}")

        unpinned_successfully = False
        # Attempt to unpin scoreboard if one was tracked
        if pinned_msg_id:
            logger.info(f"Attempting to unpin scoreboard message {pinned_msg_id} in chat {group_id_str}")
            try:
                await bot.unpin_chat_message(chat_id=message.chat.id, message_id=pinned_msg_id);
                unpinned_successfully = True;
                logger.info(f"Successfully unpinned scoreboard message {pinned_msg_id} in chat {message.chat.id}")
            except (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError) as e:
                err_msg = str(e).lower()
                if "message to unpin not found" in err_msg or "message not found" in err_msg:
                    logger.warning(f"Scoreboard message {pinned_msg_id} to unpin not found (already deleted or unpinned?) in chat {message.chat.id}.")
                    unpinned_successfully = True # Treat as success if already gone
                elif "not enough rights" in err_msg or "can't manage pinned messages" in err_msg:
                    logger.warning(f"Bot lacks permission to unpin message {pinned_msg_id} in chat {message.chat.id}. Admin may need to unpin manually.")
                else:
                    logger.warning(f"Could not unpin message {pinned_msg_id} in chat {message.chat.id}: {type(e).__name__} - {e}")
            except Exception as e:
                logger.exception(f"Unexpected error unpinning message {pinned_msg_id} in chat {message.chat.id}: {e}")

            # Remove from tracking dictionary regardless of unpin success/failure
            if group_id_str in pinned_messages:
                 del pinned_messages[group_id_str]
                 logger.debug(f"Removed pin tracking for chat {group_id_str}")

        # Clean up associated data structures for the token (use token_key_to_remove)
        if token_key_to_remove:
            logger.debug(f"Cleaning up cache/state for removed token key: {token_key_to_remove}")
            if token_key_to_remove in token_cache:
                logger.debug(f"Removing token {token_key_to_remove} from token_cache.")
                del token_cache[token_key_to_remove]
            if token_key_to_remove in market_data_state:
                logger.debug(f"Removing token {token_key_to_remove} from market_data_state.")
                del market_data_state[token_key_to_remove]
            # Do NOT clear user votes/log as they might be global or used by other groups

        # Save all changes
        await save_data();
        logger.info(f"Reset complete for group {group_id_str}. Data saved.")

        # Confirmation message
        reply_text = "✅ Bot configuration for this group has been successfully reset."
        if pinned_msg_id: # If there *was* a pinned message tracked
             if unpinned_successfully: reply_text += " The tracked scoreboard message has been unpinned."
             else: reply_text += " However, I failed to unpin the tracked scoreboard message - please unpin it manually if needed."
        else:
             reply_text += " (No scoreboard message was tracked as pinned for this group)."

        reply_text += "\nYou can now run /setup again to configure a new token for this group.";
        await message.reply(reply_text)
    else:
        # Group was not configured
        # --- FIX: Use message.from_user.id ---
        logger.info(f"User {message.from_user.id} tried to reset group {group_id_str}, but it was not found in config.")
        await message.reply("The bot doesn't seem to be configured for this group. Nothing to reset.")


@router.message(Command("togglenotifications"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_togglenotifications(message: Message):
    """Toggles vote notifications on/off for the group."""
    logger.info(f"User {message.from_user.id} requested /togglenotifications in chat {message.chat.id}")
    if not await _admin_command_check(message): return

    group_id_str = str(message.chat.id);
    config_data = group_configs.get(group_id_str)

    if not config_data or not config_data.get('config', {}).get('setup_complete'):
        logger.warning(f"togglenotifications failed: Setup not complete for group {group_id_str}")
        await message.reply("The bot setup must be completed first using /setup before managing notifications."); return

    conf = config_data["config"]; # Get the inner config dict
    current_status = conf.get("notifications_enabled", True); # Default to True if key missing
    new_status = not current_status

    # Update the config
    conf["notifications_enabled"] = new_status;
    conf["updated_at"] = datetime.now(timezone.utc).isoformat(); # Update timestamp
    await save_data(); # Save the change
    logger.info(f"Notifications for group {group_id_str} toggled to {new_status} by user {message.from_user.id}")

    await message.reply(f"✅ Vote notifications for this group have been turned <b>{'ON' if new_status else 'OFF'}</b>.")

# --- <<< INSERT THIS FUNCTION DEFINITION >>> ---
# (Place it before send_pumping_notification, etc.)

async def send_main_channel_notification(text: str, photo_url: typing.Optional[str] = None, reply_markup=None):
    """Helper to send notifications to configured main channel AND/OR group."""
    sent_channel = False
    sent_group = False
    logger.debug("Attempting to send main channel/group notification.")

    async def _send(target_chat_id_config, chat_name):
        """Inner send helper with robust error handling."""
        if not target_chat_id_config:
            logger.debug(f"Notification target '{chat_name}' not configured, skipping.")
            return False # Not an error, just not configured

        target_chat_id = None
        try:
            # Convert target_chat_id_config to integer ID or use username string
            if isinstance(target_chat_id_config, str) and target_chat_id_config.lstrip('-').isdigit():
                target_chat_id = int(target_chat_id_config)
            elif isinstance(target_chat_id_config, int):
                 target_chat_id = target_chat_id_config
            elif isinstance(target_chat_id_config, str) and target_chat_id_config.startswith('@'):
                target_chat_id = target_chat_id_config # Keep as string for send_message/photo
            else:
                 logger.error(f"Invalid chat ID format for notification target '{chat_name}': {target_chat_id_config}")
                 return False

            logger.debug(f"Sending notification to {chat_name} ({target_chat_id}). Photo: {'Yes' if photo_url else 'No'}")
            # Attempt to send message or photo
            if photo_url:
                 # Note: Sending photos by URL might require the bot to download it first.
                 # Using FSInputFile is generally for local files. If photo_url is a web URL,
                 # sending it directly might work for Telegram, but consider error handling.
                 # For simplicity, assuming photo_url works directly if provided.
                 await bot.send_photo(target_chat_id, photo=photo_url, caption=text, reply_markup=reply_markup)
            else:
                 await bot.send_message(target_chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
            logger.info(f"Notification sent successfully to {chat_name} ({target_chat_id})")
            return True

        except ValueError as e: # Catch conversion errors if any slip through
             logger.error(f"Invalid Chat ID value during send for {chat_name}: {target_chat_id_config}, Error: {e}")
             return False
        except TelegramRetryAfter as e:
            logger.warning(f"Rate limit hit sending notification to {chat_name} ({target_chat_id}). Retrying after {e.retry_after}s.")
            await asyncio.sleep(e.retry_after + 1)
            # Retry ONLY ONCE after waiting
            try:
                logger.debug(f"Retrying notification send to {chat_name} ({target_chat_id})")
                if photo_url: await bot.send_photo(target_chat_id, photo=photo_url, caption=text, reply_markup=reply_markup)
                else: await bot.send_message(target_chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
                logger.info(f"Notification sent successfully to {chat_name} ({target_chat_id}) after retry.")
                return True
            except Exception as retry_e:
                logger.error(f"Failed sending notification to {chat_name} ({target_chat_id}) even after retry: {type(retry_e).__name__} - {retry_e}")
                return False
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            err_msg = str(e).lower()
            # Check for common blocking/permission errors
            if "chat not found" in err_msg or "bot was kicked" in err_msg or "user is deactivated" in err_msg or "bot is not a member" in err_msg or "peer_id_invalid" in err_msg:
                 logger.error(f"Cannot send notification to {chat_name} ({target_chat_id}): Bot blocked/kicked/chat deleted/invalid ID. Error: {e}")
                 # TODO: Consider adding logic here to disable this target in config if persistent?
            elif "have no rights to send" in err_msg or "not enough rights" in err_msg:
                 logger.error(f"Cannot send notification to {chat_name} ({target_chat_id}): Missing required permissions (Send Messages/Media?). Error: {e}")
            else:
                 logger.error(f"Telegram error sending notification to {chat_name} ({target_chat_id}): {type(e).__name__} - {e}")
            return False
        except Exception as e:
            logger.exception(f"Unexpected error sending notification to {chat_name} ({target_chat_id}): {e}")
            return False

    # Send to Main Channel if configured
    sent_channel = await _send(config.MAIN_CHANNEL_ID, "Main Channel")

    # Send to Main Group if configured AND different from Main Channel
    if hasattr(config, 'MAIN_GROUP_ID') and config.MAIN_GROUP_ID and config.MAIN_GROUP_ID != config.MAIN_CHANNEL_ID:
        logger.debug("Main Group ID differs from Channel ID, attempting send to group.")
        sent_group = await _send(config.MAIN_GROUP_ID, "Main Group")
    elif hasattr(config, 'MAIN_GROUP_ID') and config.MAIN_GROUP_ID and config.MAIN_GROUP_ID == config.MAIN_CHANNEL_ID:
        logger.debug("Main Group ID is same as Main Channel ID, skipping duplicate notification to group.")
        sent_group = sent_channel # Consider it sent if channel send worked
    else:
         logger.debug("Main Group ID not configured or same as channel.")

    return sent_channel or sent_group # Return True if sent to at least one target successfully

# --- <<< END OF INSERTED FUNCTION >>> ---


async def send_pumping_notification(token_key: str, multiplier: int, current_mcap: float, first_mcap: float):
    """Formats and sends the 'Pumping!' notification, potentially with image."""
    logger.info(f"Formatting 'Pumping' notification for {token_key} ({multiplier}x)")
    token_info = token_cache.get(token_key, {});
    token_display = await get_token_display_info(token_key, include_symbol=True);
    group_link = token_info.get('group_link');
    dex_link = token_info.get('url') # Dexscreener link from cache
    contract_addr = token_key.split('_')[0] if '_' in token_key else token_key # Extract CA

    text = (f"🟢 {hbold(token_display)} is Pumping! 🚀\n\n"
            f"💲 {hbold(f'{multiplier}x')} Increase from first tracked MC!\n"
            f"   (From ${first_mcap:,.0f} to ${current_mcap:,.0f})\n\n"
            f"🔸 CA: {hcode(contract_addr)}\n")
    if group_link:
        text += f"🔸 Group: {hlink('Join Chat', group_link)}\n"
    text += f"🔸 Marketcap: ${current_mcap:,.0f}\n" # Always include current MC

    keyboard = InlineKeyboardBuilder();
    if dex_link: keyboard.button(text="📊 Chart (DexScreener)", url=dex_link);
    if group_link: keyboard.button(text="💬 Group Chat", url=group_link);
    keyboard.adjust(1) # Arrange buttons vertically
    markup = keyboard.as_markup()

    # --- Add Image Logic ---
    image_path = None
    image_filename = getattr(config, 'PUMPING_NOTIF_IMAGE', None)
    if image_filename:
        potential_path = os.path.join(config.IMAGES_DIR, image_filename)
        if os.path.isfile(potential_path):
            image_path = potential_path
        else:
            logger.warning(f"Pumping notification image not found: {potential_path}")

    photo_input = FSInputFile(image_path) if image_path else None
    await send_main_channel_notification(text, photo_url=photo_input, reply_markup=markup)



async def send_leaderboard_entry_notification(token_key: str, rank: int, current_mcap: float):
    """Formats and sends the 'Entered Leaderboard' notification, potentially with image."""
    logger.info(f"Formatting 'Leaderboard Entry' notification for {token_key} (Rank {rank})")
    token_info = token_cache.get(token_key, {});
    token_display = await get_token_display_info(token_key, include_symbol=True);
    group_link = token_info.get('group_link');
    dex_link = token_info.get('url') # Dexscreener link
    contract_addr = token_key.split('_')[0] if '_' in token_key else token_key

    text = (f"🏆 {hbold(token_display)} entered Top {config.SCOREBOARD_TOP_N} Trending Leaderboard at #{rank}!\n\n"
            f"🔸 CA: {hcode(contract_addr)}\n")
    if group_link:
        text += f"🔸 Group: {hlink('Join Chat', group_link)}\n"
    text += f"🔸 Marketcap: ${current_mcap:,.0f}\n"

    keyboard = InlineKeyboardBuilder();
    if dex_link: keyboard.button(text="📊 Chart (DexScreener)", url=dex_link);
    if group_link: keyboard.button(text="💬 Group Chat", url=group_link);
    keyboard.adjust(1)
    markup = keyboard.as_markup()

    # --- Add Image Logic ---
    image_path = None
    image_filename = getattr(config, 'LEADERBOARD_ENTRY_IMAGE', None)
    if image_filename:
        potential_path = os.path.join(config.IMAGES_DIR, image_filename)
        if os.path.isfile(potential_path):
            image_path = potential_path
        else:
            logger.warning(f"Leaderboard entry image not found: {potential_path}")

    photo_input = FSInputFile(image_path) if image_path else None
    await send_main_channel_notification(text, photo_url=photo_input, reply_markup=markup)




async def send_biggest_gainers_post():
    """Fetches data, calculates gainers, and posts the list, potentially with image."""
    global last_gainers_post_time
    logger.info("Attempting to generate and post 'Biggest Gainers' list...")

    gainers = [] # List to hold eligible gainers' data
    tracked_token_keys = list(token_cache.keys()) # Get currently tracked tokens

    # Define criteria from config with defaults
    min_mcap_gainer = getattr(config, 'BIGGEST_GAINERS_MIN_MCAP', 0)
    min_change_gainer = getattr(config, 'BIGGEST_GAINERS_MIN_CHANGE_PCT', 0)
    list_count_gainer = getattr(config, 'BIGGEST_GAINERS_COUNT', 10)

    # Filter tokens based on criteria
    logger.debug(f"Filtering {len(tracked_token_keys)} tokens for gainers post. Min MCAP: {min_mcap_gainer}, Min Change: {min_change_gainer}%")
    for token_key in tracked_token_keys:
        token_data = token_cache.get(token_key)
        if not token_data: continue

        change_pct = token_data.get('price_change_h24')
        mcap = token_data.get('market_cap_usd', 0)

        if change_pct is not None and mcap > min_mcap_gainer:
             if change_pct > min_change_gainer:
                  logger.debug(f"Token {token_key} met criteria: MCAP={mcap}, Change={change_pct}%")
                  gainers.append({
                      "key": token_key, "name": token_data.get("name", token_key),
                      "symbol": token_data.get("symbol", ""), "mcap": mcap,
                      "change_pct": change_pct, "dex_link": token_data.get("url"),
                      "group_link": token_data.get("group_link"),
                  })

    if not gainers:
        logger.info("No tokens met the 'Biggest Gainers' criteria for posting this cycle.")
        return

    gainers.sort(key=lambda x: x['change_pct'], reverse=True)
    logger.info(f"Found {len(gainers)} eligible gainers. Sorting complete.")

    # Prepare the post text
    text = (f"📈 {hbold('Biggest Gainers - Last 24 Hours')}\n"
            f"(Min MCAP: ${min_mcap_gainer:,.0f}, Min Change: {min_change_gainer}%)\n\n")

    gainers_to_list = gainers[:list_count_gainer]
    logger.debug(f"Listing top {len(gainers_to_list)} gainers.")

    for i, gainer in enumerate(gainers_to_list):
        rank = i + 1;
        symbol_str = f" ({hbold(gainer['symbol'])})" if gainer['symbol'] else "";
        name_display = hlink(gainer['name'], gainer['dex_link']) if gainer['dex_link'] else hbold(gainer['name'])
        change_sign = "+" if gainer['change_pct'] >= 0 else ""

        text += f"{rank}. {name_display}{symbol_str}\n";
        text += f"   💰 MC: ${gainer['mcap']:,.0f} | 💹 {change_sign}{gainer['change_pct']:.1f}%\n"
        if gainer['group_link']: text += f"   {hlink('➡️ Join Group', gainer['group_link'])}\n"
        text += "\n"

    promo_url = getattr(config, 'PROMO_URL', None)
    if promo_url:
         promo_link_text = hlink('Buy Trending Spots Here!', promo_url)
         text += f"\n⚡️ Want your token featured? {promo_link_text}"

    # --- Add Image Logic ---
    image_path = None
    image_filename = getattr(config, 'GAINERS_POST_IMAGE', None)
    if image_filename:
        potential_path = os.path.join(config.IMAGES_DIR, image_filename)
        if os.path.isfile(potential_path):
            image_path = potential_path
        else:
            logger.warning(f"Biggest Gainers image not found: {potential_path}")

    photo_input = FSInputFile(image_path) if image_path else None

    # Send the notification (with or without image)
    logger.info("Sending formatted gainers list...")
    sent = await send_main_channel_notification(text, photo_url=photo_input, disable_web_page_preview=True); # Pass photo_input

    if sent:
        logger.info(f"Successfully sent 'Biggest Gainers' post ({len(gainers)} eligible, {len(gainers_to_list)} listed).")
        last_gainers_post_time = datetime.now(timezone.utc);
        await save_data() # Save the updated last post time
    else:
        logger.error("Failed to send 'Biggest Gainers' post to any target channel/group.")

# --- <<< END OF INSERTED FUNCTIONS >>> ---

@router.message(Command("setvotethreshold"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_setvotethreshold(message: Message):
    """Sets the vote notification threshold for the group."""
    logger.info(f"User {message.from_user.id} requested /setvotethreshold in chat {message.chat.id}")
    if not await _admin_command_check(message): return

    group_id_str = str(message.chat.id);
    config_data = group_configs.get(group_id_str)

    if not config_data or not config_data.get('config', {}).get('setup_complete'):
        logger.warning(f"setvotethreshold failed: Setup not complete for group {group_id_str}")
        await message.reply("The bot setup must be completed first using /setup before setting the vote threshold."); return

    args = message.text.split() if message.text else [];

    if len(args) < 2:
        await message.reply("<b>Usage:</b> /setvotethreshold <code>[number]</code>\nExample: <code>/setvotethreshold 5</code> (Notifies on 1st vote, then 5th, 10th, etc.)"); return

    try:
        threshold = int(args[1]);
        if threshold < 1:
            # Raise value error for consistency
            raise ValueError("Threshold must be a positive integer (1 or greater).")

        # Update config
        conf = config_data["config"];
        conf["notification_threshold"] = threshold;
        conf["updated_at"] = datetime.now(timezone.utc).isoformat(); # Update timestamp
        await save_data(); # Save the change
        logger.info(f"Vote threshold for group {group_id_str} set to {threshold} by user {message.from_user.id}")

        await message.reply(f"✅ Vote notification threshold set to <b>{threshold}</b>.\nA notification will be sent for the first vote, and then every {threshold} votes thereafter.")

    except ValueError:
        logger.warning(f"Invalid threshold value '{args[1]}' provided by user {message.from_user.id} in chat {group_id_str}")
        await message.reply("⚠️ Invalid number provided. Please enter a whole number greater than 0 (e.g., 1, 5, 10).")
    except Exception as e:
        logger.exception(f"Error processing setvotethreshold for group {group_id_str}: {e}")
        await message.reply("An unexpected error occurred while setting the threshold.")

# --- Background Tasks --- (Keep as is, relies on fixed helpers/config)
async def periodic_scoreboard_update():
    """Periodically updates all pinned scoreboard messages."""
    await asyncio.sleep(20) # Initial delay before first update cycle
    logger.info("Periodic scoreboard updater task started.")
    while True:
        # --- Outer Task Loop Try/Except ---
        update_interval_seconds = config.SCOREBOARD_UPDATE_INTERVAL_MINUTES * 60
        try:
            # Operate on a copy of the keys to avoid modification issues during iteration
            pinned_chats_copy = list(pinned_messages.keys())

            if pinned_chats_copy:
                logger.info(f"Starting scoreboard update cycle for {len(pinned_chats_copy)} pinned messages...")
                # Generate the current scoreboard content ONCE per cycle
                current_scoreboard_text = await generate_scoreboard_text(include_market_data=True)
                logger.debug("Generated current scoreboard text.")

                save_needed = False # Flag to save data at the end if pins were removed

                for chat_id_str in pinned_chats_copy:
                    # Double-check if the chat ID still exists in the main dictionary *before* processing
                    if chat_id_str not in pinned_messages:
                        logger.debug(f"Skipping chat {chat_id_str} update, removed during cycle.")
                        continue

                    message_id = pinned_messages[chat_id_str]
                    chat_id = None # Initialize chat_id

                    # --- Inner Try/Except for individual message update ---
                    try:
                        chat_id = int(chat_id_str) # Convert string key to integer chat ID
                        logger.debug(f"Attempting scoreboard update for chat {chat_id} (msg: {message_id})")
                        await bot.edit_message_text(
                            text=current_scoreboard_text,
                            chat_id=chat_id,
                            message_id=message_id,
                            disable_web_page_preview=True
                        )
                        logger.debug(f"Scoreboard updated successfully for chat {chat_id} (msg: {message_id})")
                        # Delay between successful edits to avoid hitting global limits
                        # --- FIX: Use correct config variable ---
                        delay_seconds = getattr(config, 'SCOREBOARD_UPDATE_DELAY_SECONDS', 0.7) # Default 0.7 if missing
                        await asyncio.sleep(delay_seconds)

                    except TelegramRetryAfter as e:
                         logger.warning(f"Scoreboard update rate limited for chat {chat_id} (msg: {message_id}). Retrying after {e.retry_after} seconds.");
                         await asyncio.sleep(e.retry_after + 1) # Wait slightly longer than requested
                    except TelegramBadRequest as e:
                        err_msg = str(e).lower()
                        if "message is not modified" in err_msg:
                            logger.debug(f"Scoreboard content unchanged for chat {chat_id}. Skipping edit.")
                        elif "message to edit not found" in err_msg or "message can't be edited" in err_msg:
                             logger.warning(f"Scoreboard message {message_id} not found or cannot be edited in chat {chat_id}. Removing pin tracking.")
                             if chat_id_str in pinned_messages: del pinned_messages[chat_id_str]; save_needed = True
                        elif "chat not found" in err_msg or "peer_id_invalid" in err_msg:
                             logger.warning(f"Chat {chat_id} not found for scoreboard update. Removing pin tracking.")
                             if chat_id_str in pinned_messages: del pinned_messages[chat_id_str]; save_needed = True
                        elif "bot is not a member" in err_msg:
                             logger.warning(f"Bot is no longer a member of chat {chat_id}. Removing pin tracking.")
                             if chat_id_str in pinned_messages: del pinned_messages[chat_id_str]; save_needed = True
                        else:
                             logger.exception(f"Telegram BadRequest during scoreboard update for chat {chat_id} (msg:{message_id}): {e}")
                    except TelegramForbiddenError as e:
                         err_msg = str(e).lower()
                         if "bot was kicked" in err_msg or "user is deactivated" in err_msg or "bot is not a member" in err_msg or "chat was deleted" in err_msg:
                             logger.warning(f"Bot access forbidden in chat {chat_id} (kicked/deleted/not member?). Removing pin tracking. Error: {e}");
                             if chat_id_str in pinned_messages: del pinned_messages[chat_id_str]; save_needed = True
                         elif "rights" in err_msg or "not enough rights" in err_msg or "USER_BOT_PINNED_MESSAGES_DISABLED" in err_msg or "need administrator rights" in err_msg:
                             logger.warning(f"Bot lost permissions (edit/pin?) in chat {chat_id}. Removing pin tracking. Error: {e}");
                             if chat_id_str in pinned_messages: del pinned_messages[chat_id_str]; save_needed = True
                         else:
                              logger.exception(f"Telegram Forbidden error during scoreboard update for chat {chat_id} (msg:{message_id}): {e}");
                              if chat_id_str in pinned_messages: del pinned_messages[chat_id_str]; save_needed = True
                    except (TelegramNetworkError, TelegramAPIError) as e:
                         logger.error(f"Network/API Error updating scoreboard for chat {chat_id} (msg: {message_id}): {e}")
                    except ValueError as e: # Catch potential int conversion error
                         logger.error(f"Invalid chat ID format found in pinned_messages key: '{chat_id_str}'. Removing. Error: {e}")
                         if chat_id_str in pinned_messages: del pinned_messages[chat_id_str]; save_needed = True
                    except Exception as e:
                         logger.exception(f"Unexpected error during scoreboard update for chat {chat_id} (msg:{message_id}): {e}")
                    # --- End of inner Try/Except ---

                # Save data IF any pins were removed during the update cycle
                if save_needed:
                    logger.info("Saving pin data after removals during update cycle.")
                    await save_data()
                logger.info(f"Scoreboard update cycle finished.")

            else:
                logger.debug("No pinned scoreboards found to update in this cycle.")

        # --- End of Outer Task Loop Try/Except ---
        except Exception as e:
            logger.exception(f"Error in outer periodic_scoreboard_update loop: {e}")
            update_interval_seconds = 60 # Wait longer before retrying if the main loop fails

        # Wait for the configured interval before the next cycle
        logger.debug(f"Scoreboard updater sleeping for {update_interval_seconds} seconds.")
        await asyncio.sleep(update_interval_seconds)

async def periodic_market_data_update():
    """Background task to fetch market data and trigger notifications."""
    global http_session, last_gainers_post_time
    await asyncio.sleep(30); logger.info("Periodic market data fetcher task started.")
    while True:
        # --- Outer Task Loop ---
        update_interval_seconds = config.MARKET_DATA_UPDATE_INTERVAL_MINUTES * 60
        try:
            now = datetime.now(timezone.utc);
            rate_limit_hit = False;
            data_changed = False # Flag to track if save_data() is needed this cycle

            tracked_token_keys = list(token_cache.keys()) # Get tokens to check
            if not tracked_token_keys:
                logger.info("No tokens configured in token_cache, skipping market data fetch cycle.");
                await asyncio.sleep(update_interval_seconds);
                continue # Skip rest of the loop if no tokens

            logger.info(f"Starting market data fetch cycle for {len(tracked_token_keys)} tokens...")

            # --- Token Fetch Loop ---
            for token_key in tracked_token_keys:
                if rate_limit_hit:
                    logger.warning("Rate limit previously hit in this cycle, ending fetch loop early.")
                    break # Exit token loop if rate limit was hit

                contract_address = token_key.split('_')[0] if '_' in token_key else token_key

                # Ensure session exists and is open
                if not http_session or http_session.closed:
                    logger.warning("aiohttp session was closed or None. Recreating session for market data fetch.");
                    try:
                        # Close previous session just in case before recreating
                        if http_session: await http_session.close()
                    except Exception as close_err: logger.error(f"Error closing previous http_session: {close_err}")
                    http_session = aiohttp.ClientSession()

                new_data = None # Reset new_data for each token
                try:
                    new_data = await fetch_token_data_dexscreener(http_session, contract_address)
                except ConnectionError as e: # Catch rate limit error specifically
                    logger.error(f"Rate limit hit while fetching {token_key}: {e}. Pausing fetch cycle.");
                    rate_limit_hit = True;
                    break # Exit token loop immediately on rate limit
                except Exception as e:
                     # Catch other errors during fetch
                    logger.exception(f"Unhandled error fetching market data for {token_key}: {e}");
                    new_data = None # Ensure new_data is None on error

                # --- Process Fetched Data ---
                if new_data:
                    logger.debug(f"Successfully fetched data for {token_key}. Processing...")
                    # Check if data actually changed compared to cache to minimize writes
                    current_fetch_ts = new_data.get('fetch_timestamp')
                    cached_fetch_ts = token_cache.get(token_key, {}).get('fetch_timestamp')

                    # Update cache only if new data or timestamp differs
                    if token_key not in token_cache or current_fetch_ts != cached_fetch_ts:
                        logger.debug(f"Updating token_cache for {token_key} as data is new or changed.")
                        if token_key not in token_cache: token_cache[token_key] = {}
                        token_cache[token_key].update(new_data)
                        data_changed = True # Mark cache updated

                    # --- Get MCAP for Notifications ---
                    current_mcap = new_data.get('market_cap_usd')
                    if current_mcap is None or current_mcap <= 0:
                        logger.debug(f"Token {token_key} has invalid/zero MCAP ({current_mcap}). Skipping notification checks.")
                        continue # Skip notification checks if MCAP is invalid

                    # --- First MCAP Logic ---
                    if token_cache[token_key].get('first_mcap_usd') is None:
                         logger.info(f"Recorded first MCAP for {token_key}: ${current_mcap:,.0f}")
                         token_cache[token_key]['first_mcap_usd'] = current_mcap
                         token_cache[token_key]['first_mcap_timestamp'] = now.isoformat()
                         data_changed = True # Mark cache updated

                    # --- Pumping Notification Logic ---
                    first_mcap = token_cache[token_key].get('first_mcap_usd')
                    min_mcap_pump = config.MIN_MCAP_FOR_PUMP_NOTIF or 0
                    if first_mcap and first_mcap > 0 and current_mcap >= min_mcap_pump:
                        # Initialize state if needed
                        if token_key not in market_data_state:
                             logger.debug(f"Initializing market_data_state for {token_key}")
                             market_data_state[token_key] = {}; data_changed = True
                        state = market_data_state[token_key];
                        last_multiplier = state.get('last_notified_multiplier', 0);
                        # Avoid division by zero if first_mcap somehow becomes zero after check
                        current_multiplier_raw = (current_mcap / first_mcap) if first_mcap > 0 else 0
                        achieved_multiplier = 0
                        # Find highest achieved multiplier threshold
                        for threshold in sorted(config.PUMPING_MULTIPLIERS, reverse=True):
                             if current_multiplier_raw >= threshold:
                                 achieved_multiplier = threshold; break
                        # Check if achieved multiplier is higher than last notified
                        if achieved_multiplier > last_multiplier:
                             logger.info(f"Potential pumping trigger for {token_key}: Achieved {achieved_multiplier}x (last notified: {last_multiplier}x)")
                             # Check notification cooldown
                             last_notif_ts_str = state.get('last_pump_notif_ts'); can_notify = True
                             if last_notif_ts_str:
                                 try:
                                     last_notif_dt = datetime.fromisoformat(last_notif_ts_str);
                                     if last_notif_dt.tzinfo is None: last_notif_dt = last_notif_dt.replace(tzinfo=timezone.utc)
                                     cooldown_duration = timedelta(minutes=config.PUMPING_NOTIF_COOLDOWN_MINUTES)
                                     if now < last_notif_dt + cooldown_duration:
                                         can_notify = False
                                         logger.info(f"Pumping trigger {token_key} ({achieved_multiplier}x) skipped due to cooldown (last: {last_notif_ts_str}).")
                                 except ValueError: logger.warning(f"Invalid pump notification timestamp format in state for {token_key}: {last_notif_ts_str}")
                             # Send notification if allowed
                             if can_notify:
                                 logger.info(f"PUMPING notification triggered for {token_key}: {achieved_multiplier}x MCAP reached.");
                                 # --- FIX: Added await ---
                                 await send_pumping_notification(token_key, achieved_multiplier, current_mcap, first_mcap);
                                 # Update state after successful notification attempt
                                 state['last_notified_multiplier'] = achieved_multiplier
                                 state['last_pump_notif_ts'] = now.isoformat()
                                 data_changed = True # Mark state changed

                    # --- Leaderboard Entry Notification Logic ---
                    trending_ranks = {key: rank+1 for rank, (key, _) in enumerate(get_trending_tokens(config.TRENDING_WINDOW_HOURS))};
                    current_rank = trending_ranks.get(token_key) # Get current rank by votes

                    if current_rank and current_rank <= config.SCOREBOARD_TOP_N:
                        logger.debug(f"Token {token_key} is currently ranked #{current_rank} in trending.")
                        # Initialize state if needed
                        if token_key not in market_data_state:
                             logger.debug(f"Initializing market_data_state for {token_key}")
                             market_data_state[token_key] = {}; data_changed = True
                        state = market_data_state[token_key];
                        previous_rank = state.get('previous_vote_rank') # Check previous *vote* rank stored
                        logger.debug(f"Previous stored rank for {token_key}: {previous_rank}")

                        # Trigger if previously unranked OR ranked outside the top N
                        if previous_rank is None or previous_rank > config.SCOREBOARD_TOP_N:
                            logger.info(f"Potential leaderboard entry trigger for {token_key}: Entered rank {current_rank} (previously {previous_rank})")
                            # Check notification cooldown for entry
                            last_entry_ts_str = state.get('last_entry_notif_ts'); can_notify_entry = True
                            if last_entry_ts_str:
                                try:
                                    last_entry_dt = datetime.fromisoformat(last_entry_ts_str);
                                    if last_entry_dt.tzinfo is None: last_entry_dt = last_entry_dt.replace(tzinfo=timezone.utc)
                                    entry_cooldown_duration = timedelta(minutes=config.LEADERBOARD_ENTRY_NOTIF_COOLDOWN_MINUTES)
                                    if now < last_entry_dt + entry_cooldown_duration:
                                        can_notify_entry = False
                                        logger.info(f"Leaderboard entry trigger {token_key} (rank {current_rank}) skipped due to cooldown (last: {last_entry_ts_str}).")
                                except ValueError: logger.warning(f"Invalid entry notification timestamp format in state for {token_key}: {last_entry_ts_str}")
                            # Send notification if allowed
                            if can_notify_entry:
                                 logger.info(f"LEADERBOARD ENTRY notification triggered for {token_key}: Entered at rank #{current_rank}.");
                                 # --- FIX: Added await ---
                                 await send_leaderboard_entry_notification(token_key, current_rank, current_mcap);
                                 # Update state after successful notification attempt
                                 state['last_entry_notif_ts'] = now.isoformat()
                                 data_changed = True # Mark state changed

                        # Always update the stored previous rank if it has changed
                        if state.get('previous_vote_rank') != current_rank:
                            logger.debug(f"Updating previous rank for {token_key} from {state.get('previous_vote_rank')} to {current_rank}")
                            state['previous_vote_rank'] = current_rank
                            data_changed = True # Mark state changed
                    else:
                        # If currently NOT ranked in top N, clear the previous rank in state if it was set
                        if token_key in market_data_state and market_data_state[token_key].get('previous_vote_rank') is not None:
                             logger.debug(f"Token {token_key} dropped out of top {config.SCOREBOARD_TOP_N}. Clearing previous rank.")
                             market_data_state[token_key]['previous_vote_rank'] = None
                             data_changed = True # Mark state changed

                # --- Delay Between API Calls ---
                api_delay = getattr(config, 'MARKET_API_DELAY_SECONDS', 1.1) # Default 1.1 if missing
                await asyncio.sleep(api_delay)

            # --- End of Token Fetch Loop ---
            logger.info("Finished fetching data for all tracked tokens in this cycle.")

            # --- Biggest Gainers Post Logic (after token loop) ---
            gainers_posted_this_cycle = False # Track if gainers post saved data
            if not rate_limit_hit: # Only consider posting if the fetch cycle wasn't interrupted
                can_post_gainers = False
                # Check if feature is enabled using getattr for safety
                if getattr(config, 'ENABLE_BIGGEST_GAINERS_POST', False):
                    logger.debug("Checking if Biggest Gainers post should be sent.")
                    if last_gainers_post_time is None: # Post if never posted before
                        logger.info("Posting gainers: Never posted before.")
                        can_post_gainers = True
                    else:
                        # Check if interval has passed since last post
                        interval_duration = timedelta(minutes=config.BIGGEST_GAINERS_POST_INTERVAL_MINUTES)
                        time_since_last = now - last_gainers_post_time
                        logger.debug(f"Time since last gainers post: {time_since_last}. Interval required: {interval_duration}")
                        if time_since_last >= interval_duration:
                            logger.info(f"Posting gainers: Interval of {interval_duration} passed.")
                            can_post_gainers = True
                        else:
                             logger.debug("Skipping gainers post: Interval not yet passed.")
                else:
                     logger.debug("Biggest Gainers post feature is disabled in config.")

                if can_post_gainers:
                     try:
                         # --- FIX: Added await ---
                         await send_biggest_gainers_post() # This function saves data if it posts
                         gainers_posted_this_cycle = True # Assume save happened if no error
                     except Exception as gainer_err:
                          logger.exception(f"Error occurred during send_biggest_gainers_post: {gainer_err}")
            else:
                 logger.info("Skipping Biggest Gainers post check due to rate limit hit during fetch cycle.")

            # --- Save Data if Changed During Cycle AND NOT saved by gainers post ---
            if data_changed and not gainers_posted_this_cycle:
                 logger.info("Saving updated market data/state after fetch cycle...")
                 await save_data()
            elif data_changed and gainers_posted_this_cycle:
                logger.info("Data changes occurred, but assuming send_biggest_gainers_post handled the save for this cycle.")
            elif not data_changed:
                logger.debug("No data changes detected in market data/state during this cycle, skipping final save.")

            logger.info("Market data update cycle finished.")

        # --- End of Outer Task Loop Try/Except ---
        except Exception as e:
            logger.exception(f"Critical error in periodic_market_data_update main loop: {e}")
            update_interval_seconds = 120 # Wait longer before retrying the whole loop
            logger.error(f"Pausing market data task for {update_interval_seconds}s due to loop error.")

        # Wait for the next full interval
        logger.debug(f"Market data task sleeping for {update_interval_seconds} seconds.")
        await asyncio.sleep(update_interval_seconds)

# --- Minigame Handlers --- (Keep as is)
async def process_minigame(message: Message, emoji: str):
    """Handles the logic for bowling and darts minigames."""
    user = message.from_user; chat_id = message.chat.id;
    chat_id_str = str(chat_id); user_id_str = str(user.id);
    user_name = user.username if user.username else user.first_name # Use first name as fallback
    logger.info(f"User {user.id} ({user_name}) initiated minigame ({emoji}) in chat {chat_id}")

    # 1. Check if group is configured and active
    group_data = group_configs.get(chat_id_str)
    if not group_data or not group_data.get("config", {}).get("setup_complete") or not group_data.get("config", {}).get("is_active"):
        logger.warning(f"Minigame attempt in unconfigured/inactive group {chat_id_str} by user {user.id}")
        await message.reply("Minigames can only be played in groups where the bot is fully configured and active."); return

    # 2. Get the token key for the reward
    token_key = group_data.get("token_key");
    if not token_key:
        logger.error(f"Minigame attempt in configured group {chat_id_str} failed: Missing token_key in config.");
        await message.reply("Configuration error: Cannot determine which token reward to give. Please contact an admin."); return

    # 3. Check minigame cooldown for the user
    logger.debug(f"Checking minigame cooldown for user {user.id}")
    cooldown = get_minigame_cooldown_remaining(user.id); # Helper function handles cleanup
    if cooldown:
        await message.reply(f"⏳ Easy there! You can play the minigame again in <b>{format_timedelta(cooldown)}</b>."); return

    # 4. Send the dice animation
    try:
        logger.debug(f"Sending dice {emoji} to chat {chat_id} for user {user.id}")
        # Reply to the user's command message to keep context
        sent_dice_msg = await bot.send_dice(chat_id=chat_id, emoji=emoji, reply_to_message_id=message.message_id);
        dice_value = sent_dice_msg.dice.value # Get the result value (1-6 for bowl/darts)
        logger.info(f"User {user.id} rolled {emoji} in chat {chat_id}, result: {dice_value}")
    except Exception as e:
        logger.exception(f"Failed to send dice emoji {emoji} for user {user.id} in chat {chat_id}: {e}");
        await message.reply(f"Oops! Something went wrong trying to roll the {emoji}. Please try again later."); return

    # 5. Determine win condition and reward
    win_value = config.MINIGAME_WIN_VALUES.get(emoji);
    if win_value is None:
        logger.error(f"Minigame configuration error: No win value defined for emoji '{emoji}' in config.MINIGAME_WIN_VALUES");
        await message.reply("Internal configuration error for this minigame. Cannot determine win condition."); return

    is_win = (dice_value == win_value);
    reward_message = "" # Initialize reward message part
    data_changed = False # Flag if save needed

    if is_win:
        # --- Handle Win ---
        logger.info(f"User {user.id} WON the minigame ({emoji})!")
        # Increment free vote count
        if user_id_str not in user_free_votes: user_free_votes[user_id_str] = {}; data_changed = True
        if token_key not in user_free_votes[user_id_str]: user_free_votes[user_id_str][token_key] = 0; data_changed = True
        # Only change data if count actually increases
        if config.FREE_VOTE_REWARD_COUNT > 0:
             user_free_votes[user_id_str][token_key] += config.FREE_VOTE_REWARD_COUNT
             data_changed = True

        # Format win message
        try: token_display = await get_token_display_info(token_key);
        except: token_display = f"Token ({token_key[:6]}...)"
        win_desc = "Strike! 🎳" if emoji == "🎳" else "Bullseye! 🎯";
        reward_message = f"\n🎉 <b>{win_desc}</b> Congratulations, {user_name}! You won <b>{config.FREE_VOTE_REWARD_COUNT}</b> free vote(s) for {token_display}!";
        logger.info(f"User {user.id} won minigame ({emoji}) in chat {chat_id}, earning {config.FREE_VOTE_REWARD_COUNT} free votes for {token_key}.")
    else:
        # --- Handle Loss ---
        logger.info(f"User {user.id} lost the minigame ({emoji}). Roll: {dice_value}, Needed: {win_value}")
        lose_desc = "Close!" # Default loss message
        if emoji == "🎳": lose_desc = "Gutter ball!" if dice_value == 1 else "So close!"
        elif emoji == "🎯": lose_desc = "Missed!" if dice_value == 1 else "Almost!"
        reward_message = f"\n😅 {lose_desc} Nice try, {user_name}! No win this time."

    # 6. Set cooldown (always happens after playing)
    logger.debug(f"Setting minigame cooldown for user {user.id}")
    minigame_cooldowns[user_id_str] = datetime.now(timezone.utc).isoformat();
    data_changed = True # Cooldown always changes data

    # Save data IF needed (win occurred or cooldown set - always true here)
    if data_changed:
        logger.debug("Saving minigame results (cooldown and/or free votes).")
        await save_data()

    # 7. Send the result message including cooldown info
    cooldown_notice = f"\nYou can play again in {config.MINIGAME_COOLDOWN_HOURS} hours.";
    # Append cooldown notice to the win/loss message
    full_reply = f"{reward_message.strip()}\n{cooldown_notice}" # Ensure single newline
    try:
        await message.reply(full_reply) # Reply to the original command
    except Exception as e:
         logger.error(f"Failed to send minigame result reply to chat {chat_id}: {e}")

@router.message(Command("bowl"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_bowl_command(message: Message):
    """Handles the /bowl command."""
    await process_minigame(message, "🎳")

@router.message(Command("darts"), F.chat.type.in_([ChatType.GROUP, ChatType.SUPERGROUP]))
async def handle_darts_command(message: Message):
    """Handles the /darts command."""
    await process_minigame(message, "🎯")


# --- Main Execution ---
async def main():
    global http_session
    logger.info("Initializing bot...")
    # Initialize aiohttp session at startup
    http_session = aiohttp.ClientSession()
    logger.info("aiohttp session initialized.")

    # Load persistent data
    logger.info("Loading data from files...")
    await load_data()
    logger.info("Data loading complete.")

    # Include the router containing all handlers
    dp.include_router(router)
    logger.info("Router included in dispatcher.")

    # Start background tasks
    logger.info("Creating background tasks...")
    # Ensure tasks are created with names for better logging during shutdown
    save_task = asyncio.create_task(periodic_save(), name="PeriodicSaveTask")
    scoreboard_update_task = asyncio.create_task(periodic_scoreboard_update(), name="ScoreboardUpdateTask")
    market_data_task = asyncio.create_task(periodic_market_data_update(), name="MarketDataUpdateTask")
    background_tasks = [save_task, scoreboard_update_task, market_data_task]
    logger.info(f"Background tasks created: {[t.get_name() for t in background_tasks if t]}")

    # Log warnings about storage types if applicable
    if isinstance(storage, MemoryStorage):
        logger.warning("="*60+"\nWARNING: Using MemoryStorage for FSM state! Suitable only for testing, state will be lost on restart. Use RedisStorage or MongoStorage for production.\n"+"="*60)
    logger.warning("="*60+"\nNOTE: Using JSON files for data persistence. Simple but may face performance/concurrency issues at scale. Consider a database (e.g., Redis, PostgreSQL+asyncpg) for production.\n"+"="*60)

    logger.info("Bot starting polling...")
    try:
        # Log bot info before starting
        bot_info = await bot.get_me();
        logger.info(f"--- Bot Info ---")
        logger.info(f" Name:     {bot_info.full_name}")
        logger.info(f" ID:       {bot_info.id}")
        logger.info(f" Username: @{bot_info.username}")
        logger.info(f"----------------")
        # Start polling
        await dp.start_polling(bot)
    except Exception as e:
        logger.exception(f"CRITICAL ERROR during bot polling startup or execution: {e}")
    finally:
        # --- Shutdown Sequence ---
        logger.info("Initiating bot shutdown sequence...")

        # 1. Gracefully cancel background tasks
        logger.info("Cancelling background tasks...")
        for task in background_tasks:
             # Check if task exists and is not already done
             if task and not task.done():
                try:
                     task.cancel()
                     logger.debug(f"Cancellation requested for task: {task.get_name()}")
                except Exception as e:
                     # Log error during cancellation request but continue
                     task_name = task.get_name() if hasattr(task, 'get_name') else 'Unknown Task'
                     logger.error(f"Error requesting cancellation for task {task_name}: {e}")

        # 2. Wait for tasks to finish cancellation/execution
        if background_tasks:
             logger.info("Waiting for background tasks to finish...")
             # Use asyncio.gather to wait for all tasks and capture results/exceptions
             results = await asyncio.gather(*[t for t in background_tasks if t], return_exceptions=True)
             logger.info("Gathering background task results after cancellation request...")
             for i, result in enumerate(results):
                 # Check index boundary before accessing task
                 if i < len(background_tasks):
                     current_task = background_tasks[i]
                     if current_task:
                         task_name = current_task.get_name() if hasattr(current_task, 'get_name') else f"Task-{i}"
                         if isinstance(result, asyncio.CancelledError):
                             logger.info(f"Background task '{task_name}' successfully cancelled.")
                         elif isinstance(result, Exception):
                             # Log exceptions that occurred *during* task execution or *during* cancellation handling
                             logger.exception(f"Exception occurred in background task '{task_name}' during execution or shutdown: {result}")
                         else:
                             # Log normal completion if it happened before cancellation took effect
                             logger.info(f"Background task '{task_name}' finished normally with result: {result}")
                 else:
                      logger.error(f"Result index {i} out of bounds for background tasks (length {len(background_tasks)})")
             logger.info("Finished waiting for background tasks.")

        # 3. Close aiohttp session
        if http_session and not http_session.closed:
            logger.info("Closing aiohttp client session...")
            await http_session.close();
            logger.info("aiohttp client session closed.")
        else:
            logger.info("aiohttp client session already closed or was None.")

        # 4. Perform final data save
        logger.info("Performing final data save before shutdown...");
        try:
            await save_data(); # Ensure final state is written
            logger.info("Final data save completed.")
        except Exception as e:
            logger.exception(f"CRITICAL ERROR during final data save: {e}")

        # 5. Close the bot session
        logger.info("Closing bot session...")
        try:
            # Use the bot's session attribute directly if available (more reliable in aiogram 3)
            if hasattr(bot, 'session') and bot.session and not bot.session.closed:
                await bot.session.close();
                logger.info("Bot session closed successfully.")
            else:
                 logger.info("Bot session seems already closed or not available.")
        except Exception as e:
            logger.exception(f"Error occurred while closing bot session: {e}")

        logger.info("------ Bot shutdown sequence complete ------")

if __name__ == "__main__":
    # Configure logging at the entry point
    # Adjust level (e.g., logging.DEBUG for more detail) and format as needed
    log_level = logging.INFO # Change to DEBUG for development
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s',
        # Optional: Add file logging
        # handlers=[
        #     logging.StreamHandler(),
        #     logging.FileHandler("bot.log", mode='a', encoding='utf-8')
        # ]
    )
    # Suppress noisy logs from libraries if needed
    logging.getLogger("aiogram.event").setLevel(logging.INFO) # Keep INFO for event handling
    logging.getLogger("aiogram.dispatcher").setLevel(logging.INFO) # Keep INFO for dispatcher flow
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


    logger.info(f"Starting bot script execution (Log Level: {logging.getLevelName(log_level)})...")
    try:
        # Run the main asynchronous function
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped manually (KeyboardInterrupt/SystemExit received).")
    except Exception as e:
        # Catch any unexpected errors right at the top level during startup
        logger.exception("Unhandled exception during bot execution in __main__:")
    finally:
         logger.info("Bot script finished.")
