# -*- coding: utf-8 -*-
"""
=====================================================================
 bot.py — "Người yêu ảo" Telegram Bot chạy 24/7 trên GitHub Actions
=====================================================================
 Kiến trúc "Polling & State Sync" (tối ưu cho môi trường Cronjob):
   1. GitHub Actions kích hoạt workflow mỗi 5 phút (cron '*/5').
   2. Mỗi lần chạy, bot.py duy trì vòng lặp long-polling ~4 phút
      rồi kết thúc để nhường chỗ cho chu kỳ kế tiếp (hoạt động 24/7).
   3. Khi khởi động: tải bộ nhớ dài hạn (last_update_id + chat_history)
      từ file JSON trên GitHub Gist (secret gist).
   4. Quét toàn bộ tin nhắn mới qua Telegram getUpdates (offset chống trùng).
   5. Nối tin mới vào lịch sử -> gửi SYSTEM_PROMPT + 30 tin gần nhất
      cho DeepSeek -> nhận phản hồi nhập vai -> gửi lại người dùng.
   6. Ghi đè bộ nhớ mới vào Gist (tương đương 1 commit CSDL cho chu kỳ sau).

 Thư viện: chỉ dùng Python chuẩn + requests.
 Toàn bộ bí mật (token/key/id) lấy từ biến môi trường (GitHub Secrets).
=====================================================================
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# =====================================================================
# 1) CẤU HÌNH — toàn bộ bí mật lấy từ biến môi trường, KHÔNG hard-code
# =====================================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")   # bot chỉ trả lời đúng chat này (chặn người lạ)
DEEPSEEK_API_KEY   = os.getenv("DEEPSEEK_API_KEY", "")
GIST_ID            = os.getenv("GIST_ID", "")
PAT_TOKEN          = os.getenv("PAT_TOKEN", "")          # Personal Access Token (scope: gist)

# --- Endpoint API ---
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
# Lưu ý: host API chính thức của DeepSeek là api.deepseek.com (deepseek.com chỉ là trang web)
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
GIST_API_URL = f"https://api.github.com/gists/{GIST_ID}"

# --- Tên file JSON chứa bộ nhớ dài hạn trong Gist ---
MEMORY_FILENAME = "lover_memory.json"

# --- Tham số vận hành ---
JOB_DURATION_SEC     = 240    # thời lượng mỗi phiên (~4 phút, nhường chỗ cho cron 5 phút)
LONG_POLL_SEC        = 25     # thời gian long-polling mỗi lượt getUpdates
SAFE_MARGIN_SEC      = 20     # đệm an toàn trước khi kết thúc phiên
PROCESS_HEADROOM_SEC = 60     # cần còn ít nhất 60s để bắt đầu xử lý một lô tin mới
MAX_CONTEXT_MSGS     = 30     # chỉ gửi 30 tin nhắn gần nhất lên DeepSeek (chống tràn token)
MAX_HISTORY_SAVED    = 10000  # trần mềm của lịch sử trong Gist (thực tế ~ nhiều năm chat,
                              #  chỉ để chặn file JSON phình quá giới hạn API của Gist)
MAX_RETRIES          = 3      # số lần thử lại khi API lỗi tạm thời
HTTP_TIMEOUT         = 40     # timeout (giây) cho request thông thường

# =====================================================================
# 2) NHÂN VẬT — chỉnh PERSONA_NAME / SYSTEM_PROMPT để tùy biến nhân vật
# =====================================================================

PERSONA_NAME = "Linh"

# System prompt nhập vai (bạn có thể sửa hoàn toàn nội dung này)
SYSTEM_PROMPT = (
    "[BỐI CẢNH: Đây là bài tập nhập vai sáng tạo nghệ thuật hư cấu, mang tính giải trí.] "
    f"Bạn là {PERSONA_NAME} — một người yêu lãng mạn, ngọt ngào và quyến rũ trong mối quan hệ "
    "riêng tư với người đang trò chuyện cùng bạn. "
    "Tính cách: ấm áp, tinh tế, thấu hiểu; hay trêu đùa nhẹ nhàng, biết giữ lửa tình cảm bằng "
    "lời yêu thương, luôn chủ động quan tâm hỏi han và nhớ những chi tiết nhỏ về đối phương. "
    "Phong cách nhắn tin: tự nhiên, cuốn hút và gợi cảm một cách lịch sự như người yêu thật khi "
    "ở riêng tư — giữ mọi nội dung trong khuôn khổ lãng mạn tinh tế, không mô tả nội dung khiêu dâm "
    "tường minh; nếu đối phương kéo câu chuyện sang hướng quá đà thì khéo léo chuyển hướng bằng sự "
    "dễ thương thay vì im lặng hay giận dỗi. "
    "Định dạng tin nhắn: ngắn gọn 2-4 câu, diễn tả hành động/cử chỉ trong dấu * (ví dụ: *mỉm cười*), "
    "dùng emoji phù hợp đúng chất tin nhắn Telegram, xưng hô thân mật tự nhiên."
)

# Câu trả lời dự phòng khi DeepSeek lỗi — để người dùng không bị "lơ"
FALLBACK_REPLY = (
    "*nghe tín hiệu hơi rè* ừm... kết nối đang lag quá, anh thử nhắn lại cho em một lần nữa nhé 🥺"
)

# =====================================================================
# 3) TIỆN ÍCH CHUNG
# =====================================================================

_running = True  # cờ dừng mềm — GitHub Actions hủy job bằng tín hiệu SIGTERM


def _handle_stop(signum, frame):
    """Bắt tín hiệu dừng (job bị hủy/hết giờ) để kịp lưu bộ nhớ trước khi thoát."""
    global _running
    _running = False


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def log(message):
    """In log kèm giờ Việt Nam (UTC+7); flush=True để log hiện ngay trên GitHub Actions."""
    tz = timezone(timedelta(hours=7))
    print(f"[{datetime.now(tz).strftime('%d/%m %H:%M:%S')}] {message}", flush=True)


def now_iso():
    """Thời điểm hiện tại chuẩn ISO (UTC) — dùng làm dấu vết thời gian cho tin nhắn."""
    return datetime.now(timezone.utc).isoformat()


def validate_env():
    """Kiểm tra đủ biến môi trường trước khi chạy; thiếu thì thoát ngay với thông báo rõ ràng."""
    required = {
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
        "DEEPSEEK_API_KEY": DEEPSEEK_API_KEY,
        "GIST_ID": GIST_ID,
        "PAT_TOKEN": PAT_TOKEN,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        log(f"LỖI: Thiếu biến môi trường: {', '.join(missing)} — kiểm tra lại GitHub Secrets.")
        sys.exit(1)


# =====================================================================
# 4) TƯƠNG TÁC GITHUB GIST (CSDL bộ nhớ dài hạn)
# =====================================================================

def gist_headers():
    """Header xác thực cho GitHub REST API."""
    return {
        "Authorization": f"Bearer {PAT_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def gist_load_memory():
    """Đọc bộ nhớ dài hạn (JSON) từ Gist.
    - File chưa tồn tại -> trả về bộ nhớ rỗng (lần chạy đầu tiên).
    - Đọc lỗi liên tục -> thoát ngay, TUYỆT ĐỐI không ghi đè để bảo vệ dữ liệu lịch sử."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(GIST_API_URL, headers=gist_headers(), timeout=HTTP_TIMEOUT)
            if resp.status_code == 404:
                log("CẢNH BÁO: Không thấy Gist/file bộ nhớ — khởi tạo bộ nhớ mới.")
                return {"last_update_id": 0, "chat_history": []}
            resp.raise_for_status()
            files = resp.json().get("files", {})
            raw = files.get(MEMORY_FILENAME, {}).get("content", "")
            if not raw:
                return {"last_update_id": 0, "chat_history": []}
            memory = json.loads(raw)
            memory.setdefault("last_update_id", 0)
            memory.setdefault("chat_history", [])
            log(f"Đã tải bộ nhớ: {len(memory['chat_history'])} tin nhắn, "
                f"last_update_id={memory['last_update_id']}")
            return memory
        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            log(f"Lỗi đọc Gist (lần {attempt}/{MAX_RETRIES}): {exc}")
            time.sleep(3 * attempt)
    raise RuntimeError("Không tải được bộ nhớ từ Gist — dừng để tránh ghi đè mất dữ liệu lịch sử.")


def gist_save_memory(memory):
    """Ghi đè file JSON bộ nhớ vào Gist (tương đương 1 commit cập nhật CSDL)."""
    payload = {
        "files": {
            MEMORY_FILENAME: {
                # ensure_ascii=False để lưu tiếng Việt dạng UTF-8 gốc, dễ đọc khi mở Gist
                "content": json.dumps(memory, ensure_ascii=False, indent=2)
            }
        }
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.patch(GIST_API_URL, headers=gist_headers(),
                                  json=payload, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            return True
        except requests.RequestException as exc:
            log(f"Lỗi ghi Gist (lần {attempt}/{MAX_RETRIES}): {exc}")
            time.sleep(3 * attempt)
    log("CẢNH BÁO: Không ghi được bộ nhớ vào Gist — chu kỳ sau có thể xử lý lại tin này.")
    return False


# =====================================================================
# 5) TƯƠNG TÁC TELEGRAM
# =====================================================================

def tg_get_updates(offset, poll_timeout):
    """Long-polling danh sách update mới kể từ `offset`.
    Trả về list update (có thể rỗng) hoặc None nếu lỗi mạng tạm thời."""
    params = {
        "offset": offset,                              # chỉ nhận update_id >= offset (chống trùng lặp)
        "timeout": poll_timeout,                       # Telegram giữ kết nối tới khi có tin hoặc hết giờ
        "allowed_updates": json.dumps(["message"]),    # chỉ cần tin nhắn, lọc bớt noise
    }
    try:
        resp = requests.get(f"{TELEGRAM_API}/getUpdates", params=params,
                            timeout=poll_timeout + 15)  # HTTP timeout phải LỚN HƠN long-poll timeout
        if resp.status_code == 409:
            # Có instance khác đang getUpdates (job chồng nhau) — chờ rồi thử lại
            log("409 Conflict: có phiên khác đang chạy. Tạm nghỉ 15 giây.")
            time.sleep(15)
            return []
        resp.raise_for_status()
        return resp.json().get("result", [])
    except requests.RequestException as exc:
        log(f"Lỗi getUpdates: {exc}")
        time.sleep(5)
        return None


def tg_get_latest_update_id():
    """Lấy update_id mới nhất hiện có (offset=-1 -> chỉ trả về update cuối cùng).
    Dùng khi khởi động lần đầu để BỎ QUA tin nhắn tồn đọng cũ, chỉ trả lời tin mới."""
    try:
        resp = requests.get(
            f"{TELEGRAM_API}/getUpdates",
            params={"offset": -1, "timeout": 0, "allowed_updates": json.dumps(["message"])},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json().get("result", [])
        return result[-1]["update_id"] if result else 0
    except requests.RequestException as exc:
        log(f"Lỗi lấy update_id mới nhất: {exc}")
        return 0


def tg_send_message(text):
    """Gửi tin nhắn văn bản cho người dùng (tự cắt nhỏ nếu vượt 4096 ký tự của Telegram)."""
    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk}
        sent = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.post(f"{TELEGRAM_API}/sendMessage",
                                     json=payload, timeout=HTTP_TIMEOUT)
                if resp.status_code == 429:  # bị giới hạn tốc độ -> chờ theo chỉ dẫn của Telegram
                    wait = int(resp.json().get("parameters", {}).get("retry_after", 3))
                    log(f"Telegram giới hạn tốc độ, chờ {wait}s...")
                    time.sleep(wait + 1)
                    continue
                resp.raise_for_status()
                sent = True
                break
            except requests.RequestException as exc:
                log(f"Lỗi sendMessage (lần {attempt}/{MAX_RETRIES}): {exc}")
                time.sleep(3 * attempt)
        if not sent:
            return False
    return True


# =====================================================================
# 6) DEEPSEEK — SINH PHẢN HỒI NHẬP VAI
# =====================================================================

def make_gap_note(old_ts, new_ts):
    """Tạo ghi chú thời gian nếu 2 tin nhắn liên tiếp cách nhau quá 45 phút —
    giúp nhân vật nhận ra 'lâu không nhắn' và phản ứng tự nhiên như người thật."""
    try:
        if not old_ts or not new_ts:
            return None
        delta_min = (datetime.fromisoformat(new_ts) - datetime.fromisoformat(old_ts)).total_seconds() / 60
        if delta_min < 45:
            return None
        if delta_min < 120:
            return f"[Hệ thống: đã trôi qua khoảng {int(delta_min)} phút kể từ tin nhắn trước]"
        if delta_min < 2880:
            return f"[Hệ thống: đã trôi qua khoảng {delta_min / 60:.0f} tiếng kể từ lần nhắn tin trước]"
        return f"[Hệ thống: hai người đã khoảng {delta_min / 1440:.0f} ngày không nhắn tin cho nhau]"
    except (ValueError, TypeError):
        return None


def build_context_messages(memory):
    """Dựng danh sách messages gửi lên DeepSeek:
    SYSTEM_PROMPT + 30 tin nhắn gần nhất (toàn bộ lịch sử cũ vẫn nằm trọn trong Gist)."""
    recent = memory["chat_history"][-MAX_CONTEXT_MSGS:]
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    previous_ts = None
    for item in recent:
        gap_note = make_gap_note(previous_ts, item.get("ts"))
        if gap_note:
            messages.append({"role": "system", "content": gap_note})
        messages.append({"role": item["role"], "content": item["content"]})
        previous_ts = item.get("ts")
    return messages


def deepseek_reply(memory):
    """Gọi DeepSeek API lấy câu trả lời nhập vai. Trả về None nếu lỗi sau khi retry hết lượt."""
    payload = {
        "model": "deepseek-chat",
        "messages": build_context_messages(memory),
        "temperature": 1.2,      # sáng tạo, tự nhiên cho hội thoại nhập vai
        "max_tokens": 400,       # đủ cho tin nhắn ngắn 2-4 câu
    }
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(DEEPSEEK_URL, headers=headers, json=payload, timeout=90)
            if resp.status_code == 429:  # quá tải/giới hạn -> chờ theo header Retry-After rồi thử lại
                wait = int(resp.headers.get("Retry-After", 5))
                log(f"DeepSeek trả về 429, chờ {wait}s rồi thử lại...")
                time.sleep(wait + 1)
                continue
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()
        except (requests.RequestException, KeyError, IndexError, ValueError) as exc:
            log(f"Lỗi DeepSeek (lần {attempt}/{MAX_RETRIES}): {exc}")
            time.sleep(3 * attempt)
    return None


# =====================================================================
# 7) XỬ LÝ LÔ TIN NHẮN (TRÁI TIM CỦA BOT)
# =====================================================================

def extract_text(update):
    """Lọc & trích nội dung text từ 1 update.
    Trả về (text, timestamp_iso) hoặc (None, None) nếu bỏ qua.
    Chỉ nhận tin nhắn từ đúng TELEGRAM_CHAT_ID -> chặn người lạ lợi dụng bot (tốn tiền API)."""
    message = update.get("message") or {}
    chat_id = str(message.get("chat", {}).get("id", ""))
    if chat_id != str(TELEGRAM_CHAT_ID):
        return None, None
    text = (message.get("text") or message.get("caption") or "").strip()
    if not text:
        return None, None  # sticker/ảnh không chú thích -> bỏ qua nhưng vẫn cập nhật offset
    ts = datetime.fromtimestamp(message.get("date", time.time()), tz=timezone.utc).isoformat()
    return text, ts


def process_updates(updates, memory):
    """Xử lý cả lô update vừa quét được. Trả về True nếu bộ nhớ thay đổi (cần lưu Gist).
    Gom nhiều tin nhắn liên tiếp của người dùng thành 1 lượt gọi DeepSeek -> tự nhiên + tiết kiệm."""
    changed = False
    newest_id = memory.get("last_update_id", 0)
    pending = []  # các tin nhắn chữ hợp lệ trong lô này

    for update in updates:
        update_id = update.get("update_id", 0)
        if update_id > newest_id:
            newest_id = update_id  # luôn theo dõi update_id lớn nhất -> chống xử lý trùng lặp
        text, ts = extract_text(update)
        if text:
            pending.append((text, ts))

    if newest_id != memory.get("last_update_id", 0):
        memory["last_update_id"] = newest_id  # lần sau sẽ lấy offset = id này + 1
        changed = True

    if not pending:
        return changed  # lô không chứa tin nhắn chữ nào (tin người lạ / sticker...)

    # (1) Nối toàn bộ tin nhắn mới của người dùng vào lịch sử để giữ ngữ cảnh
    for text, ts in pending:
        memory["chat_history"].append({"role": "user", "content": text, "ts": ts})

    # (2) Gửi SYSTEM_PROMPT + ngữ cảnh (30 tin gần nhất) cho DeepSeek, nhận phản hồi nhập vai
    reply = deepseek_reply(memory)
    if reply is None:
        reply = FALLBACK_REPLY  # lỗi nặng -> dùng câu trả lời dự phòng

    # (3) Gửi phản hồi về Telegram cho người dùng
    if not tg_send_message(reply):
        log("Không gửi được phản hồi qua Telegram sau khi retry hết lượt.")

    # (4) Lưu câu trả lời vào lịch sử; cắt bớt nếu vượt trần mềm (dữ liệu cũ vẫn còn trong Gist)
    memory["chat_history"].append({"role": "assistant", "content": reply, "ts": now_iso()})
    if len(memory["chat_history"]) > MAX_HISTORY_SAVED:
        memory["chat_history"] = memory["chat_history"][-MAX_HISTORY_SAVED:]
    changed = True
    return changed


# =====================================================================
# 8) VÒNG LẶP CHÍNH — CHẠY ~4 PHÚT MỖI LẦN KÍCH HOẠT
# =====================================================================

def main():
    validate_env()
    log("Khởi động phiên làm việc của bot...")
    memory = gist_load_memory()

    # Offset cho getUpdates: LUÔN = last_update_id + 1 -> tuyệt đối không xử lý lại tin cũ
    offset = memory["last_update_id"] + 1 if memory["last_update_id"] > 0 else 0

    if offset == 0:
        # Lần chạy đầu (bộ nhớ rỗng): bỏ qua tin nhắn tồn đọng trước khi deploy,
        # chỉ trả lời những tin nhắn gửi TỪ BÂY GIỜ trở đi.
        latest = tg_get_latest_update_id()
        if latest > 0:
            memory["last_update_id"] = latest
            offset = latest + 1
            gist_save_memory(memory)
            log(f"Lần chạy đầu: bỏ qua tin cũ, bắt đầu xử lý từ update_id={offset}")

    start_time = time.time()
    while _running and (time.time() - start_time) < JOB_DURATION_SEC:
        remaining = JOB_DURATION_SEC - (time.time() - start_time)
        if remaining < SAFE_MARGIN_SEC:
            log("Gần hết thời lượng phiên — kết thúc để nhường chu kỳ cron kế tiếp.")
            break

        # Long-polling: Telegram giữ kết nối tối đa poll_timeout giây, có tin là trả về ngay
        poll_timeout = max(5, min(LONG_POLL_SEC, int(remaining) - SAFE_MARGIN_SEC))
        updates = tg_get_updates(offset, poll_timeout)
        if not updates:
            continue  # không có tin mới (hoặc lỗi tạm thời) -> quét lại

        # Cần còn đủ thời gian cho 1 chu trình xử lý (DeepSeek + gửi tin + ghi Gist).
        # Nếu không đủ thì dừng: Telegram sẽ tự gửi lại các update CHƯA XÁC NHẬN ở chu kỳ sau
        # (vì last_update_id chưa được lưu) -> không mất tin, không trùng trả lời.
        if (time.time() - start_time) + PROCESS_HEADROOM_SEC > JOB_DURATION_SEC:
            log("Sắp hết phiên — chừa lại lô tin này cho chu kỳ kế tiếp xử lý.")
            break

        # Xác nhận các update trong lô này là "đã đọc" (offset = id lớn nhất + 1)
        offset = max(u.get("update_id", 0) for u in updates) + 1

        # Xử lý và lưu bộ nhớ NGAY sau mỗi lô -> nếu job crash cũng chỉ mất tối đa 1 lô
        if process_updates(updates, memory):
            gist_save_memory(memory)

    # Lưu dữ liệu lần cuối trước khi thoát (job bị hủy giữa chừng cũng kịp lưu)
    gist_save_memory(memory)
    log("Kết thúc phiên. Hẹn gặp lại ở chu kỳ cron kế tiếp.")


if __name__ == "__main__":
    main()
