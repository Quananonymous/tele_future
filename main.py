import json
import hmac
import hashlib
import time
import threading
import urllib.request
import urllib.parse
import numpy as np
import websocket
import logging
import requests
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot_logs.log')
    ]
)
logger = logging.getLogger()

# Lấy cấu hình từ biến môi trường
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '')
BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    
# Cấu hình bot từ biến môi trường (dạng JSON)
bot_config_json = os.getenv('BOT_CONFIGS', '[]')
try:
    BOT_CONFIGS = json.loads(bot_config_json)
except:
    BOT_CONFIGS = []

API_KEY = BINANCE_API_KEY
API_SECRET = BINANCE_SECRET_KEY

# ========== HÀM GỬI TELEGRAM ==========
def send_telegram(message, chat_id=None, reply_markup=None):
    """Gửi thông báo qua Telegram với menu tùy chọn"""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("Chưa cấu hình Telegram Bot Token")
        return
    
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        logger.warning("Chưa cấu hình Telegram Chat ID")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error(f"Lỗi gửi Telegram: {response.text}")
    except Exception as e:
        logger.error(f"Lỗi kết nối Telegram: {e}")

# ========== HÀM TẠO MENU TELEGRAM ==========
def create_menu_keyboard():
    """Tạo menu 3 nút cho Telegram"""
    return {
        "keyboard": [
            [{"text": "📊 Danh sách Bot"}],
            [{"text": "➕ Thêm Bot"}, {"text": "⛔ Dừng Bot"}],
            [{"text": "💰 Số dư tài khoản"}, {"text": "📈 Vị thế đang mở"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

def create_cancel_keyboard():
    """Tạo bàn phím hủy"""
    return {
        "keyboard": [[{"text": "❌ Hủy bỏ"}]],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_symbols_keyboard():
    """Tạo bàn phím chọn cặp coin"""
    popular_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]
    keyboard = []
    row = []
    for symbol in popular_symbols:
        row.append({"text": symbol})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def create_leverage_keyboard():
    """Tạo bàn phím chọn đòn bẩy"""
    leverages = ["10", "20", "30", "50", "75", "100"]
    keyboard = []
    row = []
    for lev in leverages:
        row.append({"text": f"⚖️ {lev}x"})
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([{"text": "❌ Hủy bỏ"}])
    
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

# ========== HÀM HỖ TRỢ API BINANCE ==========
def sign(query):
    try:
        return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    except Exception as e:
        logger.error(f"Sign error: {e}")
        send_telegram(f"⚠️ <b>LỖI SIGN:</b> {str(e)}")
        return ""

def get_step_size(symbol):
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
    except Exception as e:
        logger.error(f"Error getting step size: {e}")
        send_telegram(f"⚠️ <b>LỖI STEP SIZE:</b> {symbol} - {str(e)}")
    return 0.001

def set_leverage(symbol, lev):
    try:
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol.upper(),
            "leverage": lev,
            "timestamp": ts
        }
        query = urllib.parse.urlencode(params)
        sig = sign(query)
        url = f"https://fapi.binance.com/fapi/v1/leverage?{query}&signature={sig}"
        req = urllib.request.Request(url, headers={'X-MBX-APIKEY': API_KEY}, method='POST')
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            if 'leverage' in data:
                return True
    except Exception as e:
        logger.error(f"Error setting leverage: {e}")
        send_telegram(f"⚠️ <b>LỖI ĐÒN BẨY:</b> {symbol} - {str(e)}")
    return False

def get_balance():
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        req = urllib.request.Request(url, headers={'X-MBX-APIKEY': API_KEY})
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read())
            for asset in data['assets']:
                if asset['asset'] == 'USDT':
                    return float(asset['availableBalance'])
    except Exception as e:
        logger.error(f"Error getting balance: {e}")
        send_telegram(f"⚠️ <b>LỖI SỐ DƯ:</b> {str(e)}")
    return 0

def place_order(symbol, side, qty):
    try:
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol.upper(),
            "side": side,
            "type": "MARKET",
            "quantity": qty,
            "timestamp": ts
        }
        query = urllib.parse.urlencode(params)
        sig = sign(query)
        url = f"https://fapi.binance.com/fapi/v1/order?{query}&signature={sig}"
        req = urllib.request.Request(url, headers={'X-MBX-APIKEY': API_KEY}, method='POST')
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read())
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        send_telegram(f"⚠️ <b>LỖI ĐẶT LỆNH:</b> {symbol} - {str(e)}")
    return None

def cancel_all_orders(symbol):
    try:
        ts = int(time.time() * 1000)
        params = {"symbol": symbol.upper(), "timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query)
        url = f"https://fapi.binance.com/fapi/v1/allOpenOrders?{query}&signature={sig}"
        req = urllib.request.Request(url, headers={'X-MBX-APIKEY': API_KEY}, method='DELETE')
        urllib.request.urlopen(req)
        return True
    except Exception as e:
        logger.error(f"Error canceling orders: {e}")
        send_telegram(f"⚠️ <b>LỖI HỦY LỆNH:</b> {symbol} - {str(e)}")
    return False

def get_current_price(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol.upper()}"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read())
            return float(data['price'])
    except Exception as e:
        logger.error(f"Error getting price: {e}")
        send_telegram(f"⚠️ <b>LỖI GIÁ:</b> {symbol} - {str(e)}")
    return 0

def get_positions(symbol=None):
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        if symbol:
            params["symbol"] = symbol.upper()
            
        query = urllib.parse.urlencode(params)
        sig = sign(query)
        url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query}&signature={sig}"
        req = urllib.request.Request(url, headers={'X-MBX-APIKEY': API_KEY})
        with urllib.request.urlopen(req) as response:
            positions = json.loads(response.read())
            
            if symbol:
                for pos in positions:
                    if pos['symbol'] == symbol.upper():
                        return [pos]
            
            return positions
    except Exception as e:
        logger.error(f"Error getting positions: {e}")
        send_telegram(f"⚠️ <b>LỖI VỊ THẾ:</b> {symbol if symbol else ''} - {str(e)}")
    return []

# ========== TÍNH CHỈ BÁO KỸ THUẬT ==========
def calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1 + rs))

def calc_ema(prices, period=21):
    if len(prices) < period:
        return None
    
    ema = np.mean(prices[:period])
    k = 2 / (period + 1)
    
    for price in prices[period:]:
        ema = price * k + ema * (1 - k)
    
    return ema

# ========== QUẢN LÝ WEBSOCKET HIỆU QUẢ ==========
class WebSocketManager:
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=20)
        self._lock = threading.Lock()
        
    def add_symbol(self, symbol, callback):
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.connections:
                self._create_connection(symbol, callback)
                
    def _create_connection(self, symbol, callback):
        stream = f"{symbol.lower()}@trade"
        url = f"wss://fstream.binance.com/ws/{stream}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'p' in data:
                    price = float(data['p'])
                    self.executor.submit(callback, price)
            except Exception as e:
                logger.error(f"Message error for {symbol}: {e}")
                
        def on_error(ws, error):
            logger.error(f"WebSocket error for {symbol}: {error}")
            time.sleep(5)
            self._reconnect(symbol, callback)
            
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket closed for {symbol}: {close_status_code} - {close_msg}")
            if symbol in self.connections:
                time.sleep(5)
                self._reconnect(symbol, callback)
                
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        thread = threading.Thread(target=ws.run_forever, daemon=True)
        thread.start()
        
        self.connections[symbol] = {
            'ws': ws,
            'thread': thread,
            'callback': callback
        }
        logger.info(f"WebSocket started for {symbol}")
        
    def _reconnect(self, symbol, callback):
        logger.info(f"Reconnecting WebSocket for {symbol}")
        self.remove_symbol(symbol)
        self._create_connection(symbol, callback)
        
    def remove_symbol(self, symbol):
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.connections:
                try:
                    self.connections[symbol]['ws'].close()
                except:
                    pass
                del self.connections[symbol]
                logger.info(f"WebSocket removed for {symbol}")
                
    def stop(self):
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)

# ========== BOT CHÍNH VỚI TÍCH HỢP TELEGRAM ==========
class IndicatorBot:
    def __init__(self, symbol, lev, percent, tp, sl, indicator, ws_manager):
        self.symbol = symbol.upper()
        self.lev = lev
        self.percent = percent
        self.tp = tp
        self.sl = sl
        self.indicator = indicator
        self.ws_manager = ws_manager
        self.status = "waiting"
        self.side = ""
        self.qty = 0
        self.entry = 0
        self.prices = []
        self._stop = False
        self.position_open = False
        self.last_trade_time = 0
        self.last_rsi = 50
        self.position_check_interval = 60
        self.last_position_check = 0
        self.last_error_log_time = 0
        
        # Đăng ký với WebSocket Manager
        self.ws_manager.add_symbol(self.symbol, self._handle_price_update)
        
        # Bắt đầu thread chính
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self.log(f"🟢 Bot khởi động cho {self.symbol}")

    def log(self, message):
        """Ghi log và gửi qua Telegram"""
        logger.info(f"[{self.symbol}] {message}")
        send_telegram(f"<b>{self.symbol}</b>: {message}")

    def _handle_price_update(self, price):
        if self._stop: 
            return
            
        self.prices.append(price)
        if len(self.prices) > 100:
            self.prices = self.prices[-100:]

    def _run(self):
        """Luồng chính quản lý bot"""
        while not self._stop:
            try:
                current_time = time.time()
                
                # Kiểm tra trạng thái vị thế định kỳ
                if current_time - self.last_position_check > self.position_check_interval:
                    self.check_position_status()
                    self.last_position_check = current_time
                
                # Xử lý logic giao dịch
                if not self.position_open and self.status == "waiting":
                    signal = self.get_signal()
                    
                    if signal and current_time - self.last_trade_time > 60:
                        self.open_position(signal)
                        self.last_trade_time = current_time
                
                # Kiểm tra TP/SL cho vị thế đang mở
                if self.position_open and self.status == "open":
                    self.check_tp_sl()
                
                time.sleep(1)
                
            except Exception as e:
                if time.time() - self.last_error_log_time > 10:
                    self.log(f"Bot error: {e}")
                    self.last_error_log_time = time.time()
                time.sleep(5)

    def stop(self):
        self._stop = True
        self.ws_manager.remove_symbol(self.symbol)
        try:
            cancel_all_orders(self.symbol)
        except Exception as e:
            if time.time() - self.last_error_log_time > 10:
                self.log(f"Lỗi hủy lệnh: {e}")
                self.last_error_log_time = time.time()
        self.log(f"🔴 Bot dừng cho {self.symbol}")

    def check_position_status(self):
        """Kiểm tra trạng thái vị thế từ API Binance"""
        try:
            positions = get_positions(self.symbol)
            
            if not positions or len(positions) == 0:
                self.position_open = False
                self.status = "waiting"
                self.side = ""
                self.qty = 0
                self.entry = 0
                return
            
            for pos in positions:
                if pos['symbol'] == self.symbol:
                    position_amt = float(pos['positionAmt'])
                    
                    if abs(position_amt) > 0:
                        self.position_open = True
                        self.status = "open"
                        self.side = "BUY" if position_amt > 0 else "SELL"
                        self.qty = position_amt
                        self.entry = float(pos['entryPrice'])
                        return
            
            self.position_open = False
            self.status = "waiting"
            self.side = ""
            self.qty = 0
            self.entry = 0
            
        except Exception as e:
            if time.time() - self.last_error_log_time > 10:
                self.log(f"Lỗi kiểm tra vị thế: {e}")
                self.last_error_log_time = time.time()

    def check_tp_sl(self):
        """Tự động kiểm tra và đóng lệnh khi đạt TP/SL"""
        if not self.position_open or not self.entry or not self.qty:
            return
            
        try:
            if len(self.prices) > 0:
                current_price = self.prices[-1]
            else:
                current_price = get_current_price(self.symbol)
                
            if current_price <= 0:
                return
                
            # Tính ROI
            if self.side == "BUY":
                profit = (current_price - self.entry) * self.qty
            else:
                profit = (self.entry - current_price) * abs(self.qty)
                
            # Tính % ROI dựa trên vốn ban đầu
            invested = self.entry * abs(self.qty) / self.lev
            roi = (profit / invested) * 100
            
            # Kiểm tra TP/SL
            if roi >= self.tp:
                self.close_position(f"✅ Đạt TP {self.tp}% (ROI: {roi:.2f}%)")
            elif roi <= -self.sl:
                self.close_position(f"❌ Đạt SL {self.sl}% (ROI: {roi:.2f}%)")
                
        except Exception as e:
            if time.time() - self.last_error_log_time > 10:
                self.log(f"Lỗi kiểm tra TP/SL: {e}")
                self.last_error_log_time = time.time()

    def get_signal(self):
        if len(self.prices) < 40:
            return None
            
        prices_arr = np.array(self.prices)
        
        # Sử dụng RSI làm chỉ báo chính
        rsi_val = calc_rsi(prices_arr)
        
        if rsi_val is not None:
            self.last_rsi = rsi_val
            if rsi_val <= 30: 
                return "BUY"
            if rsi_val >= 70: 
                return "SELL"
                    
        return None

    def open_position(self, side):
        # Kiểm tra lại trạng thái trước khi vào lệnh
        self.check_position_status()
        
        if self.position_open:
            self.log(f"⚠️ Đã có vị thế mở, không vào lệnh mới")
            return
            
        try:
            # Hủy lệnh tồn đọng
            cancel_all_orders(self.symbol)
            
            # Đặt đòn bẩy
            if not set_leverage(self.symbol, self.lev):
                self.log(f"Không thể đặt đòn bẩy {self.lev}")
                return
            
            # Tính toán khối lượng
            balance = get_balance()
            if balance <= 0:
                self.log(f"Không đủ số dư USDT")
                return
            
            usdt_amount = balance * (self.percent / 100)
            price = get_current_price(self.symbol)
            if price <= 0:
                self.log(f"Lỗi lấy giá")
                return
                
            step = get_step_size(self.symbol)
            if step <= 0:
                step = 0.001
            
            # Tính số lượng với đòn bẩy
            qty = (usdt_amount * self.lev) / price
            
            # Làm tròn số lượng theo step size
            if step > 0:
                steps = qty / step
                qty = round(steps) * step
            
            qty = max(qty, 0)
            qty = round(qty, 8)
            
            min_qty = step
            
            if qty < min_qty:
                self.log(f"⚠️ Số lượng quá nhỏ ({qty}), không đặt lệnh")
                return
                
            # Đặt lệnh
            res = place_order(self.symbol, side, qty)
            if not res:
                self.log(f"Lỗi khi đặt lệnh")
                return
                
            executed_qty = float(res.get('executedQty', 0))
            if executed_qty <= 0:
                self.log(f"Lệnh không khớp, số lượng thực thi: {executed_qty}")
                return

            # Cập nhật trạng thái
            self.entry = float(res.get('avgPrice', price))
            self.side = side
            self.qty = executed_qty if side == "BUY" else -executed_qty
            self.status = "open"
            self.position_open = True
            
            # Thông báo qua Telegram
            message = (
                f"✅ <b>ĐÃ MỞ VỊ THẾ {self.symbol}</b>\n"
                f"📌 Hướng: {side}\n"
                f"🏷️ Giá vào: {self.entry:.4f}\n"
                f"📊 Khối lượng: {executed_qty}\n"
                f"💵 Giá trị: {executed_qty * self.entry:.2f} USDT\n"
                f"⚖️ Đòn bẩy: {self.lev}x\n"
                f"🎯 TP: {self.tp}% | 🛡️ SL: {self.sl}%"
            )
            self.log(message)

        except Exception as e:
            self.position_open = False
            self.log(f"❌ Lỗi khi vào lệnh: {e}")

    def close_position(self, reason=""):
        try:
            # Hủy lệnh tồn đọng
            cancel_all_orders(self.symbol)
            
            if abs(self.qty) > 0:
                close_side = "SELL" if self.side == "BUY" else "BUY"
                close_qty = abs(self.qty)
                
                # Làm tròn số lượng
                step = get_step_size(self.symbol)
                if step > 0:
                    steps = close_qty / step
                    close_qty = round(steps) * step
                
                close_qty = max(close_qty, 0)
                close_qty = round(close_qty, 8)
                
                res = place_order(self.symbol, close_side, close_qty)
                if res:
                    price = float(res.get('avgPrice', 0))
                    # Thông báo qua Telegram
                    message = (
                        f"⛔ <b>ĐÃ ĐÓNG VỊ THẾ {self.symbol}</b>\n"
                        f"📌 Lý do: {reason}\n"
                        f"🏷️ Giá ra: {price:.4f}\n"
                        f"📊 Khối lượng: {close_qty}\n"
                        f"💵 Giá trị: {close_qty * price:.2f} USDT"
                    )
                    self.log(message)
                else:
                    self.log(f"Lỗi khi đóng lệnh")
                    
            # Kiểm tra lại trạng thái
            time.sleep(1)
            self.check_position_status()
            
            # Nếu vẫn còn vị thế, thử đóng lại
            if self.position_open:
                self.log(f"⚠️ Vị thế chưa đóng, thử đóng lại")
                self.close_position("Thử đóng lại")
                return
                    
            # Reset trạng thái
            self.status = "waiting"
            self.side = ""
            self.qty = 0
            self.entry = 0
            self.position_open = False
            self.last_trade_time = time.time()
            
        except Exception as e:
            self.log(f"❌ Lỗi khi đóng lệnh: {e}")

# ========== QUẢN LÝ BOT CHẠY NỀN VÀ TƯƠNG TÁC TELEGRAM ==========
class BotManager:
    def __init__(self):
        self.ws_manager = WebSocketManager()
        self.bots = {}
        self.running = True
        self.start_time = time.time()
        self.user_states = {}  # Lưu trạng thái người dùng
        self.admin_chat_id = TELEGRAM_CHAT_ID
        
        self.log("🟢 HỆ THỐNG BOT ĐÃ KHỞI ĐỘNG")
        
        # Bắt đầu thread kiểm tra trạng thái
        self.status_thread = threading.Thread(target=self._status_monitor, daemon=True)
        self.status_thread.start()
        
        # Bắt đầu thread lắng nghe Telegram
        self.telegram_thread = threading.Thread(target=self._telegram_listener, daemon=True)
        self.telegram_thread.start()
        
        # Gửi menu chính khi khởi động
        if self.admin_chat_id:
            self.send_main_menu(self.admin_chat_id)

    def log(self, message):
        """Ghi log hệ thống và gửi Telegram"""
        logger.info(f"[SYSTEM] {message}")
        send_telegram(f"<b>SYSTEM</b>: {message}")

    def send_main_menu(self, chat_id):
        """Gửi menu chính cho người dùng"""
        welcome = (
            "🤖 <b>BOT GIAO DỊCH FUTURES BINANCE</b>\n\n"
            "Chọn một trong các tùy chọn bên dưới:"
        )
        send_telegram(welcome, chat_id, create_menu_keyboard())

    def add_bot(self, symbol, lev, percent, tp, sl, indicator):
        symbol = symbol.upper()
        if symbol in self.bots:
            self.log(f"⚠️ Đã có bot cho {symbol}")
            return False
            
        # Kiểm tra API key
        if not API_KEY or not API_SECRET:
            self.log("❌ Chưa cấu hình API Key và Secret Key!")
            return False
            
        try:
            # Kiểm tra kết nối API
            price = get_current_price(symbol)
            if price <= 0:
                self.log(f"❌ Không thể lấy giá cho {symbol}")
                return False
            
            # Kiểm tra vị thế hiện tại
            positions = get_positions(symbol)
            if positions and any(float(pos['positionAmt']) != 0 for pos in positions):
                self.log(f"⚠️ Đã có vị thế mở cho {symbol} trên Binance")
                return False
            
            # Tạo bot mới
            bot = IndicatorBot(
                symbol, lev, percent, tp, sl, 
                indicator, self.ws_manager
            )
            self.bots[symbol] = bot
            self.log(f"✅ Đã thêm bot: {symbol} | ĐB: {lev}x | %: {percent} | TP/SL: {tp}%/{sl}%")
            return True
            
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot {symbol}: {str(e)}")
            return False

    def stop_bot(self, symbol):
        symbol = symbol.upper()
        bot = self.bots.get(symbol)
        if bot:
            bot.stop()
            if bot.status == "open":
                bot.close_position("⛔ Dừng bot thủ công")
            self.log(f"⛔ Đã dừng bot cho {symbol}")
            del self.bots[symbol]
            return True
        return False

    def stop_all(self):
        self.log("⛔ Đang dừng tất cả bot...")
        for symbol in list(self.bots.keys()):
            self.stop_bot(symbol)
        self.ws_manager.stop()
        self.running = False
        self.log("🔴 Hệ thống đã dừng")

    def _status_monitor(self):
        """Kiểm tra và báo cáo trạng thái định kỳ"""
        while self.running:
            try:
                # Tính thời gian hoạt động
                uptime = time.time() - self.start_time
                hours, rem = divmod(uptime, 3600)
                minutes, seconds = divmod(rem, 60)
                uptime_str = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                
                # Báo cáo số bot đang chạy
                active_bots = [s for s, b in self.bots.items() if not b._stop]
                
                # Báo cáo số dư tài khoản
                balance = get_balance()
                
                # Tạo báo cáo
                status_msg = (
                    f"📊 <b>BÁO CÁO HỆ THỐNG</b>\n"
                    f"⏱ Thời gian hoạt động: {uptime_str}\n"
                    f"🤖 Số bot đang chạy: {len(active_bots)}\n"
                    f"📈 Bot hoạt động: {', '.join(active_bots) if active_bots else 'Không có'}\n"
                    f"💰 Số dư khả dụng: {balance:.2f} USDT"
                )
                send_telegram(status_msg)
                
                # Log chi tiết
                for symbol, bot in self.bots.items():
                    if bot.status == "open":
                        status_msg = (
                            f"🔹 <b>{symbol}</b>\n"
                            f"📌 Hướng: {bot.side}\n"
                            f"🏷️ Giá vào: {bot.entry:.4f}\n"
                            f"📊 Khối lượng: {abs(bot.qty)}\n"
                            f"⚖️ Đòn bẩy: {bot.lev}x\n"
                            f"🎯 TP: {bot.tp}% | 🛡️ SL: {bot.sl}%"
                        )
                        send_telegram(status_msg)
                
            except Exception as e:
                logger.error(f"Lỗi báo cáo trạng thái: {str(e)}")
            
            # Kiểm tra mỗi 6 giờ
            time.sleep(6 * 3600)

    def _telegram_listener(self):
        """Lắng nghe và xử lý tin nhắn từ Telegram"""
        last_update_id = 0
        
        while self.running:
            try:
                # Lấy tin nhắn mới
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={last_update_id+1}&timeout=30"
                response = requests.get(url, timeout=35)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('ok'):
                        for update in data['result']:
                            update_id = update['update_id']
                            message = update.get('message', {})
                            chat_id = str(message.get('chat', {}).get('id'))
                            text = message.get('text', '').strip()
                            
                            # Chỉ xử lý tin nhắn từ admin
                            if chat_id != self.admin_chat_id:
                                continue
                            
                            # Cập nhật ID tin nhắn cuối
                            if update_id > last_update_id:
                                last_update_id = update_id
                            
                            # Xử lý tin nhắn
                            self._handle_telegram_message(chat_id, text)
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Lỗi Telegram listener: {str(e)}")
                time.sleep(5)

    def _handle_telegram_message(self, chat_id, text):
        """Xử lý tin nhắn từ người dùng"""
        # Lưu trạng thái người dùng
        user_state = self.user_states.get(chat_id, {})
        current_step = user_state.get('step')
        
        # Xử lý theo bước hiện tại
        if current_step == 'waiting_symbol':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_menu_keyboard())
            else:
                symbol = text.upper()
                self.user_states[chat_id] = {
                    'step': 'waiting_leverage',
                    'symbol': symbol
                }
                send_telegram(f"Chọn đòn bẩy cho {symbol}:", chat_id, create_leverage_keyboard())
        
        elif current_step == 'waiting_leverage':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_menu_keyboard())
            elif 'x' in text:
                leverage = int(text.replace('⚖️', '').replace('x', '').strip())
                user_state['leverage'] = leverage
                user_state['step'] = 'waiting_percent'
                send_telegram(
                    f"📌 Cặp: {user_state['symbol']}\n⚖️ Đòn bẩy: {leverage}x\n\nNhập % số dư muốn sử dụng (1-100):",
                    chat_id,
                    create_cancel_keyboard()
                )
        
        elif current_step == 'waiting_percent':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_menu_keyboard())
            else:
                try:
                    percent = float(text)
                    if 1 <= percent <= 100:
                        user_state['percent'] = percent
                        user_state['step'] = 'waiting_tp'
                        send_telegram(
                            f"📌 Cặp: {user_state['symbol']}\n⚖️ ĐB: {user_state['leverage']}x\n📊 %: {percent}%\n\nNhập % Take Profit (ví dụ: 10):",
                            chat_id,
                            create_cancel_keyboard()
                        )
                    else:
                        send_telegram("⚠️ Vui lòng nhập % từ 1-100", chat_id)
                except:
                    send_telegram("⚠️ Giá trị không hợp lệ, vui lòng nhập số", chat_id)
        
        elif current_step == 'waiting_tp':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_menu_keyboard())
            else:
                try:
                    tp = float(text)
                    if tp > 0:
                        user_state['tp'] = tp
                        user_state['step'] = 'waiting_sl'
                        send_telegram(
                            f"📌 Cặp: {user_state['symbol']}\n⚖️ ĐB: {user_state['leverage']}x\n📊 %: {user_state['percent']}%\n🎯 TP: {tp}%\n\nNhập % Stop Loss (ví dụ: 5):",
                            chat_id,
                            create_cancel_keyboard()
                        )
                    else:
                        send_telegram("⚠️ TP phải lớn hơn 0", chat_id)
                except:
                    send_telegram("⚠️ Giá trị không hợp lệ, vui lòng nhập số", chat_id)
        
        elif current_step == 'waiting_sl':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy thêm bot", chat_id, create_menu_keyboard())
            else:
                try:
                    sl = float(text)
                    if sl > 0:
                        # Thêm bot
                        symbol = user_state['symbol']
                        leverage = user_state['leverage']
                        percent = user_state['percent']
                        tp = user_state['tp']
                        
                        if self.add_bot(symbol, leverage, percent, tp, sl, "RSI"):
                            send_telegram(
                                f"✅ <b>ĐÃ THÊM BOT THÀNH CÔNG</b>\n\n"
                                f"📌 Cặp: {symbol}\n"
                                f"⚖️ Đòn bẩy: {leverage}x\n"
                                f"📊 % Số dư: {percent}%\n"
                                f"🎯 TP: {tp}%\n"
                                f"🛡️ SL: {sl}%",
                                chat_id,
                                create_menu_keyboard()
                            )
                        else:
                            send_telegram("❌ Không thể thêm bot, vui lòng kiểm tra log", chat_id, create_menu_keyboard())
                        
                        # Reset trạng thái
                        self.user_states[chat_id] = {}
                    else:
                        send_telegram("⚠️ SL phải lớn hơn 0", chat_id)
                except:
                    send_telegram("⚠️ Giá trị không hợp lệ, vui lòng nhập số", chat_id)
        
        # Xử lý các lệnh chính
        elif text == "📊 Danh sách Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id)
            else:
                message = "🤖 <b>DANH SÁCH BOT ĐANG CHẠY</b>\n\n"
                for symbol, bot in self.bots.items():
                    status = "🟢 Mở" if bot.status == "open" else "🟡 Chờ"
                    message += f"🔹 {symbol} | {status} | {bot.side}\n"
                send_telegram(message, chat_id)
        
        elif text == "➕ Thêm Bot":
            self.user_states[chat_id] = {'step': 'waiting_symbol'}
            send_telegram("Chọn cặp coin:", chat_id, create_symbols_keyboard())
        
        elif text == "⛔ Dừng Bot":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id)
            else:
                message = "⛔ <b>CHỌN BOT ĐỂ DỪNG</b>\n\n"
                keyboard = []
                row = []
                
                for i, symbol in enumerate(self.bots.keys()):
                    message += f"🔹 {symbol}\n"
                    row.append({"text": f"⛔ {symbol}"})
                    if len(row) == 2 or i == len(self.bots) - 1:
                        keyboard.append(row)
                        row = []
                
                keyboard.append([{"text": "❌ Hủy bỏ"}])
                
                send_telegram(
                    message, 
                    chat_id, 
                    {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}
                )
        
        elif text.startswith("⛔ "):
            symbol = text.replace("⛔ ", "").strip().upper()
            if symbol in self.bots:
                self.stop_bot(symbol)
                send_telegram(f"⛔ Đã gửi lệnh dừng bot {symbol}", chat_id, create_menu_keyboard())
            else:
                send_telegram(f"⚠️ Không tìm thấy bot {symbol}", chat_id, create_menu_keyboard())
        
        elif text == "💰 Số dư tài khoản":
            try:
                balance = get_balance()
                send_telegram(f"💰 <b>SỐ DƯ KHẢ DỤNG</b>: {balance:.2f} USDT", chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy số dư: {str(e)}", chat_id)
        
        elif text == "📈 Vị thế đang mở":
            try:
                positions = get_positions()
                if not positions:
                    send_telegram("📭 Không có vị thế nào đang mở", chat_id)
                    return
                
                message = "📈 <b>VỊ THẾ ĐANG MỞ</b>\n\n"
                for pos in positions:
                    if float(pos['positionAmt']) != 0:
                        symbol = pos['symbol']
                        amount = float(pos['positionAmt'])
                        entry = float(pos['entryPrice'])
                        side = "LONG" if amount > 0 else "SHORT"
                        pnl = float(pos['unRealizedProfit'])
                        
                        message += (
                            f"🔹 {symbol} | {side}\n"
                            f"📊 Khối lượng: {abs(amount):.4f}\n"
                            f"🏷️ Giá vào: {entry:.4f}\n"
                            f"💰 PnL: {pnl:.2f} USDT\n\n"
                        )
                
                send_telegram(message, chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy vị thế: {str(e)}", chat_id)
        
        # Gửi lại menu nếu không có lệnh phù hợp
        elif text:
            self.send_main_menu(chat_id)

# ========== HÀM KHỞI CHẠY CHÍNH ==========
def main():
    # Khởi tạo hệ thống
    manager = BotManager()
    
    # Thêm các bot từ cấu hình
    if BOT_CONFIGS:
        for config in BOT_CONFIGS:
            manager.add_bot(*config)
    else:
        manager.log("⚠️ Không có cấu hình bot nào được tìm thấy!")
    
    # Thông báo số dư ban đầu
    try:
        balance = get_balance()
        manager.log(f"💰 SỐ DƯ BAN ĐẦU: {balance:.2f} USDT")
    except:
        pass
    
    try:
        # Giữ chương trình chạy
        while manager.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        manager.log("👋 Nhận tín hiệu dừng từ người dùng...")
    finally:
        manager.stop_all()

if __name__ == "__main__":
    main()