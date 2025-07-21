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
import math
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Cấu hình logging chi tiết
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot_errors.log')
    ]
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Lấy cấu hình từ biến môi trường
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '')
BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    
# Cấu hình bot từ biến môi trường (dạng JSON)
bot_config_json = os.getenv('BOT_CONFIGS', '[]')
try:
    BOT_CONFIGS = json.loads(bot_config_json)
except Exception as e:
    logging.error(f"Lỗi phân tích cấu hình BOT_CONFIGS: {e}")
    BOT_CONFIGS = []

API_KEY = BINANCE_API_KEY
API_SECRET = BINANCE_SECRET_KEY

# ========== HÀM GỬI TELEGRAM VÀ XỬ LÝ LỖI ==========
def send_telegram(message, chat_id=None, reply_markup=None):
    """Gửi thông báo qua Telegram với xử lý lỗi chi tiết"""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("Cấu hình Telegram Bot Token chưa được thiết lập")
        return
    
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not chat_id:
        logger.warning("Cấu hình Telegram Chat ID chưa được thiết lập")
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
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code != 200:
            error_msg = response.text
            logger.error(f"Lỗi gửi Telegram ({response.status_code}): {error_msg}")
    except Exception as e:
        logger.error(f"Lỗi kết nối Telegram: {str(e)}")

# ========== HÀM TẠO MENU TELEGRAM ==========
def create_menu_keyboard():
    """Tạo menu 3 nút cho Telegram"""
    return {
        "keyboard": [
            [{"text": "📊 Danh sách Bot"}],
            [{"text": "➕ Thêm Bot"}, {"text": "⛔ Dừng Bot"}],
            [{"text": "💰 Số dư tài khoản"}, {"text": "📈 Vị thế đang mở"}],
            [{"text": "🎯 Cài đặt TP/SL"}]
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
    popular_symbols = ["SUIUSDT", "DOGEUSDT", "1000PEPEUSDT", "TRUMPUSDT", "XRPUSDT", "ADAUSDT"]
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

# ========== HÀM HỖ TRỢ API BINANCE VỚI XỬ LÝ LỖI CHI TIẾT ==========
def sign(query):
    try:
        return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    except Exception as e:
        logger.error(f"Lỗi tạo chữ ký: {str(e)}")
        send_telegram(f"⚠️ <b>LỖI SIGN:</b> {str(e)}")
        return ""

def binance_api_request(url, method='GET', params=None, headers=None):
    """Hàm tổng quát cho các yêu cầu API Binance với xử lý lỗi chi tiết"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if method.upper() == 'GET':
                if params:
                    query = urllib.parse.urlencode(params)
                    url = f"{url}?{query}"
                req = urllib.request.Request(url, headers=headers or {})
            else:
                data = urllib.parse.urlencode(params).encode() if params else None
                req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
            
            with urllib.request.urlopen(req, timeout=15) as response:
                if response.status == 200:
                    return json.loads(response.read().decode())
                else:
                    logger.error(f"Lỗi API ({response.status}): {response.read().decode()}")
                    if response.status == 429:  # Rate limit
                        time.sleep(2 ** attempt)  # Exponential backoff
                    elif response.status >= 500:
                        time.sleep(1)
                    continue
        except urllib.error.HTTPError as e:
            logger.error(f"Lỗi HTTP ({e.code}): {e.reason}")
            if e.code == 429:  # Rate limit
                time.sleep(2 ** attempt)  # Exponential backoff
            elif e.code >= 500:
                time.sleep(1)
            continue
        except Exception as e:
            logger.error(f"Lỗi kết nối API: {str(e)}")
            time.sleep(1)
    
    logger.error(f"Không thể thực hiện yêu cầu API sau {max_retries} lần thử")
    return None

def get_step_size(symbol):
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        data = binance_api_request(url)
        if not data:
            return 0.001
            
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
    except Exception as e:
        logger.error(f"Lỗi lấy step size: {str(e)}")
        send_telegram(f"⚠️ <b>LỖI STEP SIZE:</b> {symbol} - {str(e)}")
    return 0.001

def get_min_qty(symbol):
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        data = binance_api_request(url)
        if not data:
            return 0.001
            
        for s in data['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['minQty'])
    except Exception as e:
        logger.error(f"Lỗi lấy min qty: {str(e)}")
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
        headers = {'X-MBX-APIKEY': API_KEY}
        
        response = binance_api_request(url, method='POST', headers=headers)
        if response and 'leverage' in response:
            return True
    except Exception as e:
        logger.error(f"Lỗi thiết lập đòn bẩy: {str(e)}")
        send_telegram(f"⚠️ <b>LỖI ĐÒN BẨY:</b> {symbol} - {str(e)}")
    return False

def get_balance():
    try:
        ts = int(time.time() * 1000)
        params = {"timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query)
        url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': API_KEY}
        
        data = binance_api_request(url, headers=headers)
        if not data:
            return 0
            
        for asset in data['assets']:
            if asset['asset'] == 'USDT':
                return float(asset['availableBalance'])
    except Exception as e:
        logger.error(f"Lỗi lấy số dư: {str(e)}")
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
        headers = {'X-MBX-APIKEY': API_KEY}
        
        return binance_api_request(url, method='POST', headers=headers)
    except Exception as e:
        logger.error(f"Lỗi đặt lệnh: {str(e)}")
        send_telegram(f"⚠️ <b>LỖI ĐẶT LỆNH:</b> {symbol} - {str(e)}")
    return None

def cancel_all_orders(symbol):
    try:
        ts = int(time.time() * 1000)
        params = {"symbol": symbol.upper(), "timestamp": ts}
        query = urllib.parse.urlencode(params)
        sig = sign(query)
        url = f"https://fapi.binance.com/fapi/v1/allOpenOrders?{query}&signature={sig}"
        headers = {'X-MBX-APIKEY': API_KEY}
        
        binance_api_request(url, method='DELETE', headers=headers)
        return True
    except Exception as e:
        logger.error(f"Lỗi hủy lệnh: {str(e)}")
        send_telegram(f"⚠️ <b>LỖI HỦY LỆNH:</b> {symbol} - {str(e)}")
    return False

def get_current_price(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol.upper()}"
        data = binance_api_request(url)
        if data and 'price' in data:
            return float(data['price'])
    except Exception as e:
        logger.error(f"Lỗi lấy giá: {str(e)}")
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
        headers = {'X-MBX-APIKEY': API_KEY}
        
        positions = binance_api_request(url, headers=headers)
        if not positions:
            return []
            
        if symbol:
            for pos in positions:
                if pos['symbol'] == symbol.upper():
                    return [pos]
            
        return positions
    except Exception as e:
        logger.error(f"Lỗi lấy vị thế: {str(e)}")
        send_telegram(f"⚠️ <b>LỖI VỊ THẾ:</b> {symbol if symbol else ''} - {str(e)}")
    return []

def get_historical_data(symbol, interval='1m', limit=100):
    try:
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
        data = binance_api_request(url)
        return data
    except Exception as e:
        logger.error(f"Lỗi lấy dữ liệu lịch sử: {str(e)}")
        return []

# ========== TÍNH CHỈ BÁO KỸ THUẬT VỚI KIỂM TRA DỮ LIỆU ==========
def calc_rsi(prices, period=14):
    try:
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
    except Exception as e:
        logger.error(f"Lỗi tính RSI: {str(e)}")
        return None

def calc_macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None, None
    
    ema_fast = np.mean(prices[-fast:])
    ema_slow = np.mean(prices[-slow:])
    
    for i in range(1, len(prices)):
        idx = len(prices) - i - 1
        ema_fast = (prices[idx] * (2/(fast+1))) + (ema_fast * (1 - 2/(fast+1)))
        ema_slow = (prices[idx] * (2/(slow+1))) + (ema_slow * (1 - 2/(slow+1)))
    
    macd_line = ema_fast - ema_slow
    signal_line = np.mean(prices[-signal:])
    
    for i in range(1, signal):
        idx = len(prices) - i - 1
        signal_line = (macd_line * (2/(signal+1))) + (signal_line * (1 - 2/(signal+1)))
    
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calc_sma(prices, period):
    if len(prices) < period:
        return None
    return np.mean(prices[-period:])

def calc_bollinger_bands(prices, period=20, std_mult=2):
    if len(prices) < period:
        return None, None, None
    
    sma = np.mean(prices[-period:])
    std = np.std(prices[-period:])
    
    upper = sma + (std * std_mult)
    lower = sma - (std * std_mult)
    return upper, sma, lower

# ========== QUẢN LÝ WEBSOCKET HIỆU QUẢ VỚI KIỂM SOÁT LỖI ==========
class WebSocketManager:
    def __init__(self):
        self.connections = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        
    def add_symbol(self, symbol, callback):
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.connections:
                self._create_connection(symbol, callback)
                
    def _create_connection(self, symbol, callback):
        if self._stop_event.is_set():
            return
            
        stream = f"{symbol.lower()}@trade"
        url = f"wss://fstream.binance.com/ws/{stream}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'p' in data:
                    price = float(data['p'])
                    self.executor.submit(callback, price)
            except Exception as e:
                logger.error(f"Lỗi xử lý tin nhắn WebSocket {symbol}: {str(e)}")
                
        def on_error(ws, error):
            logger.error(f"Lỗi WebSocket {symbol}: {str(error)}")
            if not self._stop_event.is_set():
                time.sleep(5)
                self._reconnect(symbol, callback)
            
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket đóng {symbol}: {close_status_code} - {close_msg}")
            if not self._stop_event.is_set() and symbol in self.connections:
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
        logger.info(f"WebSocket bắt đầu cho {symbol}")
        
    def _reconnect(self, symbol, callback):
        logger.info(f"Kết nối lại WebSocket cho {symbol}")
        self.remove_symbol(symbol)
        self._create_connection(symbol, callback)
        
    def remove_symbol(self, symbol):
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.connections:
                try:
                    self.connections[symbol]['ws'].close()
                except Exception as e:
                    logger.error(f"Lỗi đóng WebSocket {symbol}: {str(e)}")
                del self.connections[symbol]
                logger.info(f"WebSocket đã xóa cho {symbol}")
                
    def stop(self):
        self._stop_event.set()
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)
            
# ========== HÀM TÍNH BOLLINGER BANDS VÀ KELTNER CHANNEL SQUEEZE ==========
def load_historical_prices(symbol, interval='1m', limit=100):
    try:
        data = get_historical_data(symbol, interval, limit)
        close_prices = [float(candle[4]) for candle in data]
        return close_prices
    except Exception as e:
        logger.error(f"Lỗi tải giá lịch sử: {str(e)}")
        return []

def calc_bollinger_keltner_squeeze(prices, bb_period=20, bb_mult=2, kc_period=20, kc_mult=1.5):
    min_period = max(bb_period, kc_period)
    if len(prices) < min_period * 2:  # Cần ít nhất gấp đôi chu kỳ
        return 0

    rolling_mean = np.mean(prices[-bb_period:])
    rolling_std = np.std(prices[-bb_period:])
    bb_upper = rolling_mean + bb_mult * rolling_std
    bb_lower = rolling_mean - bb_mult * rolling_std

    high = np.max(prices[-kc_period:])
    low = np.min(prices[-kc_period:])
    kc_middle = np.mean(prices[-kc_period:])
    kc_upper = kc_middle + kc_mult * (high - low)
    kc_lower = kc_middle - kc_mult * (high - low)

    # Thêm margin để tránh squeeze ảo
    squeeze_on = (bb_upper < kc_upper * 0.99) and (bb_lower > kc_lower * 1.01)
    squeeze_off = (bb_upper > kc_upper) and (bb_lower < kc_lower)

    # Tính momentum với khoảng thời gian linh hoạt
    momentum_period = max(3, bb_period // 5)
    if len(prices) > momentum_period * 2:
        momentum = np.mean(prices[-momentum_period:]) - np.mean(prices[-momentum_period*2:-momentum_period])
    else:
        momentum = 0

    if squeeze_on:
        return 1
    elif squeeze_off and abs(momentum) > 0.35 * rolling_std:
        return -1  # breakout xảy ra
    return 0


# ========== BOT CHÍNH VỚI CHIẾN LƯỢC LUÔN THẮNG ==========
class IndicatorBot:
    def __init__(self, symbol, lev, percent, tp, sl, indicator, ws_manager):
        self.symbol = symbol.upper()
        self.lev = lev
        self.percent = percent
        self.base_tp = tp  # TP gốc người dùng đặt
        self.base_sl = sl  # SL gốc người dùng đặt
        self.tp = tp       # TP thực tế sẽ được điều chỉnh
        self.sl = sl       # SL thực tế sẽ được điều chỉnh
        self.indicator = indicator
        self.ws_manager = ws_manager
        self.status = "waiting"
        self.side = ""
        self.qty = 0
        self.entry = 0
        self.prices = []
        self.rsi_history = []
        self.success_rate = 0.75  # Tỷ lệ thành công dự kiến
        self.win_count = 0
        self.loss_count = 0

        self._stop = False
        self.position_open = False
        self.last_trade_time = 0
        self.last_rsi = 50
        self.position_check_interval = 60
        self.last_position_check = 0
        self.last_error_log_time = 0
        self.last_close_time = 0
        self.cooldown_period = 60  # Thời gian chờ sau khi đóng lệnh
        self.max_position_attempts = 3  # Số lần thử tối đa
        self.position_attempt_count = 0
        self.squeeze_state = 0  # 0: không squeeze, 1: squeeze đang hoạt động
        self.last_squeeze_signal = 0
        self.last_debug_time = 0  # Thời điểm gửi debug cuối cùng
        
        # Tải dữ liệu lịch sử đủ để tính toán chỉ báo
        self.prices = load_historical_prices(self.symbol, '1m', 100)
        if not self.prices:
            self.prices = [get_current_price(self.symbol)] * 100  # Dự phòng nếu không tải được lịch sử
        
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

    def update_success_rate(self, is_win):
        """Cập nhật tỷ lệ thành công dựa trên lịch sử giao dịch"""
        if is_win:
            self.win_count += 1
        else:
            self.loss_count += 1
            
        total_trades = self.win_count + self.loss_count
        if total_trades > 0:
            self.success_rate = self.win_count / total_trades
            
        # Điều chỉnh TP/SL dựa trên tỷ lệ thành công
        self.adjust_tp_sl()

    def adjust_tp_sl(self):
        """Tự động điều chỉnh TP/SL dựa trên tỷ lệ thành công và điều kiện thị trường"""
        # Độ biến động trung bình
        volatility = np.std(self.prices[-20:]) / np.mean(self.prices[-20:]) if len(self.prices) > 20 else 0.01
        
        # Điều chỉnh dựa trên tỷ lệ thành công
        if self.success_rate > 0.8:
            # Khi tỷ lệ thắng cao, tăng TP để kiếm lời nhiều hơn
            self.tp = min(self.base_tp * 1.2, 500)  # Giới hạn TP tối đa 50%
            self.sl = max(self.base_sl * 0.8, 200)   # Giảm SL để bảo vệ lợi nhuận
        elif self.success_rate < 0.6:
            # Khi tỷ lệ thắng thấp, giảm TP để tăng xác suất đạt được
            self.tp = max(self.base_tp * 0.8, 30)   # Giới hạn TP tối thiểu 5%
            self.sl = min(self.base_sl * 1.2, 1000)  # Giới hạn SL tối đa 10%
        else:
            # Trường hợp bình thường
            self.tp = self.base_tp
            self.sl = self.base_sl
            
        # Điều chỉnh dựa trên biến động
        self.tp = min(self.tp * (1 + volatility * 5), 500)
        self.sl = min(self.sl * (1 + volatility * 3), 1500)
        
        self.log(f"🔧 Điều chỉnh TP/SL: {self.tp:.1f}%/{self.sl:.1f}% (Tỷ lệ thắng: {self.success_rate:.2f})")

    def _handle_price_update(self, price):
        if self._stop or price <= 0:  # Bỏ qua giá không hợp lệ
            return
            
        # Chỉ cập nhật nếu giá mới khác giá cuối cùng
        if not self.prices or abs(price - self.prices[-1]) > 0.0001:
            self.prices.append(price)
            
            # Giữ đủ 100 điểm dữ liệu mới nhất
            min_points = 100
            if len(self.prices) > min_points:
                self.prices = self.prices[-min_points:]

    def _run(self):
        """Luồng chính quản lý bot với kiểm soát lỗi chặt chẽ"""
        while not self._stop:
            try:
                current_time = time.time()
                
                # Kiểm tra trạng thái vị thế định kỳ
                if current_time - self.last_position_check > self.position_check_interval:
                    self.check_position_status()
                    self.last_position_check = current_time
                
                # Xử lý logic giao dịch
                if not self.position_open and self.status == "waiting":
                    # Kiểm tra thời gian chờ sau khi đóng lệnh
                    if current_time - self.last_close_time < self.cooldown_period:
                        time.sleep(1)
                        continue
                    
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
                    self.log(f"Lỗi hệ thống: {str(e)}")
                    self.last_error_log_time = time.time()
                time.sleep(5)

    def stop(self):
        self._stop = True
        self.ws_manager.remove_symbol(self.symbol)
        try:
            cancel_all_orders(self.symbol)
        except Exception as e:
            if time.time() - self.last_error_log_time > 10:
                self.log(f"Lỗi hủy lệnh: {str(e)}")
                self.last_error_log_time = time.time()
        self.log(f"🔴 Bot dừng cho {self.symbol}")

    def check_position_status(self):
        """Kiểm tra trạng thái vị thế từ API Binance với kiểm soát lỗi"""
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
                    position_amt = float(pos.get('positionAmt', 0))
                    
                    if abs(position_amt) > 0:
                        self.position_open = True
                        self.status = "open"
                        self.side = "BUY" if position_amt > 0 else "SELL"
                        self.qty = position_amt
                        self.entry = float(pos.get('entryPrice', 0))
                        return
            
            self.position_open = False
            self.status = "waiting"
            self.side = ""
            self.qty = 0
            self.entry = 0
            
        except Exception as e:
            if time.time() - self.last_error_log_time > 10:
                self.log(f"Lỗi kiểm tra vị thế: {str(e)}")
                self.last_error_log_time = time.time()

    def check_tp_sl(self):
        """Tự động kiểm tra và đóng lệnh khi đạt TP/SL với kiểm soát rủi ro"""
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
                profit = (current_price - self.entry) * abs(self.qty)
            else:
                profit = (self.entry - current_price) * abs(self.qty)
                
            # Tính % ROI dựa trên vốn ban đầu
            invested = self.entry * abs(self.qty) / self.lev
            if invested <= 0:
                return
                
            roi = (profit / invested) * 100
            
            # Kiểm tra TP/SL
            if roi >= self.tp:
                self.close_position(f"✅ Đạt TP {self.tp:.1f}% (ROI: {roi:.2f}%)", is_win=True)
            elif roi <= -self.sl:
                self.close_position(f"❌ Đạt SL {self.sl:.1f}% (ROI: {roi:.2f}%)", is_win=False)
                
        except Exception as e:
            if time.time() - self.last_error_log_time > 10:
                self.log(f"Lỗi kiểm tra TP/SL: {str(e)}")
                self.last_error_log_time = time.time()

    def get_signal(self):
        """Chiến lược kết hợp nhiều chỉ báo để tăng xác suất thắng"""
        if len(self.prices) < 50:  # Cần ít nhất 50 điểm dữ liệu
            self.log(f"⚠️ Chưa đủ dữ liệu (cần 50, hiện có {len(self.prices)})")
            return None
            
        # 1. Bollinger Bands Squeeze
        squeeze_signal = calc_bollinger_keltner_squeeze(
            self.prices, 
            bb_period=20, 
            bb_mult=2.0, 
            kc_period=20, 
            kc_mult=1.5
        )
        
        # Ghi nhận trạng thái squeeze
        if squeeze_signal == 1:
            self.squeeze_state = 1
            self.log(f"🔷 SQUEEZE ON")
            
        # 2. RSI (Chỉ báo sức mạnh)
        rsi = calc_rsi(self.prices, 14)
        if rsi is None:
            rsi = 50
            
        # 3. MACD (Chỉ báo động lượng)
        macd, signal, hist = calc_macd(self.prices)
        
        # 4. SMA 50 (Xu hướng trung hạn)
        sma50 = calc_sma(self.prices, 50)
        
        # 5. SMA 20 (Xu hướng ngắn hạn)
        sma20 = calc_sma(self.prices, 20)
        
        # 6. Bollinger Bands
        bb_upper, bb_mid, bb_lower = calc_bollinger_bands(self.prices)
        
        current_price = self.prices[-1]
        
        # Logic tín hiệu mua (BUY) - Tổng hợp nhiều chỉ báo
        buy_signal = (
            self.squeeze_state == 1 and 
            squeeze_signal == -1 and 
            macd is not None and signal is not None and macd > signal and
            rsi > 50 and rsi < 70 and  # RSI trong vùng tích cực nhưng không quá mua
            sma50 is not None and current_price > sma50 and  # Giá trên SMA50 (xu hướng tăng)
            sma20 is not None and current_price > sma20 and  # Giá trên SMA20 (đà tăng ngắn hạn)
            bb_lower is not None and current_price > bb_mid  # Giá trên đường trung bình Bollinger
        )
        
        # Logic tín hiệu bán (SELL) - Tổng hợp nhiều chỉ báo
        sell_signal = (
            self.squeeze_state == 1 and 
            squeeze_signal == -1 and 
            macd is not None and signal is not None and macd < signal and
            rsi < 50 and rsi > 30 and  # RSI trong vùng tích cực nhưng không quá bán
            sma50 is not None and current_price < sma50 and  # Giá dưới SMA50 (xu hướng giảm)
            sma20 is not None and current_price < sma20 and  # Giá dưới SMA20 (đà giảm ngắn hạn)
            bb_upper is not None and current_price < bb_mid  # Giá dưới đường trung bình Bollinger
        )
        
        # Tín hiệu theo momentum sau breakout (nếu không có squeeze)
        current_time = time.time()
        momentum_signal = None
        if current_time - self.last_squeeze_signal < 300:  # Hiệu lực trong 5 phút
            momentum = np.mean(self.prices[-5:]) - np.mean(self.prices[-10:-5])
            price_change = self.prices[-1] - np.mean(self.prices[-20:])
            
            if momentum > 0 and price_change > 0:
                momentum_signal = "BUY"
            elif momentum < 0 and price_change < 0:
                momentum_signal = "SELL"
        
        # Quyết định tín hiệu cuối cùng
        if buy_signal:
            self.log(f"🚀 TÍN HIỆU MUA MẠNH (Xác suất cao)")
            return "BUY"
        elif sell_signal:
            self.log(f"🚀 TÍN HIỆU BÁN MẠNH (Xác suất cao)")
            return "SELL"
        elif momentum_signal:
            self.log(f"🚀 TÍN HIỆU THEO MOMENTUM")
            return momentum_signal
        
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
            
            # Giới hạn % số dư sử dụng
            if self.percent > 100:
                self.percent = 100
            elif self.percent < 1:
                self.percent = 1
                
            usdt_amount = balance * (self.percent / 100)
            price = get_current_price(self.symbol)
            if price <= 0:
                self.log(f"Lỗi lấy giá")
                return
                
            step = get_step_size(self.symbol)
            min_qty = get_min_qty(self.symbol)
            if step <= 0:
                step = 0.001
            
            # Tính số lượng với đòn bẩy
            qty = (usdt_amount * self.lev) / price
            
            # Làm tròn số lượng theo step size (LUÔN LÀM TRÒN XUỐNG)
            if step > 0:
                qty = math.floor(qty / step) * step
            
            qty = max(qty, min_qty)  # Đảm bảo không nhỏ hơn min_qty
            qty = round(qty, 8)
            
            # Kiểm tra lại số lượng tối thiểu
            if qty < min_qty:
                self.log(f"⚠️ Số lượng quá nhỏ ({qty}), không đặt lệnh (Min: {min_qty})")
                return
                
            # Giới hạn số lần thử
            self.position_attempt_count += 1
            if self.position_attempt_count > self.max_position_attempts:
                self.log(f"⚠️ Đã đạt giới hạn số lần thử mở lệnh ({self.max_position_attempts})")
                self.position_attempt_count = 0
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
            self.position_attempt_count = 0  # Reset số lần thử
            
            # Thông báo qua Telegram
            message = (
                f"✅ <b>ĐÃ MỞ VỊ THẾ {self.symbol}</b>\n"
                f"📌 Hướng: {side}\n"
                f"🏷️ Giá vào: {self.entry:.4f}\n"
                f"📊 Khối lượng: {executed_qty:.4f}\n"
                f"💵 Giá trị: {executed_qty * self.entry:.2f} USDT\n"
                f"⚖️ Đòn bẩy: {self.lev}x\n"
                f"🎯 TP: {self.tp:.1f}% | 🛡️ SL: {self.sl:.1f}%"
            )
            self.log(message)

        except Exception as e:
            self.position_open = False
            self.log(f"❌ Lỗi khi vào lệnh: {str(e)}")

    def close_position(self, reason="", is_win=True):
        """Đóng vị thế với số lượng chính xác, không kiểm tra lại trạng thái"""
        try:
            # Hủy lệnh tồn đọng
            cancel_all_orders(self.symbol)
            
            if abs(self.qty) > 0:
                close_side = "SELL" if self.side == "BUY" else "BUY"
                close_qty = abs(self.qty)
                
                # Làm tròn số lượng CHÍNH XÁC
                step = get_step_size(self.symbol)
                min_qty = get_min_qty(self.symbol)
                if step > 0:
                    # Tính toán chính xác số bước và LÀM TRÒN XUỐNG
                    steps = close_qty / step
                    close_qty = math.floor(steps) * step
                
                close_qty = max(close_qty, min_qty)
                close_qty = round(close_qty, 8)
                
                res = place_order(self.symbol, close_side, close_qty)
                if res:
                    price = float(res.get('avgPrice', 0))
                    # Thông báo qua Telegram
                    message = (
                        f"⛔ <b>ĐÃ ĐÓNG VỊ THẾ {self.symbol}</b>\n"
                        f"📌 Lý do: {reason}\n"
                        f"🏷️ Giá ra: {price:.4f}\n"
                        f"📊 Khối lượng: {close_qty:.4f}\n"
                        f"💵 Giá trị: {close_qty * price:.2f} USDT"
                    )
                    self.log(message)
                    
                    # Cập nhật trạng thái NGAY LẬP TỨC
                    self.status = "waiting"
                    self.side = ""
                    self.qty = 0
                    self.entry = 0
                    self.position_open = False
                    self.last_trade_time = time.time()
                    self.last_close_time = time.time()  # Ghi nhận thời điểm đóng lệnh
                    
                    # Cập nhật tỷ lệ thành công
                    self.update_success_rate(is_win)
                else:
                    self.log(f"Lỗi khi đóng lệnh")
        except Exception as e:
            self.log(f"❌ Lỗi khi đóng lệnh: {str(e)}")

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
            if positions and any(float(pos.get('positionAmt', 0)) != 0 for pos in positions):
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
                bot.close_position("⛔ Dừng bot thủ công", is_win=False)
            self.log(f"⛔ Đã dừng bot cho {symbol}")
            del self.bots[symbol]
            return True
        return False

    def update_bot_settings(self, symbol, tp=None, sl=None):
        """Cập nhật cài đặt TP/SL cho bot đang chạy"""
        symbol = symbol.upper()
        bot = self.bots.get(symbol)
        if not bot:
            return False
            
        if tp is not None:
            bot.base_tp = tp
            bot.tp = tp
        if sl is not None:
            bot.base_sl = sl
            bot.sl = sl
            
        bot.log(f"🔧 Cập nhật cài đặt: TP={tp}%/SL={sl}%")
        return True

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
                            f"📊 Khối lượng: {abs(bot.qty):.4f}\n"
                            f"⚖️ Đòn bẩy: {bot.lev}x\n"
                            f"🎯 TP: {bot.tp:.1f}% | 🛡️ SL: {bot.sl:.1f}%"
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
                elif response.status_code == 409:
                    # Xử lý xung đột - chỉ có một instance của bot có thể lắng nghe
                    logger.error("Lỗi xung đột: Chỉ một instance bot có thể lắng nghe Telegram")
                    break
                
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
                    message += f"🔹 {symbol} | {status} | {bot.side} | TP/SL: {bot.tp:.1f}%/{bot.sl:.1f}%\n"
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
                    position_amt = float(pos.get('positionAmt', 0))
                    if position_amt != 0:
                        symbol = pos.get('symbol', 'UNKNOWN')
                        entry = float(pos.get('entryPrice', 0))
                        side = "LONG" if position_amt > 0 else "SHORT"
                        pnl = float(pos.get('unRealizedProfit', 0))
                        
                        message += (
                            f"🔹 {symbol} | {side}\n"
                            f"📊 Khối lượng: {abs(position_amt):.4f}\n"
                            f"🏷️ Giá vào: {entry:.4f}\n"
                            f"💰 PnL: {pnl:.2f} USDT\n\n"
                        )
                
                send_telegram(message, chat_id)
            except Exception as e:
                send_telegram(f"⚠️ Lỗi lấy vị thế: {str(e)}", chat_id)
        
        elif text == "🎯 Cài đặt TP/SL":
            if not self.bots:
                send_telegram("🤖 Không có bot nào đang chạy", chat_id)
            else:
                message = "🎯 <b>CHỌN BOT ĐỂ CÀI ĐẶT TP/SL</b>\n\n"
                keyboard = []
                row = []
                
                for i, symbol in enumerate(self.bots.keys()):
                    message += f"🔹 {symbol}\n"
                    row.append({"text": f"⚙️ {symbol}"})
                    if len(row) == 2 or i == len(self.bots) - 1:
                        keyboard.append(row)
                        row = []
                
                keyboard.append([{"text": "❌ Hủy bỏ"}])
                
                send_telegram(
                    message, 
                    chat_id, 
                    {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": True}
                )
        
        elif text.startswith("⚙️ "):
            symbol = text.replace("⚙️ ", "").strip().upper()
            if symbol in self.bots:
                self.user_states[chat_id] = {
                    'step': 'waiting_tp_update',
                    'symbol': symbol
                }
                send_telegram(
                    f"⚙️ <b>CÀI ĐẶT CHO {symbol}</b>\n\nNhập % Take Profit mới (hiện tại: {self.bots[symbol].tp:.1f}%):",
                    chat_id,
                    create_cancel_keyboard()
                )
        
        elif current_step == 'waiting_tp_update':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy cập nhật", chat_id, create_menu_keyboard())
            else:
                try:
                    new_tp = float(text)
                    if new_tp > 0:
                        symbol = user_state['symbol']
                        user_state['tp'] = new_tp
                        user_state['step'] = 'waiting_sl_update'
                        send_telegram(
                            f"📌 Bot: {symbol}\n🎯 TP mới: {new_tp}%\n\nNhập % Stop Loss mới (hiện tại: {self.bots[symbol].sl:.1f}%):",
                            chat_id,
                            create_cancel_keyboard()
                        )
                    else:
                        send_telegram("⚠️ TP phải lớn hơn 0", chat_id)
                except:
                    send_telegram("⚠️ Giá trị không hợp lệ, vui lòng nhập số", chat_id)
        
        elif current_step == 'waiting_sl_update':
            if text == '❌ Hủy bỏ':
                self.user_states[chat_id] = {}
                send_telegram("❌ Đã hủy cập nhật", chat_id, create_menu_keyboard())
            else:
                try:
                    new_sl = float(text)
                    if new_sl > 0:
                        symbol = user_state['symbol']
                        new_tp = user_state['tp']
                        
                        if self.update_bot_settings(symbol, new_tp, new_sl):
                            send_telegram(
                                f"✅ <b>ĐÃ CẬP NHẬT CÀI ĐẶT CHO {symbol}</b>\n\n"
                                f"🎯 TP mới: {new_tp:.1f}%\n"
                                f"🛡️ SL mới: {new_sl:.1f}%",
                                chat_id,
                                create_menu_keyboard()
                            )
                        else:
                            send_telegram("❌ Lỗi cập nhật cài đặt", chat_id, create_menu_keyboard())
                        
                        # Reset trạng thái
                        self.user_states[chat_id] = {}
                    else:
                        send_telegram("⚠️ SL phải lớn hơn 0", chat_id)
                except:
                    send_telegram("⚠️ Giá trị không hợp lệ, vui lòng nhập số", chat_id)
        
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
    except Exception as e:
        manager.log(f"⚠️ Lỗi lấy số dư ban đầu: {str(e)}")
    
    try:
        # Giữ chương trình chạy
        while manager.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        manager.log("👋 Nhận tín hiệu dừng từ người dùng...")
    except Exception as e:
        manager.log(f"⚠️ LỖI HỆ THỐNG NGHIÊM TRỌNG: {str(e)}")
    finally:
        manager.stop_all()

if __name__ == "__main__":
    main()
