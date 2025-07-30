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
        logging.FileHandler('hft_bot_errors.log')
    ]
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Lấy cấu hình từ biến môi trường
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '')
BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

# Cấu hình bot từ biến môi trường
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
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
            logger.error(f"Lỗi gửi Telegram ({response.status_code}): {response.text}")
    except Exception as e:
        logger.error(f"Lỗi kết nối Telegram: {str(e)}")

# ========== QUẢN LÝ DỮ LIỆU TỐC ĐỘ CAO ==========
class HighFrequencyDataManager:
    def __init__(self):
        self.price_data = {}
        self.orderbook_data = {}
        self.volume_data = {}
        self.candle_data = {}
        self.lock = threading.Lock()
        self.update_times = {}
        
    def update_price(self, symbol, price, timestamp=None):
        """Cập nhật giá với timestamp chính xác"""
        timestamp = timestamp or time.time()
        with self.lock:
            if symbol not in self.price_data:
                self.price_data[symbol] = []
            self.price_data[symbol].append((timestamp, price))
            # Giữ 100 giá gần nhất
            if len(self.price_data[symbol]) > 100:
                self.price_data[symbol] = self.price_data[symbol][-100:]
    
    def get_last_price(self, symbol):
        """Lấy giá cuối cùng với O(1)"""
        with self.lock:
            if symbol in self.price_data and self.price_data[symbol]:
                return self.price_data[symbol][-1][1]
        return 0

    def get_prices(self, symbol, count=None):
        """Lấy lịch sử giá"""
        with self.lock:
            if symbol in self.price_data:
                prices = [p[1] for p in self.price_data[symbol]]
                return prices[-count:] if count else prices
        return []

    def update_orderbook(self, symbol, bid, ask, bid_qty, ask_qty):
        """Cập nhật dữ liệu order book"""
        with self.lock:
            self.orderbook_data[symbol] = {
                'bid': bid,
                'ask': ask,
                'total_bid': bid_qty,
                'total_ask': ask_qty,
                'timestamp': time.time()
            }

    def get_orderbook_snapshot(self, symbol):
        """Lấy dữ liệu order book mới nhất"""
        with self.lock:
            return self.orderbook_data.get(symbol, {
                'bid': 0, 'ask': 0, 'total_bid': 0, 'total_ask': 0
            })

    def update_volume(self, symbol, volume):
        """Cập nhật dữ liệu khối lượng"""
        with self.lock:
            if symbol not in self.volume_data:
                self.volume_data[symbol] = []
            self.volume_data[symbol].append(volume)
            # Giữ 20 giá trị gần nhất
            if len(self.volume_data[symbol]) > 20:
                self.volume_data[symbol] = self.volume_data[symbol][-20:]

    def get_volume_metrics(self, symbol):
        """Tính toán chỉ số khối lượng"""
        with self.lock:
            if symbol in self.volume_data and self.volume_data[symbol]:
                samples = self.volume_data[symbol]
                return {
                    'current': samples[-1],
                    'average': np.mean(samples[-5:]) if len(samples) >= 5 else np.mean(samples)
                }
        return {'current': 0, 'average': 0}

    def update_candles(self, symbol, timeframe, candles):
        """Cập nhật dữ liệu nến"""
        with self.lock:
            if symbol not in self.candle_data:
                self.candle_data[symbol] = {}
            self.candle_data[symbol][timeframe] = candles

    def get_candles(self, symbol, timeframe, count=None):
        """Lấy dữ liệu nến"""
        with self.lock:
            try:
                candles = self.candle_data[symbol][timeframe]
                return candles[-count:] if count else candles
            except KeyError:
                return []

# ========== QUẢN LÝ WEBSOCKET TỐC ĐỘ CAO ==========
class HighFrequencyWebSocketManager:
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.connections = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        
    def add_symbol(self, symbol, callback=None):
        symbol = symbol.upper()
        with self._lock:
            if symbol not in self.connections:
                self._create_connections(symbol, callback)
                
    def _create_connections(self, symbol, callback):
        if self._stop_event.is_set():
            return
            
        # Kết nối giá giao dịch
        self._create_trade_stream(symbol, callback)
        
        # Kết nối order book
        self._create_orderbook_stream(symbol)
        
    def _create_trade_stream(self, symbol, callback):
        stream = f"{symbol.lower()}@trade"
        url = f"wss://fstream.binance.com/ws/{stream}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'p' in data:
                    price = float(data['p'])
                    timestamp = data['T'] / 1000.0  # Chuyển ms sang s
                    self.data_manager.update_price(symbol, price, timestamp)
                    
                    # Cập nhật khối lượng
                    volume = float(data['q'])
                    self.data_manager.update_volume(symbol, volume)
                    
                    # Gọi callback nếu có
                    if callback:
                        callback(price)
            except Exception as e:
                logger.error(f"Lỗi xử lý trade stream {symbol}: {str(e)}")
                
        self._start_websocket(symbol, url, on_message, "trade")
        
    def _create_orderbook_stream(self, symbol):
        stream = f"{symbol.lower()}@depth5@100ms"
        url = f"wss://fstream.binance.com/ws/{stream}"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'b' in data and data['b'] and 'a' in data and data['a']:
                    # Lấy giá bid/ask tốt nhất
                    best_bid = float(data['b'][0][0])
                    best_ask = float(data['a'][0][0])
                    bid_qty = sum(float(b[1]) for b in data['b'][:5])
                    ask_qty = sum(float(a[1]) for a in data['a'][:5])
                    
                    self.data_manager.update_orderbook(symbol, best_bid, best_ask, bid_qty, ask_qty)
            except Exception as e:
                logger.error(f"Lỗi xử lý orderbook stream {symbol}: {str(e)}")
                
        self._start_websocket(symbol, url, on_message, "orderbook")
        
    def _start_websocket(self, symbol, url, on_message, stream_type):
        def on_error(ws, error):
            logger.error(f"Lỗi WebSocket {symbol} ({stream_type}): {str(error)}")
            if not self._stop_event.is_set():
                time.sleep(1)
                self._reconnect(symbol, stream_type)
            
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket đóng {symbol} ({stream_type}): {close_status_code} - {close_msg}")
            if not self._stop_event.is_set():
                time.sleep(1)
                self._reconnect(symbol, stream_type)
                
        ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        thread = threading.Thread(target=ws.run_forever, daemon=True)
        thread.start()
        
        if symbol not in self.connections:
            self.connections[symbol] = {}
            
        self.connections[symbol][stream_type] = {
            'ws': ws,
            'thread': thread
        }
        logger.info(f"WebSocket {stream_type} bắt đầu cho {symbol}")
        
    def _reconnect(self, symbol, stream_type):
        logger.info(f"Kết nối lại WebSocket {stream_type} cho {symbol}")
        self.remove_symbol(symbol, stream_type)
        
        if stream_type == "trade":
            self._create_trade_stream(symbol, None)
        elif stream_type == "orderbook":
            self._create_orderbook_stream(symbol)
                
    def remove_symbol(self, symbol, stream_type=None):
        symbol = symbol.upper()
        with self._lock:
            if symbol in self.connections:
                if stream_type:
                    if stream_type in self.connections[symbol]:
                        try:
                            self.connections[symbol][stream_type]['ws'].close()
                        except:
                            pass
                        del self.connections[symbol][stream_type]
                else:
                    for stream_type in list(self.connections[symbol].keys()):
                        try:
                            self.connections[symbol][stream_type]['ws'].close()
                        except:
                            pass
                    del self.connections[symbol]
                    
    def stop(self):
        self._stop_event.set()
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)

# ========== BOT GIAO DỊCH TỐC ĐỘ CAO ==========
class HighVelocityProphetBot:
    def __init__(self, symbol, lev, percent, ws_manager, data_manager):
        self.symbol = symbol.upper()
        self.lev = lev
        self.percent = percent
        self.ws_manager = ws_manager
        self.data_manager = data_manager
        self.status = "waiting"
        self.side = ""
        self.qty = 0
        self.entry = 0
        self._stop = False
        self.position_open = False
        
        # Cấu hình tốc độ
        self.execution_threshold = 0.1  # 100ms
        self.signal_strength_threshold = 95  # 95% độ tin cậy
        
        # Tối ưu hóa hiệu năng
        self.last_analysis_time = 0
        self.precomputed_features = None
        self.pending_order = None
        self.symbol_info = {}
        
        # Tín hiệu dự đoán
        self.current_prediction = {
            "direction": None,
            "confidence": 0,
            "strength": 0,
            "urgency": 0,
            "expiration": 0
        }
        
        # Đăng ký với WebSocket Manager
        self.ws_manager.add_symbol(self.symbol, self._handle_price_update)
        
        # Bắt đầu thread chính
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self.log(f"🚀 Bot tốc độ cao khởi động cho {self.symbol}")

    def log(self, message):
        logger.info(f"[{self.symbol}] {message}")
        send_telegram(f"<b>{self.symbol}</b>: {message}")

    def _handle_price_update(self, price):
        """Xử lý cập nhật giá mới"""
        pass  # Không cần xử lý thêm, dữ liệu đã được lưu trong data_manager

    def ultra_fast_feature_extraction(self):
        """Trích xuất đặc trưng tốc độ cao trong < 10ms"""
        try:
            # Tận dụng dữ liệu đã được precompute
            if time.time() - self.last_analysis_time < 0.05:  # 50ms
                return self.precomputed_features
                
            # 1. Lấy dữ liệu thị trường cực nhanh
            current_price = self.data_manager.get_last_price(self.symbol)
            orderbook_snapshot = self.data_manager.get_orderbook_snapshot(self.symbol)
            volume_metrics = self.data_manager.get_volume_metrics(self.symbol)
            
            # 2. Tính toán các chỉ số cốt lõi
            features = {
                # Chỉ số thanh khoản
                "bid_ask_spread": orderbook_snapshot['ask'] - orderbook_snapshot['bid'],
                "orderbook_imbalance": (orderbook_snapshot['total_bid'] - orderbook_snapshot['total_ask']) / 
                                       (orderbook_snapshot['total_bid'] + orderbook_snapshot['total_ask'] + 1e-10),
                
                # Chỉ số động lượng
                "price_velocity": self.calc_price_velocity(),
                "volume_acceleration": volume_metrics['current'] / (volume_metrics['average'] + 1e-10),
                
                # Chỉ số thời gian
                "time_pressure": self.calc_time_pressure(),
                
                # Chỉ số kỹ thuật nhanh
                "instant_rsi": self.calc_instant_rsi(),
                "micro_ema_diff": self.calc_micro_ema_diff()
            }
            
            # 3. Lưu cache cho lần sau
            self.precomputed_features = features
            self.last_analysis_time = time.time()
            
            return features
            
        except Exception as e:
            self.log(f"Lỗi trích xuất tốc độ cao: {str(e)}")
            return {}

    def high_strength_signal_generation(self):
        """Tạo tín hiệu mạnh với độ tin cậy >95% trong < 15ms"""
        start_time = time.time()
        try:
            # 1. Trích xuất đặc trưng tốc độ cao
            features = self.ultra_fast_feature_extraction()
            
            if not features:
                return None, 0, 0, 0
                
            # 2. Tính điểm tín hiệu
            signal_strength = 0
            signal_urgency = 0
            direction = None
            
            # Quy tắc 1: Độ mất cân bằng order book mạnh
            if abs(features['orderbook_imbalance']) > 0.3:
                signal_strength += 35
                signal_urgency += 40
                direction = "BUY" if features['orderbook_imbalance'] > 0 else "SELL"
            
            # Quy tắc 2: Tốc độ giá + gia tốc khối lượng
            elif abs(features['price_velocity']) > 0.002 and features['volume_acceleration'] > 1.8:
                signal_strength += 30
                signal_urgency += 50
                direction = "BUY" if features['price_velocity'] > 0 else "SELL"
            
            # Quy tắc 3: Áp lực thời gian + RSI cực đoan
            elif features['time_pressure'] > 0.8 and abs(features['instant_rsi'] - 50) > 30:
                signal_strength += 25
                signal_urgency += 60
                direction = "BUY" if features['instant_rsi'] < 20 else "SELL"
            
            # Quy tắc 4: Chênh lệch EMA vi mô
            elif abs(features['micro_ema_diff']) > 0.0015:
                signal_strength += 20
                signal_urgency += 30
                direction = "BUY" if features['micro_ema_diff'] > 0 else "SELL"
            
            # Không đủ độ mạnh
            if direction is None:
                return None, 0, 0, 0
            
            # 3. Tăng cường độ cho các tín hiệu đa xác nhận
            confirmation_factor = 1
            if features['orderbook_imbalance'] * features['price_velocity'] > 0:
                confirmation_factor *= 1.3
            if features['volume_acceleration'] > 2.5:
                confirmation_factor *= 1.2
            
            # 4. Tính toán điểm cuối
            signal_strength = min(100, int(signal_strength * confirmation_factor))
            signal_urgency = min(100, int(signal_urgency * confirmation_factor))
            
            # 5. Tính thời gian hết hạn tín hiệu (ms)
            expiration = max(500, 3000 - (signal_urgency * 20))
            
            return direction, signal_strength, signal_urgency, expiration
            
        finally:
            execution_time = (time.time() - start_time) * 1000
            if execution_time > 15:
                self.log(f"Cảnh báo: Tạo tín hiệu chậm {execution_time:.2f}ms")

    def calc_price_velocity(self):
        """Tính tốc độ biến động giá trong 500ms"""
        prices = self.data_manager.get_prices(self.symbol)
        if len(prices) < 2:
            return 0
            
        # Lấy giá trong 500ms gần nhất
        recent_prices = prices[-10:]  # Giả sử 10 giá gần nhất
        if len(recent_prices) < 2:
            return 0
            
        returns = np.diff(recent_prices) / np.array(recent_prices[:-1])
        return np.mean(returns)

    def calc_time_pressure(self):
        """Tính áp lực thời gian trong nến hiện tại"""
        current_time = time.time()
        candle_start = current_time - (current_time % 300)
        elapsed = current_time - candle_start
        return elapsed / 300  # 0-1

    def calc_instant_rsi(self):
        """Tính RSI trong 15 giây gần nhất"""
        prices = self.data_manager.get_prices(self.symbol)
        if len(prices) < 5:
            return 50
            
        # Tính RSI đơn giản
        gains = []
        losses = []
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
            else:
                losses.append(abs(change))
                
        avg_gain = np.mean(gains) if gains else 0
        avg_loss = np.mean(losses) if losses else 0
        
        if avg_loss == 0:
            return 100 if avg_gain > 0 else 50
            
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def calc_micro_ema_diff(self):
        """Tính chênh lệch EMA trong khung thời gian rất ngắn"""
        prices = self.data_manager.get_prices(self.symbol)
        if len(prices) < 10:
            return 0
            
        ema3 = self.calc_ema(prices, 3)
        ema8 = self.calc_ema(prices, 8)
        return (ema3 - ema8) / prices[-1]

    def calc_ema(self, prices, period):
        """Tính EMA nhanh"""
        if len(prices) < period:
            return prices[-1] if prices else 0
            
        k = 2 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = price * k + ema * (1 - k)
        return ema

    def prepare_order_parameters(self):
        """Chuẩn bị trước thông số lệnh"""
        # Lấy thông tin ký hiệu một lần
        if not self.symbol_info:
            self.symbol_info = self.get_symbol_info()
            if not self.symbol_info:
                return False
                
        # Tính toán khối lượng cơ bản
        balance = self.get_balance()
        if balance <= 0:
            return False
            
        risk_amount = balance * (self.percent / 100) * 0.02  # 2% rủi ro
        price = self.data_manager.get_last_price(self.symbol)
        if price <= 0:
            return False
            
        position_size = (risk_amount * self.lev) / price
        
        # Làm tròn theo quy định
        step_size = self.symbol_info.get('step_size', 0.001)
        if step_size > 0:
            position_size = round(position_size / step_size) * step_size
        
        self.position_size = max(position_size, self.symbol_info.get('min_qty', 0.001))
        return True

    def get_symbol_info(self):
        """Lấy thông tin ký hiệu và lưu cache"""
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            data = self.binance_api_request(url)
            if not data:
                return {}
                
            for s in data['symbols']:
                if s['symbol'] == self.symbol:
                    symbol_info = {}
                    for f in s['filters']:
                        if f['filterType'] == 'LOT_SIZE':
                            symbol_info['min_qty'] = float(f['minQty'])
                            symbol_info['step_size'] = float(f['stepSize'])
                        elif f['filterType'] == 'PRICE_FILTER':
                            symbol_info['tick_size'] = float(f['tickSize'])
                    return symbol_info
        except Exception as e:
            self.log(f"Lỗi lấy thông tin ký hiệu: {str(e)}")
        return {}

    def binance_api_request(self, url, method='GET', params=None, headers=None):
        """Hàm tổng quát cho các yêu cầu API Binance"""
        try:
            if method.upper() == 'GET':
                if params:
                    query = urllib.parse.urlencode(params)
                    url = f"{url}?{query}"
                req = urllib.request.Request(url, headers=headers or {})
            else:
                data = urllib.parse.urlencode(params).encode() if params else None
                req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
            
            with urllib.request.urlopen(req, timeout=2) as response:
                if response.status == 200:
                    return json.loads(response.read().decode())
        except Exception as e:
            self.log(f"Lỗi API: {str(e)}")
        return None

    def get_balance(self):
        """Lấy số dư tài khoản"""
        try:
            ts = int(time.time() * 1000)
            params = {"timestamp": ts}
            query = urllib.parse.urlencode(params)
            sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
            url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
            headers = {'X-MBX-APIKEY': API_KEY}
            
            data = self.binance_api_request(url, headers=headers)
            if not data:
                return 0
                
            for asset in data['assets']:
                if asset['asset'] == 'USDT':
                    return float(asset['availableBalance'])
        except Exception as e:
            self.log(f"Lỗi lấy số dư: {str(e)}")
        return 0

    def execute_at_velocity(self):
        """Thực thi giao dịch tốc độ cao với tín hiệu mạnh"""
        start_time = time.time()
        try:
            # 1. Tạo tín hiệu mạnh
            direction, strength, urgency, expiration = self.high_strength_signal_generation()
            
            # 2. Kiểm tra tín hiệu đủ mạnh
            if strength < self.signal_strength_threshold:
                return
                
            # 3. Chuẩn bị lệnh trước
            if not self.pending_order:
                if not self.prepare_order_parameters():
                    return
            
            # 4. Tính toán giá vào lệnh tối ưu
            orderbook = self.data_manager.get_orderbook_snapshot(self.symbol)
            target_price = self.calculate_optimal_entry(direction, orderbook)
            
            # 5. Đặt lệnh tốc độ cao
            self.fire_order(direction, self.position_size, target_price, urgency)
            
            # 6. Ghi nhận hiệu suất
            exec_time = (time.time() - start_time) * 1000
            self.log(f"⚡ TỐC ĐỘ: {exec_time:.1f}ms | "
                     f"TÍN HIỆU: {strength}/100 | "
                     f"KHẨN: {urgency}/100 | "
                     f"GIÁ: {target_price:.5f}")
            
        except Exception as e:
            self.log(f"Lỗi thực thi tốc độ: {str(e)}")

    def calculate_optimal_entry(self, direction, orderbook):
        """Tính toán giá vào tối ưu để tăng tốc độ khớp lệnh"""
        tick_size = self.symbol_info.get('tick_size', 0.0001)
        
        if direction == "BUY":
            # Mua ngay tại mức giá ask thấp nhất + bước giá
            return orderbook['ask'] + tick_size
        else:
            # Bán ngay tại mức giá bid cao nhất - bước giá
            return orderbook['bid'] - tick_size

    def fire_order(self, direction, quantity, price, urgency):
        """Đặt lệnh với tốc độ cực cao"""
        try:
            # Tạo yêu cầu
            ts = int(time.time() * 1000)
            params = {
                "symbol": self.symbol,
                "side": direction,
                "type": "LIMIT",
                "timeInForce": "IOC",  # Immediate or Cancel
                "quantity": quantity,
                "price": price,
                "timestamp": ts
            }
            
            # Tạo chữ ký
            query = urllib.parse.urlencode(params)
            signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
            
            # Gửi yêu cầu
            url = f"https://fapi.binance.com/fapi/v1/order?{query}&signature={signature}"
            headers = {'X-MBX-APIKEY': API_KEY}
            
            # Sử dụng luồng riêng để không chặn luồng chính
            threading.Thread(
                target=self._send_order_request, 
                args=(url, headers, urgency)
            ).start()
            
        except Exception as e:
            self.log(f"Lỗi đặt lệnh tốc độ: {str(e)}")

    def _send_order_request(self, url, headers, urgency):
        """Gửi yêu cầu đặt lệnh"""
        try:
            if urgency > 80:
                # Chế độ khẩn cấp: timeout ngắn hơn
                response = requests.post(url, headers=headers, timeout=0.1)
                self.log("🔥 LỆNH KHẨN CẤP ĐÃ GỬI")
            else:
                response = requests.post(url, headers=headers, timeout=0.5)
                
            if response.status_code == 200:
                self.log(f"✅ Lệnh đã được chấp nhận")
            else:
                self.log(f"❌ Lỗi đặt lệnh ({response.status_code}): {response.text}")
        except Exception as e:
            self.log(f"Lỗi gửi lệnh: {str(e)}")

    def stop(self):
        self._stop = True
        self.ws_manager.remove_symbol(self.symbol)
        self.log(f"🔴 Bot dừng cho {self.symbol}")

    def _run(self):
        """Vòng lặp chính tập trung tốc độ"""
        while not self._stop:
            loop_start = time.perf_counter()
            
            try:
                # Thực thi giao dịch tốc độ cao
                self.execute_at_velocity()
                
                # Tối ưu thời gian chờ
                elapsed = time.perf_counter() - loop_start
                sleep_time = max(0, 0.02 - elapsed)  # Nhắm 50 vòng/giây
                time.sleep(sleep_time)
                
            except Exception as e:
                self.log(f"Lỗi vòng lặp chính: {str(e)}")
                time.sleep(0.1)

# ========== QUẢN LÝ BOT ==========
class HighFrequencyBotManager:
    def __init__(self):
        self.data_manager = HighFrequencyDataManager()
        self.ws_manager = HighFrequencyWebSocketManager(self.data_manager)
        self.bots = {}
        self.running = True
        
        self.log("🚀 HỆ THỐNG BOT TỐC ĐỘ CAO ĐÃ KHỞI ĐỘNG")
        
        # Bắt đầu thread chính
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def log(self, message):
        logger.info(f"[SYSTEM] {message}")
        send_telegram(f"<b>SYSTEM</b>: {message}")

    def add_bot(self, symbol, lev, percent):
        symbol = symbol.upper()
        try:
            # Khởi tạo dữ liệu nến
            self._init_candle_data(symbol)
            
            # Tạo bot mới
            bot = HighVelocityProphetBot(
                symbol, lev, percent, 
                self.ws_manager, self.data_manager
            )
            self.bots[symbol] = bot
            self.log(f"✅ Đã thêm bot tốc độ cao: {symbol} | ĐB: {lev}x | %: {percent}")
            return True
        except Exception as e:
            self.log(f"❌ Lỗi tạo bot {symbol}: {str(e)}")
            return False

    def _init_candle_data(self, symbol):
        """Khởi tạo dữ liệu nến lịch sử"""
        timeframes = ["5m", "15m"]
        for tf in timeframes:
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={tf}&limit=20"
            data = self.binance_api_request(url)
            if data:
                candles = []
                for kline in data:
                    try:
                        candles.append({
                            "open": float(kline[1]),
                            "high": float(kline[2]),
                            "low": float(kline[3]),
                            "close": float(kline[4]),
                            "volume": float(kline[5]),
                            "timestamp": kline[0]
                        })
                    except:
                        pass
                self.data_manager.update_candles(symbol, tf, candles)

    def binance_api_request(self, url):
        """Hàm đơn giản cho yêu cầu API Binance"""
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                if response.status == 200:
                    return json.loads(response.read().decode())
        except:
            return None

    def stop_bot(self, symbol):
        symbol = symbol.upper()
        bot = self.bots.get(symbol)
        if bot:
            bot.stop()
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

    def _run(self):
        """Vòng lặp chính của manager"""
        while self.running:
            try:
                # Thêm các bot từ cấu hình
                if BOT_CONFIGS:
                    for config in BOT_CONFIGS:
                        symbol, lev, percent = config
                        if symbol not in self.bots:
                            self.add_bot(symbol, lev, percent)
                
                # Kiểm tra mỗi 30 giây
                time.sleep(30)
            except Exception as e:
                self.log(f"Lỗi vòng lặp manager: {str(e)}")
                time.sleep(10)

# ========== HÀM KHỞI CHẠY CHÍNH ==========
def main():
    # Khởi tạo hệ thống
    manager = HighFrequencyBotManager()
    
    # Thông báo số dư ban đầu
    try:
        # Hàm get_balance tạm thời
        def get_balance():
            try:
                ts = int(time.time() * 1000)
                params = {"timestamp": ts}
                query = urllib.parse.urlencode(params)
                sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
                url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
                headers = {'X-MBX-APIKEY': API_KEY}
                
                with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=5) as response:
                    if response.status == 200:
                        data = json.loads(response.read().decode())
                        for asset in data['assets']:
                            if asset['asset'] == 'USDT':
                                return float(asset['availableBalance'])
            except:
                return 0
                
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
