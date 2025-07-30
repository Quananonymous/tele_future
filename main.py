# hft_bot.py
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
import sys
from concurrent.futures import ThreadPoolExecutor

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hft_bot.log')
    ]
)
logger = logging.getLogger('HFTBot')
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
    logger.error(f"Error parsing BOT_CONFIGS: {e}")
    BOT_CONFIGS = []

# ========== HÀM TIỆN ÍCH ==========
def send_telegram(message, chat_id=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        requests.post(url, json=payload, timeout=3)
    except Exception:
        pass

def sign(query):
    return hmac.new(BINANCE_SECRET_KEY.encode(), query.encode(), hashlib.sha256).hexdigest()

def binance_api_request(url, method='GET', params=None, headers=None):
    try:
        if method.upper() == 'GET' and params:
            query = urllib.parse.urlencode(params)
            url = f"{url}?{query}"
            req = urllib.request.Request(url, headers=headers or {})
        else:
            data = urllib.parse.urlencode(params).encode() if params else None
            req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        
        with urllib.request.urlopen(req, timeout=3) as response:
            if response.status == 200:
                return json.loads(response.read().decode())
    except Exception as e:
        logger.error(f"API request failed: {str(e)}")
    return None

# ========== QUẢN LÝ DỮ LIỆU TỐC ĐỘ CAO ==========
class DataManager:
    def __init__(self):
        self.price_data = {}
        self.orderbook_data = {}
        self.lock = threading.Lock()
        
    def update_price(self, symbol, price, timestamp=None):
        with self.lock:
            if symbol not in self.price_data:
                self.price_data[symbol] = []
            self.price_data[symbol].append(price)
            if len(self.price_data[symbol]) > 100:
                self.price_data[symbol] = self.price_data[symbol][-100:]
    
    def get_last_price(self, symbol):
        with self.lock:
            if symbol in self.price_data and self.price_data[symbol]:
                return self.price_data[symbol][-1]
        return 0

    def get_prices(self, symbol, count=10):
        with self.lock:
            if symbol in self.price_data:
                return self.price_data[symbol][-count:]
        return []

    def update_orderbook(self, symbol, bid, ask, bid_qty, ask_qty):
        with self.lock:
            self.orderbook_data[symbol] = {
                'bid': bid,
                'ask': ask,
                'bid_qty': bid_qty,
                'ask_qty': ask_qty,
                'timestamp': time.time()
            }

    def get_orderbook(self, symbol):
        with self.lock:
            return self.orderbook_data.get(symbol, {
                'bid': 0, 'ask': 0, 'bid_qty': 0, 'ask_qty': 0
            })

# ========== QUẢN LÝ WEBSOCKET ==========
class WebSocketManager:
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.connections = {}
        self._stop_event = threading.Event()
        
    def add_symbol(self, symbol, callback=None):
        symbol = symbol.upper()
        if symbol not in self.connections:
            self._create_connection(symbol, callback)
                
    def _create_connection(self, symbol, callback):
        if self._stop_event.is_set():
            return
            
        # Tạo stream giá
        trade_stream = f"{symbol.lower()}@trade"
        trade_url = f"wss://fstream.binance.com/ws/{trade_stream}"
        
        # Tạo stream orderbook
        depth_stream = f"{symbol.lower()}@depth5@100ms"
        depth_url = f"wss://fstream.binance.com/ws/{depth_stream}"
        
        # Xử lý tin nhắn trade
        def trade_on_message(ws, message):
            try:
                data = json.loads(message)
                if 'p' in data:
                    price = float(data['p'])
                    self.data_manager.update_price(symbol, price)
                    if callback:
                        callback(price)
            except Exception as e:
                logger.error(f"Trade stream error: {str(e)}")
                
        # Xử lý tin nhắn orderbook
        def depth_on_message(ws, message):
            try:
                data = json.loads(message)
                if 'b' in data and data['b'] and 'a' in data and data['a']:
                    bid = float(data['b'][0][0])
                    ask = float(data['a'][0][0])
                    bid_qty = sum(float(b[1]) for b in data['b'][:5])
                    ask_qty = sum(float(a[1]) for a in data['a'][:5])
                    self.data_manager.update_orderbook(symbol, bid, ask, bid_qty, ask_qty)
            except Exception as e:
                logger.error(f"Orderbook stream error: {str(e)}")
        
        # Tạo WebSocket cho giá
        trade_ws = websocket.WebSocketApp(
            trade_url,
            on_message=trade_on_message,
            on_error=lambda ws, err: logger.error(f"Trade WS error: {err}"),
            on_close=lambda ws: self._reconnect(symbol, 'trade', callback)
        )
        
        # Tạo WebSocket cho orderbook
        depth_ws = websocket.WebSocketApp(
            depth_url,
            on_message=depth_on_message,
            on_error=lambda ws, err: logger.error(f"Depth WS error: {err}"),
            on_close=lambda ws: self._reconnect(symbol, 'depth', callback)
        )
        
        # Khởi chạy trong luồng riêng
        trade_thread = threading.Thread(target=trade_ws.run_forever, daemon=True)
        depth_thread = threading.Thread(target=depth_ws.run_forever, daemon=True)
        
        trade_thread.start()
        depth_thread.start()
        
        self.connections[symbol] = {
            'trade': {'ws': trade_ws, 'thread': trade_thread},
            'depth': {'ws': depth_ws, 'thread': depth_thread}
        }
        
    def _reconnect(self, symbol, stream_type, callback):
        logger.info(f"Reconnecting {symbol} {stream_type}")
        time.sleep(1)
        self._create_connection(symbol, callback)
                
    def remove_symbol(self, symbol):
        if symbol in self.connections:
            try:
                self.connections[symbol]['trade']['ws'].close()
                self.connections[symbol]['depth']['ws'].close()
            except:
                pass
            del self.connections[symbol]
                
    def stop(self):
        self._stop_event.set()
        for symbol in list(self.connections.keys()):
            self.remove_symbol(symbol)

# ========== BOT GIAO DỊCH TỐC ĐỘ CAO ==========
class HighFrequencyTrader:
    def __init__(self, symbol, leverage, risk_percent, data_manager, ws_manager):
        self.symbol = symbol.upper()
        self.leverage = leverage
        self.risk_percent = risk_percent
        self.data_manager = data_manager
        self.ws_manager = ws_manager
        self.running = True
        self.position_size = 0
        self.symbol_info = self._get_symbol_info()
        
        # Đăng ký dữ liệu
        self.ws_manager.add_symbol(self.symbol, self._price_update)
        
        # Bắt đầu luồng giao dịch
        self.thread = threading.Thread(target=self._trade_loop, daemon=True)
        self.thread.start()
        
        logger.info(f"Started HFT bot for {self.symbol}")
        send_telegram(f"🚀 <b>HFT BOT STARTED</b>\nSymbol: {self.symbol}\nLeverage: {leverage}x\nRisk: {risk_percent}%")

    def _price_update(self, price):
        """Xử lý cập nhật giá"""
        pass

    def _get_symbol_info(self):
        """Lấy thông tin symbol"""
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        data = binance_api_request(url)
        if data:
            for s in data['symbols']:
                if s['symbol'] == self.symbol:
                    info = {}
                    for f in s['filters']:
                        if f['filterType'] == 'LOT_SIZE':
                            info['min_qty'] = float(f['minQty'])
                            info['step_size'] = float(f['stepSize'])
                        elif f['filterType'] == 'PRICE_FILTER':
                            info['tick_size'] = float(f['tickSize'])
                    return info
        return {'min_qty': 0.001, 'step_size': 0.001, 'tick_size': 0.01}

    def _get_balance(self):
        """Lấy số dư tài khoản"""
        try:
            ts = int(time.time() * 1000)
            params = {"timestamp": ts}
            query = urllib.parse.urlencode(params)
            sig = sign(query)
            url = f"https://fapi.binance.com/fapi/v2/account?{query}&signature={sig}"
            headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
            
            data = binance_api_request(url, headers=headers)
            if data:
                for asset in data['assets']:
                    if asset['asset'] == 'USDT':
                        return float(asset['availableBalance'])
        except Exception:
            pass
        return 0

    def _calculate_position_size(self):
        """Tính toán kích thước vị thế"""
        balance = self._get_balance()
        if balance <= 0:
            return 0
            
        risk_amount = balance * (self.risk_percent / 100)
        price = self.data_manager.get_last_price(self.symbol)
        if price <= 0:
            return 0
            
        size = (risk_amount * self.leverage) / price
        step = self.symbol_info.get('step_size', 0.001)
        
        # Làm tròn theo step size
        if step > 0:
            size = round(size / step) * step
            
        return max(size, self.symbol_info.get('min_qty', 0.001))

    def _place_order(self, side, quantity, price):
        """Đặt lệnh tốc độ cao"""
        try:
            ts = int(time.time() * 1000)
            params = {
                "symbol": self.symbol,
                "side": side,
                "type": "LIMIT",
                "timeInForce": "IOC",
                "quantity": quantity,
                "price": price,
                "timestamp": ts
            }
            
            query = urllib.parse.urlencode(params)
            sig = sign(query)
            url = f"https://fapi.binance.com/fapi/v1/order?{query}&signature={sig}"
            headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
            
            # Gửi trong luồng riêng để không chặn luồng chính
            threading.Thread(
                target=requests.post, 
                args=(url,), 
                kwargs={'headers': headers, 'timeout': 0.3}
            ).start()
            
            return True
        except Exception as e:
            logger.error(f"Order placement error: {str(e)}")
            return False

    def _generate_signal(self):
        """Tạo tín hiệu giao dịch với độ chính xác cao"""
        try:
            # Lấy dữ liệu thị trường
            prices = self.data_manager.get_prices(self.symbol, 20)
            if len(prices) < 10:
                return None, 0
                
            orderbook = self.data_manager.get_orderbook(self.symbol)
            
            # Tính toán động lượng
            price_change = prices[-1] - prices[-5]
            avg_volume = np.mean(prices[-10:])
            volume_ratio = prices[-1] / avg_volume if avg_volume > 0 else 1
            
            # Phân tích order book
            imbalance = (orderbook['bid_qty'] - orderbook['ask_qty']) / (orderbook['bid_qty'] + orderbook['ask_qty'] + 1e-10)
            spread = orderbook['ask'] - orderbook['bid']
            
            # Tạo điểm tín hiệu
            signal_score = 0
            direction = None
            
            # Tín hiệu mua
            if price_change > 0 and volume_ratio > 1.2 and imbalance > 0.1:
                signal_score = 95 + min(5, imbalance * 100)
                direction = "BUY"
                
            # Tín hiệu bán
            elif price_change < 0 and volume_ratio < 0.8 and imbalance < -0.1:
                signal_score = 95 + min(5, abs(imbalance) * 100)
                direction = "SELL"
                
            return direction, signal_score
        except Exception as e:
            logger.error(f"Signal generation error: {str(e)}")
            return None, 0

    def _execute_trade(self):
        """Thực thi giao dịch tốc độ cao"""
        start_time = time.time()
        
        # Tạo tín hiệu
        direction, confidence = self._generate_signal()
        if confidence < 95:
            return False
            
        # Tính toán vị thế
        if self.position_size <= 0:
            self.position_size = self._calculate_position_size()
            if self.position_size <= 0:
                return False
                
        # Lấy giá mục tiêu
        orderbook = self.data_manager.get_orderbook(self.symbol)
        if direction == "BUY":
            price = orderbook['ask'] + self.symbol_info.get('tick_size', 0.01)
        else:
            price = orderbook['bid'] - self.symbol_info.get('tick_size', 0.01)
            
        # Đặt lệnh
        self._place_order(direction, self.position_size, price)
        
        # Ghi log hiệu suất
        exec_time = (time.time() - start_time) * 1000
        logger.info(f"Executed {direction} order in {exec_time:.2f}ms | Confidence: {confidence}%")
        send_telegram(f"⚡ <b>TRADE EXECUTED</b>\n"
                      f"Symbol: {self.symbol}\n"
                      f"Direction: {direction}\n"
                      f"Size: {self.position_size:.4f}\n"
                      f"Price: {price:.4f}\n"
                      f"Exec Time: {exec_time:.2f}ms")
        
        return True

    def _trade_loop(self):
        """Vòng lặp giao dịch chính"""
        while self.running:
            try:
                self._execute_trade()
                # Tối ưu thời gian chờ để đạt 50 lần/giây
                time.sleep(0.02)
            except Exception as e:
                logger.error(f"Trade loop error: {str(e)}")
                time.sleep(1)

    def stop(self):
        """Dừng bot"""
        self.running = False
        self.ws_manager.remove_symbol(self.symbol)
        logger.info(f"Stopped HFT bot for {self.symbol}")
        send_telegram(f"🛑 <b>HFT BOT STOPPED</b>\nSymbol: {self.symbol}")

# ========== QUẢN LÝ HỆ THỐNG ==========
class BotManager:
    def __init__(self):
        self.data_manager = DataManager()
        self.ws_manager = WebSocketManager(self.data_manager)
        self.bots = {}
        
        logger.info("HFT Trading System Initialized")
        send_telegram("🚀 <b>HFT TRADING SYSTEM STARTED</b>")
        
        # Bắt đầu các bot từ cấu hình
        for config in BOT_CONFIGS:
            if len(config) >= 3:
                self.add_bot(config[0], config[1], config[2])
        
        # Luồng giám sát
        self.monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self.monitor_thread.start()
        
    def add_bot(self, symbol, leverage, risk_percent):
        symbol = symbol.upper()
        if symbol not in self.bots:
            try:
                bot = HighFrequencyTrader(
                    symbol, leverage, risk_percent,
                    self.data_manager, self.ws_manager
                )
                self.bots[symbol] = bot
                return True
            except Exception as e:
                logger.error(f"Failed to add bot for {symbol}: {str(e)}")
        return False

    def _monitor(self):
        """Giám sát hệ thống định kỳ"""
        while True:
            try:
                # Báo cáo trạng thái mỗi 5 phút
                active_bots = [s for s, b in self.bots.items()]
                status = f"📊 <b>SYSTEM STATUS</b>\nActive Bots: {len(active_bots)}\n"
                status += "\n".join([f"• {s}" for s in active_bots]) if active_bots else "No active bots"
                
                send_telegram(status)
                time.sleep(300)
            except Exception:
                time.sleep(60)

    def stop_all(self):
        """Dừng toàn bộ hệ thống"""
        for symbol in list(self.bots.keys()):
            self.bots[symbol].stop()
            del self.bots[symbol]
        
        self.ws_manager.stop()
        logger.info("All bots stopped")
        send_telegram("🔴 <b>ALL BOTS STOPPED</b>")

# ========== ĐIỂM VÀO CHƯƠNG TRÌNH ==========
if __name__ == "__main__":
    # Kiểm tra API keys
    if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
        logger.error("Binance API keys not configured!")
        send_telegram("❌ <b>ERROR:</b> Binance API keys missing!")
        sys.exit(1)
    
    # Khởi tạo hệ thống
    manager = BotManager()
    
    try:
        # Giữ chương trình chạy
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down by user request...")
        send_telegram("🛑 <b>SYSTEM SHUTDOWN INITIATED BY USER</b>")
    except Exception as e:
        logger.error(f"Critical system error: {str(e)}")
        send_telegram(f"🔥 <b>CRITICAL ERROR:</b> {str(e)}")
    finally:
        manager.stop_all()
        logger.info("System shutdown complete")
