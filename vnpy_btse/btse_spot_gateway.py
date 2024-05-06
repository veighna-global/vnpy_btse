import hashlib
import hmac
import json
import time
from datetime import datetime
from types import TracebackType

from requests import Response

from vnpy_evo.event import EventEngine
from vnpy_evo.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status
)
from vnpy_evo.trader.gateway import BaseGateway
from vnpy_evo.trader.utility import ZoneInfo
from vnpy_evo.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData
)
from vnpy_rest import Request, RestClient
from vnpy_websocket import WebsocketClient


# Timezone
UTC_TZ: ZoneInfo = ZoneInfo("UTC")

# Real server hosts
REAL_REST_HOST: str = "https://api.btse.com/spot"
REAL_WEBSOCKET_HOST: str = "wss://ws.btse.com/ws/spot"
REAL_ORDERBOOK_HOST: str = "wss://ws.btse.com/ws/oss/spot"

REAL_FUTURES_REST_HOST: str = "https://api.btse.com/futures"

# Testnet server hosts
TESTNET_REST_HOST: str = "https://testapi.btse.io/spot"
TESTNET_WEBSOCKET_HOST: str = "wss://testws.btse.io/ws/spot"
TESTNET_ORDERBOOK_HOST: str = "wss://testws.btse.io/ws/oss/spot"

TESTNET_FUTURES_REST_HOST: str = "https://testapi.btse.io/futures"

# Order status map
STATUS_BTSE2VT: dict[str, Status] = {
    2: Status.NOTTRADED,
    5: Status.PARTTRADED,
    4: Status.ALLTRADED,
    6: Status.CANCELLED,
    7: Status.CANCELLED
}

# Order type map
ORDERTYPE_BTSE2VT: dict[str, OrderType] = {
    76: OrderType.LIMIT,
    77: OrderType.MARKET
}
ORDERTYPE_VT2BTSE: dict[OrderType, str] = {v: k for k, v in ORDERTYPE_BTSE2VT.items()}

# Direction map
DIRECTION_BTSE2VT: dict[str, Direction] = {
    "BUY": Direction.LONG,
    "SELL": Direction.SHORT,
    "MODE_BUY": Direction.LONG,
    "MODE_SELL": Direction.SHORT
}
DIRECTION_VT2BTSE: dict[Direction, str] = {v: k for k, v in DIRECTION_BTSE2VT.items()}

# Kline interval map
INTERVAL_VT2BTSE: dict[Interval, str] = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60",
    Interval.DAILY: "1440",
}

# Global dict for contract data
symbol_contract_map: dict[str, ContractData] = {}

# Global map for local and sys order id
local_sys_map: dict[str, str] = {}
sys_local_map: dict[str, str] = {}


class BtseGateway(BaseGateway):
    """
    The BTSE spot trading gateway for VeighNa.
    """

    default_name = "BTSE_SPOT"

    default_setting: dict = {
        "API Key": "b4afb5fef43d94814c8ca4f0155e734c4e367d83d51471e6dd240554888af17a",
        "Secret Key": "0044e478db40fcd3a62e7d2970f8c27d43417d7cb3d025bd3c512ff3c085b643",
        "Server": ["REAL", "TESTNET"],
        "Proxy Host": "",
        "Proxy Port": "",
    }

    exchanges: Exchange = [Exchange.BTSE]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.spot_rest_api: SpotRestApi = SpotRestApi(self)
        self.spot_ob_api: SpotOrderbookApi = SpotOrderbookApi(self)
        self.spot_ws_api: SpotWebsocketApi = SpotWebsocketApi(self)

        self.futures_rest_api: FuturesRestApi = FuturesRestApi(self)
        self.futures_ob_api: FuturesOrderbookApi = FuturesOrderbookApi(self)
        self.futures_ws_api: FuturesWebsocketApi = FuturesWebsocketApi(self)

        self.orders: dict[str, OrderData] = {}
        self.ticks: dict[str, TickData] = {}

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["Secret Key"]
        server: str = setting["Server"]
        proxy_host: str = setting["Proxy Host"]
        proxy_port: str = setting["Proxy Port"]

        if proxy_port.isdigit():
            proxy_port = int(proxy_port)
        else:
            proxy_port = 0

        self.spot_rest_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )
        self.futures_rest_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )

        self.spot_ob_api.connect(
            server,
            proxy_host,
            proxy_port,
        )
        self.spot_ws_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port,
        )

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        if req.symbol in self.ticks:
            return

        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(),
            gateway_name=self.gateway_name
        )
        self.ticks[req.symbol] = tick

        self.spot_ws_api.subscribe_market_trade(req)
        self.spot_ob_api.subscribe_orderbook(req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.spot_ws_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.spot_ws_api.cancel_order(req)

    def query_account(self) -> None:
        """Not required since BTSE provides websocket update"""
        pass

    def query_position(self) -> None:
        """Not required since BTSE provides websocket update"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        return self.spot_rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections"""
        self.spot_rest_api.stop()
        self.spot_ob_api.stop()
        self.spot_ws_api.stop()

    def on_order(self, order: OrderData) -> None:
        """Save a copy of order and then pus"""
        self.orders[order.orderid] = order
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """Get previously saved order"""
        return self.orders.get(orderid, None)

    def on_tick(self, tick: TickData) -> None:
        """Save a copy of tick and then push"""
        self.ticks[tick.symbol] = tick
        super().on_tick(tick)

    def get_tick(self, symbol: str) -> TickData:
        """Get previously saved tick"""
        return self.ticks.get(symbol, None)


class SpotRestApi(RestClient):
    """The REST API of BtseGateway"""

    def __init__(self, gateway: BtseGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

    def sign(self, request: Request) -> Request:
        """Standard callback for signing a request"""
        # Generate signature
        timestamp: str = str(int(time.time() * 1000))

        body: str = ""
        if request.data:
            body = json.dumps(request.data)

        msg: str = f"{request.path}{timestamp}{body}"
        signature: bytes = generate_signature(msg, self.secret)

        # Add request header
        request.headers = {
            "request-api": self.key,
            "request-nonce": timestamp,
            "request-sign": signature,
            "Accept": "application/json;charset=UTF-8",
            "Content-Type": "application/json"
        }

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        server_hosts: dict[str, str] = {
            "REAL": REAL_REST_HOST,
            "TESTNET": TESTNET_REST_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port)

        self.start()
        self.gateway.write_log("REST API started")

        self.query_time()
        self.query_order()
        self.query_account()
        self.query_contract()

    def query_time(self) -> None:
        """Query server time"""
        self.add_request(
            "GET",
            "/api/v3.2/time",
            callback=self.on_query_time
        )

    def query_order(self) -> None:
        """Query open orders"""
        self.add_request(
            "GET",
            "/api/v3.2/user/open_orders",
            callback=self.on_query_order,
        )

    def query_account(self) -> None:
        """Query account balance"""
        self.add_request(
            "GET",
            "/api/v3.2/user/wallet",
            callback=self.on_query_account,
        )

    def query_contract(self) -> None:
        """Query available contract"""
        self.add_request(
            "GET",
            "/api/v3.2/market_summary",
            callback=self.on_query_contract
        )

    def on_query_time(self, packet: dict, request: Request) -> None:
        """Callback of server time query"""
        timestamp: int = packet["epoch"]
        server_time: datetime = datetime.fromtimestamp(timestamp / 1000)
        local_time: datetime = datetime.now()

        msg: str = f"Server time: {server_time}, local time: {local_time}"
        self.gateway.write_log(msg)

    def on_query_order(self, packet: dict, request: Request) -> None:
        """Callback of open orders query"""
        for d in packet:
            local_id: str = d["clOrderID"]
            sys_id: str = d["orderID"]

            local_sys_map[local_id] = sys_id
            sys_local_map[sys_id] = local_id

            order: OrderData = OrderData(
                symbol=d["symbol"],
                exchange=Exchange.BTSE,
                type=ORDERTYPE_BTSE2VT[d["orderType"]],
                orderid=local_id,
                direction=DIRECTION_BTSE2VT[d["side"]],
                offset=Offset.NONE,
                traded=d["filledSize"],
                price=d["price"],
                volume=d["size"],
                datetime=parse_timestamp(d["timestamp"]),
                gateway_name=self.gateway_name,
            )

            if d["orderState"] == "STATUS_ACTIVE":
                if not order.traded:
                    order.status = Status.NOTTRADED
                else:
                    order.status = Status.PARTTRADED

            self.gateway.on_order(order)

        self.gateway.write_log("Open orders data is received")

    def on_query_account(self, packet: dict, request: Request) -> None:
        """Callback of account balance query"""
        for d in packet:
            if not d["total"]:
                continue

            account: AccountData = AccountData(
                accountid=d["currency"],
                balance=d["total"],
                frozen=d["total"] - d["available"],
                gateway_name=self.gateway_name,
            )
            self.gateway.on_account(account)

    def on_query_contract(self, packet: dict, request: Request) -> None:
        """Callback of available contracts query"""
        for d in packet:
            symbol: str = d["symbol"]

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.BTSE,
                name=symbol,
                product=Product.SPOT,
                size=1,
                pricetick=float(d["minPriceIncrement"]),
                min_volume=float(d["minSizeIncrement"]),
                history_data=True,
                net_position=True,
                gateway_name=self.gateway_name,
            )

            self.gateway.on_contract(contract)

        self.gateway.write_log("Available contracts data is received")

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb: TracebackType,
        request: Request
    ) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb, request)

        msg: str = f"Exception catched by REST API: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        buf: dict[datetime, BarData] = {}
        end_time: str = ""
        path: str = "/api/v5/market/candles"

        for i in range(15):
            # Create query params
            params: dict = {
                "instId": req.symbol,
                "bar": INTERVAL_VT2BTSE[req.interval]
            }

            if end_time:
                params["after"] = end_time

            # Get response from server
            resp: Response = self.request(
                "GET",
                path,
                params=params
            )

            # Break loop if request is failed
            if resp.status_code // 100 != 2:
                msg = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()

                if not data["data"]:
                    m = data["msg"]
                    msg = f"No kline history data is received, {m}"
                    break

                for bar_list in data["data"]:
                    ts, o, h, l, c, vol, _ = bar_list
                    dt = parse_timestamp(ts)
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(vol),
                        open_price=float(o),
                        high_price=float(h),
                        low_price=float(l),
                        close_price=float(c),
                        gateway_name=self.gateway_name
                    )
                    buf[bar.datetime] = bar

                begin: str = data["data"][-1][0]
                end: str = data["data"][0][0]
                msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {parse_timestamp(begin)} - {parse_timestamp(end)}"
                self.gateway.write_log(msg)

                # Update end time
                end_time = begin

        index: list[datetime] = list(buf.keys())
        index.sort()

        history: list[BarData] = [buf[i] for i in index]
        return history


class SpotOrderbookApi(WebsocketClient):
    """The public websocket API of BtseGateway"""

    def __init__(self, gateway: BtseGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.callbacks: dict[str, callable] = {}

    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        server_hosts: dict[str, str] = {
            "REAL": REAL_ORDERBOOK_HOST,
            "TESTNET": TESTNET_ORDERBOOK_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port, 20)

        self.start()

    def subscribe_orderbook(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        topic: str = f"snapshotL1:{req.symbol}_0"
        self.callbacks[topic] = self.on_orderbook

        btse_req: dict = {
            "op": "subscribe",
            "args": [topic]
        }
        self.send_packet(btse_req)

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Orderbook API is connected")

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Orderbook websocket API is disconnected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        if "errors" in packet:
            for d in packet["errors"]:
                error: dict = d["error"]
                error_code: int = error["code"]
                error_message: str = error["message"]

                msg: str = f"Request caused error by orderbook API, code: {error_code}, message: {error_message}"
                self.gateway.write_log(msg)

            return
        elif "event" in packet:
            event: str = packet["event"]
            callback: callable = self.callbacks.get(event, None)
            if callback:
                callback(packet)
        elif "topic" in packet:
            topic: str = packet["topic"]
            callback: callable = self.callbacks.get(topic, None)
            if callback:
                callback(packet)
        else:
            print(packet)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb)

        msg: str = f"Exception catched by orderbook API: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def on_orderbook(self, packet: dict) -> None:
        """Callback of market trade update"""
        print(packet)
        data: dict = packet["data"]

        symbol: str = data["symbol"]
        tick: TickData = self.gateway.get_tick(symbol)

        bid_data: tuple = data["bids"][0]
        tick.bid_price_1 = float(bid_data[0])
        tick.bid_volume_1 = float(bid_data[1])

        ask_data: tuple = data["asks"][0]
        tick.ask_price_1 = float(ask_data[0])
        tick.ask_volume_1 = float(ask_data[1])

        tick.datetime = parse_timestamp(data["timestamp"])

        self.gateway.on_tick(tick)


class SpotWebsocketApi(WebsocketClient):
    """The private websocket API of BtseGateway"""

    def __init__(self, gateway: BtseGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.callbacks: dict[str, callable] = {
            "login": self.on_login,
            "notificationApiV2": self.on_order,
            "fills": self.on_trade
        }

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        server_hosts: dict[str, str] = {
            "REAL": REAL_WEBSOCKET_HOST,
            "TESTNET": TESTNET_WEBSOCKET_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port, 20)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Private websocket API is connected")
        self.login()

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Private websocket API is disconnected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        print(packet)
        if "errors" in packet:
            for d in packet["errors"]:
                error: dict = d["error"]
                error_code: int = error["code"]
                error_message: str = error["message"]

                msg: str = f"Request caused error by websocket API, code: {error_code}, message: {error_message}"
                self.gateway.write_log(msg)

            return
        elif "event" in packet:
            event: str = packet["event"]
            callback: callable = self.callbacks.get(event, None)
            if callback:
                callback(packet)
        elif "topic" in packet:
            topic: str = packet["topic"]
            callback: callable = self.callbacks.get(topic, None)
            if callback:
                callback(packet)
        else:
            print(packet)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb)

        msg: str = f"Exception catched by websocket API: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def on_login(self, packet: dict) -> None:
        """Callback of user login"""
        self.gateway.write_log("Websocket API login successful")

        self.subscribe_topic()

    def on_order(self, packet: dict) -> None:
        """Callback of order update"""
        data: dict = packet["data"]

        local_id: str = data["clOrderID"]
        sys_id: str = data["orderID"]

        local_sys_map[local_id] = sys_id
        sys_local_map[sys_id] = local_id

        order: OrderData = OrderData(
            symbol=data["symbol"],
            exchange=Exchange.BTSE,
            type=ORDERTYPE_BTSE2VT[data["type"]],
            orderid=local_id,
            direction=DIRECTION_BTSE2VT[data["side"]],
            offset=Offset.NONE,
            traded=data["fillSize"],
            price=data["price"],
            volume=data["size"],
            status=STATUS_BTSE2VT.get(data["status"], Status.SUBMITTING),
            datetime=parse_timestamp(data["timestamp"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_order(order)

    def on_trade(self, packet: dict) -> None:
        """Callback of trade update"""
        for data in packet["data"]:
            trade: TradeData = TradeData(
                symbol=data["symbol"],
                exchange=Exchange.BTSE,
                orderid=data["clOrderId"],
                tradeid=data["tradeId"],
                direction=DIRECTION_BTSE2VT[data["side"]],
                offset=Offset.NONE,
                price=data["price"],
                volume=data["size"],
                datetime=parse_timestamp(data["timestamp"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)

    def on_market_trade(self, packet: dict) -> None:
        """Callback of market trade update"""
        for data in packet["data"]:
            symbol: str = data["symbol"]
            tick: TickData = self.gateway.get_tick(symbol)

            tick.last_price = data["price"]
            tick.last_volume = data["size"]
            tick.datetime = parse_timestamp(data["timestamp"])

            self.gateway.on_tick(tick)

    def login(self) -> None:
        """User login"""
        timestamp: str = str(int(time.time() * 1000))
        msg: str = f"/ws/spot{timestamp}"
        signature: str = generate_signature(msg, self.secret)

        btse_req: dict = {
            "op": "authKeyExpires",
            "args": [self.key, timestamp, signature]
        }
        self.send_packet(btse_req)

    def subscribe_topic(self) -> None:
        """Subscribe topics"""
        btse_req: dict = {
            "op": "subscribe",
            "args": ["notificationApiV2", "fills"]
        }
        self.send_packet(btse_req)

    def subscribe_market_trade(self, req: SubscribeRequest) -> None:
        """Subscribe market trade fill"""
        topic: str = f"tradeHistoryApi:{req.symbol}"
        self.callbacks[topic] = self.on_market_trade

        btse_req: dict = {
            "op": "subscribe",
            "args": [topic]
        }
        self.send_packet(btse_req)


class FuturesRestApi(RestClient):
    """The REST API of BtseGateway"""

    def __init__(self, gateway: BtseGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

    def sign(self, request: Request) -> Request:
        """Standard callback for signing a request"""
        # Generate signature
        timestamp: str = str(int(time.time() * 1000))

        body: str = ""
        if request.data:
            body = json.dumps(request.data)

        msg: str = f"{request.path}{timestamp}{body}"
        signature: bytes = generate_signature(msg, self.secret)

        # Add request header
        request.headers = {
            "request-api": self.key,
            "request-nonce": timestamp,
            "request-sign": signature,
            "Accept": "application/json;charset=UTF-8",
            "Content-Type": "application/json"
        }

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        server_hosts: dict[str, str] = {
            "REAL": REAL_FUTURES_REST_HOST,
            "TESTNET": TESTNET_FUTURES_REST_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port)

        self.start()
        self.gateway.write_log("REST API started")

        self.query_order()
        self.query_position()
        self.query_contract()

    def query_order(self) -> None:
        """Query open orders"""
        self.add_request(
            "GET",
            "/api/v2.1/user/open_orders",
            callback=self.on_query_order,
        )

    def query_position(self) -> None:
        """Query holding positions"""
        self.add_request(
            "GET",
            "/api/v2.1/user/positions",
            callback=self.on_query_position,
        )

    def query_contract(self) -> None:
        """Query available contract"""
        self.add_request(
            "GET",
            "/api/v2.1/market_summary",
            callback=self.on_query_contract
        )

    def on_query_order(self, packet: dict, request: Request) -> None:
        """Callback of open orders query"""
        for d in packet:
            local_id: str = d["clOrderID"]
            sys_id: str = d["orderID"]

            local_sys_map[local_id] = sys_id
            sys_local_map[sys_id] = local_id

            order: OrderData = OrderData(
                symbol=d["symbol"],
                exchange=Exchange.BTSE,
                type=ORDERTYPE_BTSE2VT[d["orderType"]],
                orderid=local_id,
                direction=DIRECTION_BTSE2VT[d["side"]],
                offset=Offset.NONE,
                traded=d["filledSize"],
                price=d["price"],
                volume=d["size"],
                datetime=parse_timestamp(d["timestamp"]),
                gateway_name=self.gateway_name,
            )

            if d["orderState"] == "STATUS_ACTIVE":
                if not order.traded:
                    order.status = Status.NOTTRADED
                else:
                    order.status = Status.PARTTRADED

            self.gateway.on_order(order)

        self.gateway.write_log("Open orders data is received")

    def on_query_position(self, packet: dict, request: Request) -> None:
        """Callback of holding positions query"""
        for d in packet:
            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BTSE,
                direction=Direction.NET,
                volume=float(d["size"]),
                price=float(d["entryPrice"]),
                pnl=float(d["unrealizedProfitLoss"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)

    def on_query_contract(self, packet: dict, request: Request) -> None:
        """Callback of available contracts query"""
        for d in packet:
            symbol: str = d["symbol"]

            if "PFC" in symbol:
                product: Product = Product.SWAP
            else:
                product: Product = Product.FUTURES

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.BTSE,
                name=symbol,
                product=product,
                size=float(d["contractSize"]),
                pricetick=float(d["minPriceIncrement"]),
                min_volume=float(d["minSizeIncrement"]),
                history_data=True,
                net_position=True,
                gateway_name=self.gateway_name,
            )

            self.gateway.on_contract(contract)

        self.gateway.write_log("Available futures contracts data is received")

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb: TracebackType,
        request: Request
    ) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb, request)

        msg: str = f"Exception catched by futures REST API: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        buf: dict[datetime, BarData] = {}
        end_time: str = ""
        path: str = "/api/v5/market/candles"

        for i in range(15):
            # Create query params
            params: dict = {
                "instId": req.symbol,
                "bar": INTERVAL_VT2BTSE[req.interval]
            }

            if end_time:
                params["after"] = end_time

            # Get response from server
            resp: Response = self.request(
                "GET",
                path,
                params=params
            )

            # Break loop if request is failed
            if resp.status_code // 100 != 2:
                msg = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()

                if not data["data"]:
                    m = data["msg"]
                    msg = f"No kline history data is received, {m}"
                    break

                for bar_list in data["data"]:
                    ts, o, h, l, c, vol, _ = bar_list
                    dt = parse_timestamp(ts)
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(vol),
                        open_price=float(o),
                        high_price=float(h),
                        low_price=float(l),
                        close_price=float(c),
                        gateway_name=self.gateway_name
                    )
                    buf[bar.datetime] = bar

                begin: str = data["data"][-1][0]
                end: str = data["data"][0][0]
                msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {parse_timestamp(begin)} - {parse_timestamp(end)}"
                self.gateway.write_log(msg)

                # Update end time
                end_time = begin

        index: list[datetime] = list(buf.keys())
        index.sort()

        history: list[BarData] = [buf[i] for i in index]
        return history


class FuturesOrderbookApi(WebsocketClient):
    """The public websocket API of BtseGateway"""

    def __init__(self, gateway: BtseGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.callbacks: dict[str, callable] = {}

    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        server_hosts: dict[str, str] = {
            "REAL": REAL_ORDERBOOK_HOST,
            "TESTNET": TESTNET_ORDERBOOK_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port, 20)

        self.start()

    def subscribe_orderbook(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        topic: str = f"snapshotL1:{req.symbol}_0"
        self.callbacks[topic] = self.on_orderbook

        btse_req: dict = {
            "op": "subscribe",
            "args": [topic]
        }
        self.send_packet(btse_req)

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Orderbook API is connected")

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Orderbook websocket API is disconnected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        if "errors" in packet:
            for d in packet["errors"]:
                error: dict = d["error"]
                error_code: int = error["code"]
                error_message: str = error["message"]

                msg: str = f"Request caused error by orderbook API, code: {error_code}, message: {error_message}"
                self.gateway.write_log(msg)

            return
        elif "event" in packet:
            event: str = packet["event"]
            callback: callable = self.callbacks.get(event, None)
            if callback:
                callback(packet)
        elif "topic" in packet:
            topic: str = packet["topic"]
            callback: callable = self.callbacks.get(topic, None)
            if callback:
                callback(packet)
        else:
            print(packet)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb)

        msg: str = f"Exception catched by orderbook API: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def on_orderbook(self, packet: dict) -> None:
        """Callback of market trade update"""
        print(packet)
        data: dict = packet["data"]

        symbol: str = data["symbol"]
        tick: TickData = self.gateway.get_tick(symbol)

        bid_data: tuple = data["bids"][0]
        tick.bid_price_1 = float(bid_data[0])
        tick.bid_volume_1 = float(bid_data[1])

        ask_data: tuple = data["asks"][0]
        tick.ask_price_1 = float(ask_data[0])
        tick.ask_volume_1 = float(ask_data[1])

        tick.datetime = parse_timestamp(data["timestamp"])

        self.gateway.on_tick(tick)


class FuturesWebsocketApi(WebsocketClient):
    """The private websocket API of BtseGateway"""

    def __init__(self, gateway: BtseGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.callbacks: dict[str, callable] = {
            "login": self.on_login,
            "notificationApiV2": self.on_order,
            "fills": self.on_trade
        }

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        server_hosts: dict[str, str] = {
            "REAL": REAL_WEBSOCKET_HOST,
            "TESTNET": TESTNET_WEBSOCKET_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port, 20)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Private websocket API is connected")
        self.login()

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Private websocket API is disconnected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        print(packet)
        if "errors" in packet:
            for d in packet["errors"]:
                error: dict = d["error"]
                error_code: int = error["code"]
                error_message: str = error["message"]

                msg: str = f"Request caused error by websocket API, code: {error_code}, message: {error_message}"
                self.gateway.write_log(msg)

            return
        elif "event" in packet:
            event: str = packet["event"]
            callback: callable = self.callbacks.get(event, None)
            if callback:
                callback(packet)
        elif "topic" in packet:
            topic: str = packet["topic"]
            callback: callable = self.callbacks.get(topic, None)
            if callback:
                callback(packet)
        else:
            print(packet)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb)

        msg: str = f"Exception catched by websocket API: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def on_login(self, packet: dict) -> None:
        """Callback of user login"""
        self.gateway.write_log("Websocket API login successful")

        self.subscribe_topic()

    def on_order(self, packet: dict) -> None:
        """Callback of order update"""
        data: dict = packet["data"]

        local_id: str = data["clOrderID"]
        sys_id: str = data["orderID"]

        local_sys_map[local_id] = sys_id
        sys_local_map[sys_id] = local_id

        order: OrderData = OrderData(
            symbol=data["symbol"],
            exchange=Exchange.BTSE,
            type=ORDERTYPE_BTSE2VT[data["type"]],
            orderid=local_id,
            direction=DIRECTION_BTSE2VT[data["side"]],
            offset=Offset.NONE,
            traded=data["fillSize"],
            price=data["price"],
            volume=data["size"],
            status=STATUS_BTSE2VT.get(data["status"], Status.SUBMITTING),
            datetime=parse_timestamp(data["timestamp"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_order(order)

    def on_trade(self, packet: dict) -> None:
        """Callback of trade update"""
        for data in packet["data"]:
            trade: TradeData = TradeData(
                symbol=data["symbol"],
                exchange=Exchange.BTSE,
                orderid=data["clOrderId"],
                tradeid=data["tradeId"],
                direction=DIRECTION_BTSE2VT[data["side"]],
                offset=Offset.NONE,
                price=data["price"],
                volume=data["size"],
                datetime=parse_timestamp(data["timestamp"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)

    def on_market_trade(self, packet: dict) -> None:
        """Callback of market trade update"""
        for data in packet["data"]:
            symbol: str = data["symbol"]
            tick: TickData = self.gateway.get_tick(symbol)

            tick.last_price = data["price"]
            tick.last_volume = data["size"]
            tick.datetime = parse_timestamp(data["timestamp"])

            self.gateway.on_tick(tick)

    def login(self) -> None:
        """User login"""
        timestamp: str = str(int(time.time() * 1000))
        msg: str = f"/ws/spot{timestamp}"
        signature: str = generate_signature(msg, self.secret)

        btse_req: dict = {
            "op": "authKeyExpires",
            "args": [self.key, timestamp, signature]
        }
        self.send_packet(btse_req)

    def subscribe_topic(self) -> None:
        """Subscribe topics"""
        btse_req: dict = {
            "op": "subscribe",
            "args": ["notificationApiV2", "fills"]
        }
        self.send_packet(btse_req)

    def subscribe_market_trade(self, req: SubscribeRequest) -> None:
        """Subscribe market trade fill"""
        topic: str = f"tradeHistoryApi:{req.symbol}"
        self.callbacks[topic] = self.on_market_trade

        btse_req: dict = {
            "op": "subscribe",
            "args": [topic]
        }
        self.send_packet(btse_req)


def generate_signature(msg: str, secret_key: str) -> bytes:
    """Generate signature from message"""
    language: str = "latin-1"

    signature: str = hmac.new(
        bytes(secret_key, language),
        msg=bytes(msg, language),
        digestmod=hashlib.sha384,
    ).hexdigest()

    return signature


def generate_timestamp() -> str:
    """Generate current timestamp"""
    now: datetime = datetime.utcnow()
    timestamp: str = now.isoformat("T", "milliseconds")
    return timestamp + "Z"


def parse_timestamp(timestamp: int) -> datetime:
    """Parse timestamp to datetime"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    return dt.replace(tzinfo=UTC_TZ)


def get_float_value(data: dict, key: str) -> float:
    """Get decimal number from float value"""
    data_str: str = data.get(key, "")
    if not data_str:
        return 0.0
    return float(data_str)
