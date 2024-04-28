import hashlib
import hmac
import json
import time
from copy import copy
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
from vnpy_evo.trader.utility import round_to, ZoneInfo
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

# Testnet server hosts
TESTNET_REST_HOST: str = "https://testapi.btse.io/spot"
TESTNET_WEBSOCKET_HOST: str = "wss://testws.btse.io/ws/spot"
TESTNET_ORDERBOOK_HOST: str = "wss://testws.btse.io/ws/oss/spot"

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
    "SELL": Direction.SHORT
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

# Global set for local order id
local_orderids: set[str] = set()


class BtseSpotGateway(BaseGateway):
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

        self.rest_api: SpotRestApi = SpotRestApi(self)
        self.ob_api: SpotOrderbookApi = SpotOrderbookApi(self)
        self.ws_api: SpotWebsocketApi = SpotWebsocketApi(self)

        self.orders: dict[str, OrderData] = {}

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

        self.rest_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )
        # self.ob_api.connect(
        #     server,
        #     proxy_host,
        #     proxy_port,
        # )
        # self.ws_api.connect(
        #     key,
        #     secret,
        #     server,
        #     proxy_host,
        #     proxy_port,
        # )

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        self.ob_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.ws_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.ws_api.cancel_order(req)

    def query_account(self) -> None:
        """Not required since BTSE provides websocket update"""
        pass

    def query_position(self) -> None:
        """Not required since BTSE provides websocket update"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections"""
        self.rest_api.stop()
        self.ob_api.stop()
        self.ws_api.stop()

    def on_order(self, order: OrderData) -> None:
        """Save a copy of order and then pus"""
        self.orders[order.orderid] = order
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """Get previously saved order"""
        return self.orders.get(orderid, None)


class SpotRestApi(RestClient):
    """The REST API of BtseSpotGateway"""

    def __init__(self, gateway: BtseSpotGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseSpotGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.local_sys_map: dict[str, str] = {}
        self.sys_local_map: dict[str, str] = {}

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

            self.local_sys_map[local_id] = sys_id
            self.sys_local_map[sys_id] = local_id

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
    """The public websocket API of BtseSpotGateway"""

    def __init__(self, gateway: BtseSpotGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseSpotGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.subscribed: dict[str, SubscribeRequest] = {}
        self.ticks: dict[str, TickData] = {}

        self.callbacks: dict[str, callable] = {
            "tickers": self.on_ticker,
            "books5": self.on_depth
        }

    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        server_hosts: dict[str, str] = {
            "REAL": REAL_WEBSOCKET_HOST,
            "TESTNET": TESTNET_WEBSOCKET_HOST,
        }

        host: str = server_hosts[server]
        self.init(host, proxy_host, proxy_port, 20)

        self.start()

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        # Add subscribe record
        self.subscribed[req.vt_symbol] = req

        # Create tick object
        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            name=req.symbol,
            datetime=datetime.now(UTC_TZ),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol] = tick

        # Send request to subscribe
        args: list = []
        for channel in ["tickers", "books5"]:
            args.append({
                "channel": channel,
                "instId": req.symbol
            })

        req: dict = {
            "op": "subscribe",
            "args": args
        }
        self.send_packet(req)

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Public websocket API is connected")

        for req in list(self.subscribed.values()):
            self.subscribe(req)

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Public websocket API is disconnected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        if "event" in packet:
            event: str = packet["event"]
            if event == "subscribe":
                return
            elif event == "error":
                code: str = packet["code"]
                msg: str = packet["msg"]
                self.gateway.write_log(f"Public websocket API request failed, status code: {code}, message: {msg}")
        else:
            channel: str = packet["arg"]["channel"]
            callback: callable = self.callbacks.get(channel, None)

            if callback:
                data: list = packet["data"]
                callback(data)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb)

        msg: str = f"Exception catched by public websocket API: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def on_ticker(self, data: list) -> None:
        """Callback of ticker update"""
        for d in data:
            tick: TickData = self.ticks[d["instId"]]
            tick.last_price = float(d["last"])
            tick.open_price = float(d["open24h"])
            tick.high_price = float(d["high24h"])
            tick.low_price = float(d["low24h"])
            tick.volume = float(d["vol24h"])

    def on_depth(self, data: list) -> None:
        """Callback of depth update"""
        for d in data:
            tick: TickData = self.ticks[d["instId"]]
            bids: list = d["bids"]
            asks: list = d["asks"]

            for n in range(min(5, len(bids))):
                price, volume, _, _ = bids[n]
                tick.__setattr__("bid_price_%s" % (n + 1), float(price))
                tick.__setattr__("bid_volume_%s" % (n + 1), float(volume))

            for n in range(min(5, len(asks))):
                price, volume, _, _ = asks[n]
                tick.__setattr__("ask_price_%s" % (n + 1), float(price))
                tick.__setattr__("ask_volume_%s" % (n + 1), float(volume))

            tick.datetime = parse_timestamp(d["ts"])
            self.gateway.on_tick(copy(tick))


class SpotWebsocketApi(WebsocketClient):
    """The private websocket API of BtseSpotGateway"""

    def __init__(self, gateway: BtseSpotGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BtseSpotGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.callbacks: dict[str, callable] = {}

    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret.encode()
        self.passphrase = passphrase

        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        server_hosts: dict[str, str] = {
            "REAL": REAL_ORDERBOOK_HOST,
            "TESTNET": TESTNET_ORDERBOOK_HOST,
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
        if "event" in packet:
            cb_name: str = packet["event"]
        elif "op" in packet:
            cb_name: str = packet["op"]
        else:
            cb_name: str = packet["arg"]["channel"]

        callback: callable = self.callbacks.get(cb_name, None)
        if callback:
            callback(packet)

    def on_error(self, exception_type: type, exception_value: Exception, tb) -> None:
        """General error callback"""
        detail: str = self.exception_detail(exception_type, exception_value, tb)

        msg: str = f"私有频道触发异常: {detail}"
        self.gateway.write_log(msg)

        print(detail)

    def on_api_error(self, packet: dict) -> None:
        """Callback of login error"""
        code: str = packet["code"]
        msg: str = packet["msg"]
        self.gateway.write_log(f"Priavte websocket API request failed, status code: {code}, message: {msg}")

    def on_login(self, packet: dict) -> None:
        """Callback of user login"""
        if packet["code"] == '0':
            self.gateway.write_log("Private websocket API login successful")
            self.subscribe_topic()
        else:
            self.gateway.write_log("Private websocket API login failed")

    def on_order(self, packet: dict) -> None:
        """Callback of order update"""
        data: list = packet["data"]
        for d in data:
            order: OrderData = parse_order_data(d, self.gateway_name)
            self.gateway.on_order(order)

            # Check if order is fileed
            if d["fillSz"] == "0":
                return

            # Round trade volume number
            trade_volume: float = float(d["fillSz"])
            contract: ContractData = symbol_contract_map.get(order.symbol, None)
            if contract:
                trade_volume = round_to(trade_volume, contract.min_volume)

            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=d["tradeId"],
                direction=order.direction,
                offset=order.offset,
                price=float(d["fillPx"]),
                volume=trade_volume,
                datetime=parse_timestamp(d["uTime"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)

    def on_account(self, packet: dict) -> None:
        """Callback of account balance update"""
        if len(packet["data"]) == 0:
            return
        buf: dict = packet["data"][0]
        for detail in buf["details"]:
            account: AccountData = AccountData(
                accountid=detail["ccy"],
                balance=float(detail["eq"]),
                gateway_name=self.gateway_name,
            )
            account.available = float(detail["availEq"]) if len(detail["availEq"]) != 0 else 0.0
            account.frozen = account.balance - account.available
            self.gateway.on_account(account)

    def on_position(self, packet: dict) -> None:
        """Callback of holding position update"""
        data: list = packet["data"]
        for d in data:
            symbol: str = d["instId"]
            pos: int = float(d["pos"])
            price: float = get_float_value(d, "avgPx")
            pnl: float = get_float_value(d, "upl")

            position: PositionData = PositionData(
                symbol=symbol,
                exchange=Exchange.BTSE,
                direction=Direction.NET,
                volume=pos,
                price=price,
                pnl=pnl,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)

    def on_send_order(self, packet: dict) -> None:
        """Callback of send_order"""
        data: list = packet["data"]

        # Wrong parameters
        if packet["code"] != "0":
            if not data:
                order: OrderData = self.reqid_order_map[packet["id"]]
                order.status = Status.REJECTED
                self.gateway.on_order(order)
                return

        # Failed to process
        for d in data:
            code: str = d["sCode"]
            if code == "0":
                return

            orderid: str = d["clOrdId"]
            order: OrderData = self.gateway.get_order(orderid)
            if not order:
                return
            order.status = Status.REJECTED
            self.gateway.on_order(copy(order))

            msg: str = d["sMsg"]
            self.gateway.write_log(f"Send order failed, status code: {code}, message: {msg}")

    def on_cancel_order(self, packet: dict) -> None:
        """Callback of cancel order"""
        # Wrong parameters
        if packet["code"] != "0":
            code: str = packet["code"]
            msg: str = packet["msg"]
            self.gateway.write_log(f"Cancel order failed, status code: {code}, message: {msg}")
            return

        # Failed to process
        data: list = packet["data"]
        for d in data:
            code: str = d["sCode"]
            if code == "0":
                return

            msg: str = d["sMsg"]
            self.gateway.write_log(f"Cancel order failed, status code: {code}, message: {msg}")

    def login(self) -> None:
        """User login"""
        timestamp: str = str(time.time())
        msg: str = timestamp + "GET" + "/users/self/verify"
        signature: bytes = generate_signature(msg, self.secret)

        btse_req: dict = {
            "op": "login",
            "args":
            [
                {
                    "apiKey": self.key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": signature.decode("utf-8")
                }
            ]
        }
        self.send_packet(btse_req)

    def subscribe_topic(self) -> None:
        """Subscribe topics"""
        btse_req: dict = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "orders",
                    "instType": "ANY"
                },
                {
                    "channel": "account"
                },
                {
                    "channel": "positions",
                    "instType": "ANY"
                },
            ]
        }
        self.send_packet(btse_req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        # Check order type
        if req.type not in ORDERTYPE_VT2BTSE:
            self.gateway.write_log(f"Send order failed, order type not supported: {req.type.value}")
            return

        # Check symbol
        contract: ContractData = symbol_contract_map.get(req.symbol, None)
        if not contract:
            self.gateway.write_log(f"Send order failed, symbol not found: {req.symbol}")
            return

        # Generate local orderid
        self.order_count += 1
        count_str = str(self.order_count).rjust(6, "0")
        orderid = f"{self.connect_time}{count_str}"

        # Generate order params
        args: dict = {
            "instId": req.symbol,
            "clOrdId": orderid,
            "side": DIRECTION_VT2BTSE[req.direction],
            "ordType": ORDERTYPE_VT2BTSE[req.type],
            "px": str(req.price),
            "sz": str(req.volume)
        }

        if contract.product == Product.SPOT:
            args["tdMode"] = "cash"
        else:
            args["tdMode"] = "cross"

        self.reqid += 1
        btse_req: dict = {
            "id": str(self.reqid),
            "op": "order",
            "args": [args]
        }
        self.send_packet(btse_req)

        # Push submitting event
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)
        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        args: dict = {"instId": req.symbol}

        # Check if order id is local id
        if req.orderid in local_orderids:
            args["clOrdId"] = req.orderid
        else:
            args["ordId"] = req.orderid

        self.reqid += 1
        btse_req: dict = {
            "id": str(self.reqid),
            "op": "cancel-order",
            "args": [args]
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


def parse_order_data(data: dict, gateway_name: str) -> OrderData:
    """Parse dict to order data"""
    order_id: str = data["clOrdId"]
    if order_id:
        local_orderids.add(order_id)
    else:
        order_id: str = data["ordId"]

    order: OrderData = OrderData(
        symbol=data["instId"],
        exchange=Exchange.BTSE,
        type=ORDERTYPE_BTSE2VT[data["ordType"]],
        orderid=order_id,
        direction=DIRECTION_BTSE2VT[data["side"]],
        offset=Offset.NONE,
        traded=float(data["accFillSz"]),
        price=float(data["px"]),
        volume=float(data["sz"]),
        datetime=parse_timestamp(data["cTime"]),
        status=STATUS_BTSE2VT[data["state"]],
        gateway_name=gateway_name,
    )
    return order
