import hashlib
import hmac
from datetime import  datetime
from operator import itemgetter
import zlib, json
from hyperquant.api import Platform, Sorting, Interval, Direction, OrderType
from hyperquant.clients import WSClient, Endpoint, Trade, Error, ErrorCode, \
    ParamName, WSConverter, RESTConverter, PrivatePlatformRESTClient, MyTrade, Candle, Ticker, OrderBookItem, Order, \
    OrderBook, Account, Balance


# Convert Interval API values to OKEX WS interval values
# 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 12hour, day, 3day, week
OKEX_WS_TimeFrames={"1m":"1min",
                    "3m":"3min",
                    "5m":"5min",
                    "15m":"15min",
                    "30m":"30min",
                    "1h":"1hour",
                    "2h":"2hour",
                    "4h":"2hour",
                    "6h":"6hour",
                    "12h":"12hour",
                    "1d":"day",
                    "3d":"3day",
                    "31":"week",
                   }


# REST

# TODO check getting trades history from_id=1
class OKEXRESTConverterV1(RESTConverter):
    # Main params:
    base_url = "https://www.okex.com/api/v1/"

    # Settings:

    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.PING: "ping",
        Endpoint.SERVER_TIME: "time",
        Endpoint.SYMBOLS: "exchangeInfo",
        Endpoint.TRADE: "trades",
# My settings
#        Endpoint.TRADE_HISTORY: "historicalTrades",
        Endpoint.TRADE_HISTORY: "trades.do",
        Endpoint.TRADE_MY: "myTrades",  # Private
# My settings
        Endpoint.CANDLE: "kline.do",
        Endpoint.TICKER: "ticker/price",
        Endpoint.ORDER_BOOK: "depth",
        # Private
        Endpoint.ACCOUNT: "account",
        Endpoint.ORDER: "order",
        Endpoint.ORDER_CURRENT: "openOrders",
        Endpoint.ORDER_MY: "allOrders",
    }
    param_name_lookup = {
        ParamName.SYMBOL: "symbol",
# My settings
        ParamName.LIMIT: "size",

#        ParamName.LIMIT: "limit",
        ParamName.IS_USE_MAX_LIMIT: None,
        # ParamName.SORTING: None,
# My settings
        ParamName.INTERVAL: "type",
        ParamName.DIRECTION: "side",
        ParamName.ORDER_TYPE: "type",

        ParamName.TIMESTAMP: "timestamp",
        ParamName.FROM_ITEM: "fromId",
        ParamName.TO_ITEM: None,
        ParamName.FROM_TIME: "startTime",
        ParamName.TO_TIME: "endTime",

        ParamName.PRICE: "price",
        ParamName.AMOUNT: "quantity",
        # -ParamName.ASKS: "asks",
        # ParamName.BIDS: "bids",
    }
    param_value_lookup = {
        # Sorting.ASCENDING: None,
        # Sorting.DESCENDING: None,
        Sorting.DEFAULT_SORTING: Sorting.ASCENDING,

# My settings
        Interval.MIN_1: "1min",
        Interval.MIN_3: "3min",
        Interval.MIN_5: "5min",
        Interval.MIN_15: "15min",
        Interval.MIN_30: "30min",
        Interval.HRS_1: "1hour",
        Interval.HRS_2: "2hour",
        Interval.HRS_4: "4hour",
        Interval.HRS_6: "6hour",
        Interval.HRS_8: "8hour",
        Interval.HRS_12: "12hour",
#        Interval.DAY_1: "1d",
#        Interval.DAY_3: "3d",
#        Interval.WEEK_1: "1w",
#        Interval.MONTH_1: "1M",

        # By properties:
        ParamName.DIRECTION: {
            Direction.SELL: "SELL",
            Direction.BUY: "BUY",
        },
        ParamName.ORDER_TYPE: {
            OrderType.LIMIT: "LIMIT",
            OrderType.MARKET: "MARKET",
        },
        # ParamName.ORDER_STATUS: {
        #     OrderStatus.: "",
        # },
    }
    max_limit_by_endpoint = {
        Endpoint.TRADE: 1000,
        Endpoint.TRADE_HISTORY: 1000,
        Endpoint.ORDER_BOOK: 1000,
        Endpoint.CANDLE: 1000,
    }

    # For parsing

    param_lookup_by_class = {
        # Error
      Error: {
            "code": "code",
            "msg": "message",
        },
        # Data


# My settings
      Trade: {
# Original
#            "time": ParamName.TIMESTAMP,
#            "id": ParamName.ITEM_ID,
#            "price": ParamName.PRICE,
#            "qty": ParamName.AMOUNT,
            # "isBuyerMaker": "",
            # "isBestMatch": "",



# My settings
            "date_ms": ParamName.TIMESTAMP,
            "tid": ParamName.ITEM_ID,
            "price": ParamName.PRICE,
            "amount": ParamName.AMOUNT,

#{'date': 1550527118, 
#'date_ms': 1550527118552, 
#'amount': 0.05679856, 
#'price': 3888.9995, 
#'type': 'buy', 
#'tid': 1021013946}

#  date: transaction time
#  date_ms: transaction time in milliseconds
#  price: transaction price
#  amount: quantity in BTC (or LTC)
#  tid: transaction ID
#  type: buy/sell



#self.param_lookup_by_class.get(object_class)
#{'time': 'timestamp', 'id': 'item_id', 'price': 'price', 'qty': 'amount'}

#object_class
#<class 'hyperquant.clients.Trade'>

# {'date': 1550527118, 'date_ms': 1550527118552, 'amount': 0.05679856, 'price': 3888.9995, 'type': 'buy', 'tid': 1021013946}
        },
      MyTrade: {
            "symbol": ParamName.SYMBOL,
            "time": ParamName.TIMESTAMP,
            "id": ParamName.ITEM_ID,
            "price": ParamName.PRICE,
            "qty": ParamName.AMOUNT,

            "orderId": ParamName.ORDER_ID,
            "commission": ParamName.FEE,
            # "commissionAsset": ParamName.FEE_SYMBOL,
            # "": ParamName.REBATE,
        },
# My settings
        Candle: [
            ParamName.TIMESTAMP,
            ParamName.PRICE_OPEN,
            ParamName.PRICE_HIGH,
            ParamName.PRICE_LOW,
            ParamName.PRICE_CLOSE,
            ParamName.AMOUNT
#           None,
#           None,
#           ParamName.TRADES_COUNT,
#           ParamName.INTERVAL,
        ],
        Ticker: {
            "symbol": ParamName.SYMBOL,
            "price": ParamName.PRICE,
        },
        Account: {
            "updateTime": ParamName.TIMESTAMP,
            "balances": ParamName.BALANCES,
        },
        Balance: {
            "asset": ParamName.SYMBOL,
            "free": ParamName.AMOUNT_AVAILABLE,
            "locked": ParamName.AMOUNT_RESERVED,
        },
        Order: {
            "symbol": ParamName.SYMBOL,
            "transactTime": ParamName.TIMESTAMP,
            "time": ParamName.TIMESTAMP,  # check "time" or "updateTime"
            "updateTime": ParamName.TIMESTAMP,
            "orderId": ParamName.ITEM_ID,
            "clientOrderId": ParamName.USER_ORDER_ID,

            "type": ParamName.ORDER_TYPE,
            "price": ParamName.PRICE,
            "origQty": ParamName.AMOUNT_ORIGINAL,
            "executedQty": ParamName.AMOUNT_EXECUTED,
            "side": ParamName.DIRECTION,
            "status": ParamName.ORDER_STATUS,
        },
        OrderBook: {
            "lastUpdateId": ParamName.ITEM_ID,
            "bids": ParamName.BIDS,
            "asks": ParamName.ASKS,
        },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
    }

    error_code_by_platform_error_code = {
        -2014: ErrorCode.UNAUTHORIZED,
        -1121: ErrorCode.WRONG_SYMBOL,
        -1100: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {
        429: ErrorCode.RATE_LIMIT,
        418: ErrorCode.IP_BAN,
    }

    # For converting time
    is_source_in_milliseconds = True

    # timestamp_platform_names = [ParamName.TIMESTAMP]

    def _process_param_value(self, name, value):
        if name == ParamName.FROM_ITEM or name == ParamName.TO_ITEM:
            if isinstance(value, Trade):  # ItemObject):
                return value.item_id
        return super()._process_param_value(name, value)

    def parse(self, endpoint, data):
        if endpoint == Endpoint.SERVER_TIME and data:
            timestamp_ms = data.get("serverTime")
            return timestamp_ms / 1000 if not self.use_milliseconds and timestamp_ms else timestamp_ms
        if endpoint == Endpoint.SYMBOLS and data and ParamName.SYMBOLS in data:
            exchange_info = data[ParamName.SYMBOLS]
            # (There are only 2 statuses: "TRADING" and "BREAK")
            # symbols = [item[ParamName.SYMBOL] for item in exchange_info if item["status"] == "TRADING"]
            symbols = [item[ParamName.SYMBOL] for item in exchange_info]
            return symbols

        result = super().parse(endpoint, data)
        return result

    # def preprocess_params(self, endpoint, params):
    #     if endpoint in self.secured_endpoints:
    #         params[ParamName.TIMESTAMP] = int(time.time() * 1000)
    #
    #     return super().preprocess_params(endpoint, params)

    def _generate_and_add_signature(self, platform_params, api_key, api_secret):
        if not api_key or not api_secret:
            self.logger.error("Empty api_key or api_secret. Cannot generate signature.")
            return None
        ordered_params_list = self._order_params(platform_params)
        # print("ordered_platform_params:", ordered_params_list)
        query_string = "&".join(["{}={}".format(d[0], d[1]) for d in ordered_params_list])
        # print("query_string:", query_string)
        m = hmac.new(api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256)
        signature = m.hexdigest()
        # Add
        # platform_params["signature"] = signature  # no need
        # if ordered_params_list and ordered_params_list[-1][0] != "signature":
        ordered_params_list.append(("signature", signature))
        return ordered_params_list

    def _order_params(self, platform_params):
        # Convert params to sorted list with signature as last element.

        params_list = [(key, value) for key, value in platform_params.items() if key != "signature"]
        # Sort parameters by key
        params_list.sort(key=itemgetter(0))
        # Append signature to the end if present
        if "signature" in platform_params:
            params_list.append(("signature", platform_params["signature"]))
        return params_list




class OKEXRESTClient(PrivatePlatformRESTClient):
    # Settings:
    platform_id = Platform.OKEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": OKEXRESTConverterV1,
        "3": OKEXRESTConverterV1,  # Only for some methods (same converter used)
    }

    # State:
    ratelimit_error_in_row_count = 0

# My settings
# Limited output by N data records
    def fetch_trades_history(self, symbol, limit=None, from_item=None, to_item=None,
                             sorting=None, is_use_max_limit=False, from_time=None, to_time=None,
                             version=None, **kwargs):
        # Fetching whole trades history as much as possible.
        # from_time and to_time used along with from_item and to_item as we often need to fetch
        # history by time and only Binance (as far as I know) doesn't support that (only by id)
        retVar1=self.fetch_history(Endpoint.TRADE, symbol, limit, from_item, to_item,
                                  sorting, is_use_max_limit, from_time, to_time,
                                  version, **kwargs)
# Size imit returned list
        return retVar1[0:limit]



    @property
    def headers(self):
        result = super().headers
        result["X-MBX-APIKEY"] = self._api_key
        result["Content-Type"] = "application/x-www-form-urlencoded"
        return result

    def _on_response(self, response, result):
        # super()._on_response(response, result)

        self.delay_before_next_request_sec = 0
        if isinstance(result, Error):
            if result.code == ErrorCode.RATE_LIMIT:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 2 * self.ratelimit_error_in_row_count  # some number - change
            elif result.code == ErrorCode.IP_BAN:
                self.ratelimit_error_in_row_count += 1
                self.delay_before_next_request_sec = 60 * 5 * self.ratelimit_error_in_row_count  # some number - change
            else:
                self.ratelimit_error_in_row_count = 0
        else:
            self.ratelimit_error_in_row_count = 0



    def fetch_history(self, endpoint, symbol, limit=None, from_item=None, to_item=None, sorting=None,
                      is_use_max_limit=False, from_time=None, to_time=None,
                      version=None, **kwargs):
        if from_item is None:
            from_item = 0
        return super().fetch_history(endpoint, symbol, limit, from_item, to_item, sorting, is_use_max_limit, from_time,
                                     to_time, **kwargs)

    def fetch_order_book(self, symbol=None, limit=None, is_use_max_limit=False, version=None, **kwargs):
        LIMIT_VALUES = [5, 10, 20, 50, 100, 500, 1000]
        if limit not in LIMIT_VALUES:
            self.logger.error("Limit value %s not in %s", limit, LIMIT_VALUES)
        return super().fetch_order_book(symbol, limit, is_use_max_limit, **kwargs)

    def fetch_tickers(self, symbols=None, version=None, **kwargs):
        items = super().fetch_tickers(symbols, version or "3", **kwargs)

        # (Binance returns timestamp only for /api/v1/ticker/24hr which has weight of 40.
        # /api/v3/ticker/price - has weight 2.)
        timestamp = self.get_server_timestamp(version)
        for item in items:
            item.timestamp = timestamp
            item.use_milliseconds = self.use_milliseconds

        return items

    def fetch_account_info(self, version=None, **kwargs):
        return super().fetch_account_info(version or "3", **kwargs)

    def create_order(self, symbol, order_type, direction, price=None, amount=None, is_test=False, version=None,
                     **kwargs):
        if order_type == OrderType.LIMIT:
            # (About values:
            # https://www.reddit.com/r/BinanceExchange/comments/8odvs4/question_about_time_in_force_binance_api/)
            kwargs["timeInForce"] = "GTC"
        return super().create_order(symbol, order_type, direction, price, amount, is_test, version, **kwargs)

    def cancel_order(self, order, symbol=None, version=None, **kwargs):
        if hasattr(order, ParamName.SYMBOL) and order.symbol:
            symbol = order.symbol
        return super().cancel_order(order, symbol, version, **kwargs)

    def check_order(self, order, symbol=None, version=None, **kwargs):
        if hasattr(order, ParamName.SYMBOL) and order.symbol:
            symbol = order.symbol
        return super().check_order(order, symbol, version, **kwargs)

    # def fetch_orders(self, symbol=None, limit=None, from_item=None, is_open=False, version=None, **kwargs):
    #     return super().fetch_orders(symbol, limit, from_item, is_open, version, **kwargs)

    def _send(self, method, endpoint, params=None, version=None, **kwargs):
        if endpoint in self.converter.secured_endpoints:
            server_timestamp = self.get_server_timestamp()
            params[ParamName.TIMESTAMP] = server_timestamp if self.use_milliseconds else int(server_timestamp * 1000)
        return super()._send(method, endpoint, params, version, **kwargs)


# WebSocket

class OKEXWSConverterV1(WSConverter):
    # Main params:
    base_url = "wss://real.okex.com:10440/ws/v1"

    IS_SUBSCRIPTION_COMMAND_SUPPORTED = True

    # supported_endpoints = [Endpoint.TRADE]
    # symbol_endpoints = [Endpoint.TRADE]
    # supported_symbols = None

    # Settings:

# My settings
    # Converting info:
    # For converting to platform
    endpoint_lookup = {
        Endpoint.CANDLE: "candle",
        Endpoint.TRADE: "trade"
    }
    '''
    endpoint_lookup = {
        Endpoint.TRADE: "{symbol}@trade",
        Endpoint.CANDLE: "{symbol}@kline_{interval}",
        Endpoint.TICKER: "{symbol}@miniTicker",
        Endpoint.TICKER_ALL: "!miniTicker@arr",
        Endpoint.ORDER_BOOK: "{symbol}@depth{level}",
        Endpoint.ORDER_BOOK_DIFF: "{symbol}@depth",
    }
    '''

    # For parsing
    param_lookup_by_class = {
        # Error
        Error: {
            # "code": "code",
            # "msg": "message",
        },

# My settings
        # Data
        Trade: {
            "s": ParamName.SYMBOL,
            "T": ParamName.TIMESTAMP,
#            "t": ParamName.ITEM_ID,
            "E": ParamName.ITEM_ID, # My settings
            "p": ParamName.PRICE,
            "q": ParamName.AMOUNT,
            # "m": "",
        },

# My settings
        Candle: {
            "E": ParamName.ITEM_ID, # My settings

            "s": ParamName.SYMBOL,
            "t": ParamName.TIMESTAMP,
            "i": ParamName.INTERVAL,

            "o": ParamName.PRICE_OPEN,
            "c": ParamName.PRICE_CLOSE,
            "h": ParamName.PRICE_HIGH,
            "l": ParamName.PRICE_LOW,
            "": ParamName.AMOUNT,  # only volume present
            "n": ParamName.TRADES_COUNT,
#        data1={ 'data': {'e': 'kline', 'E': 1551048846552, 's': 'ETHBTC', 'k': {'t': 1551048840000, 'T': 1551048899999, 's': 'ETHBTC', 'i': '1m', 'f': 108856813, 'L': 108856822, 'o': '0.03654800', 'c': '0.03654800', 'h': '0.03655800', 'l': '0.03654600', 'v': '5.15300000', 'n': 10, 'x': False, 'q': '0.18834029', 'V': '1.16200000', 'Q': '0.04247473', 'B': '0'}}}
        },

        Ticker: {
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "c": ParamName.PRICE,  # todo check to know for sure
        },
        OrderBook: {
            # Partial Book Depth Streams
            "lastUpdateId": ParamName.ITEM_ID,
            "asks": ParamName.ASKS,
            "bids": ParamName.BIDS,
            # Diff. Depth Stream
            "s": ParamName.SYMBOL,
            "E": ParamName.TIMESTAMP,
            "u": ParamName.ITEM_ID,
            "b": ParamName.BIDS,
            "a": ParamName.ASKS,
        },
        OrderBookItem: [ParamName.PRICE, ParamName.AMOUNT],
    }
    event_type_param = "e"
    endpoint_by_event_type = {
        "trade": Endpoint.TRADE,
        "kline": Endpoint.CANDLE,
        "24hrMiniTicker": Endpoint.TICKER,
        "24hrTicker": Endpoint.TICKER,
        "depthUpdate": Endpoint.ORDER_BOOK,
        # "depthUpdate": Endpoint.ORDER_BOOK_DIFF,
    }

    # https://github.com/binance-exchange/binance-official-api-docs/blob/master/errors.md
    error_code_by_platform_error_code = {
        # -2014: ErrorCode.UNAUTHORIZED,
        # -1121: ErrorCode.WRONG_SYMBOL,
        # -1100: ErrorCode.WRONG_PARAM,
    }
    error_code_by_http_status = {}

    # For converting time
    is_source_in_milliseconds = True

# Original settings
#    def _generate_subscription(self, endpoint, symbol=None, **params):
#        return super()._generate_subscription(endpoint, symbol.lower() if symbol else symbol, **params)

# My settings



    def _generate_subscription(self, endpoint, symbol=None, **params):
        channel = super()._generate_subscription(endpoint, symbol, **params)
        return (channel, symbol)


    def parse(self, endpoint, data):
        if "data" in data:
            # stream = data["stream"]  # no need
            data = data["data"]
        var1=super().parse(endpoint, data)
        return var1


    def _parse_item(self, endpoint, item_data):
        if endpoint == Endpoint.CANDLE and "k" in item_data:
            item_data = item_data["k"]
        return super()._parse_item(endpoint, item_data)


class OKEXWSClient(WSClient):
    # TODO consider reconnection and resubscription
    # TODO consider reconnect on connection, pong and other timeouts

    # Settings:
    platform_id = Platform.OKEX
    version = "1"  # Default version

    _converter_class_by_version = {
        "1": OKEXWSConverterV1,
#        "2": OKEXWSConverterV2,
    }




    def _parse(self, endpoint, data):
        if data and isinstance(data, list):
            var1=[self.converter.parse(endpoint, data_item) for data_item in data]
            return var1
        var2=self.converter.parse(endpoint, data)
        return var2


    # State:

    def _send_subscribe(self, subscriptions):
        for channel, symbol in subscriptions:
            trading_pair_symbol = "t" + symbol
            event_data = {
                "event": "subscribe",
                "channel": channel,
                "symbol": trading_pair_symbol}
            self._send(event_data)



    # Закомментированные методы можно свободно удалять, если проще переносить код из другой библиотеки заново

    # def on_item_received(self, item):
    #     # if isinstance(item, Channel):
    #     #     self.channel_by_id[item.channel_id] = item
    #     #     return
    #     #
    #     super().on_item_received(item)
    #
    #     # # Handle data
    #     # if isinstance(data, dict):
    #     #     # This is a system message
    #     #     self._system_handler(data, received_at)
    #     # else:
    #     #     # This is a list of data
    #     #     if data[1] == 'hb':
    #     #         self._heartbeat_handler()
    #     #     else:
    #     #         self._data_handler(data, received_at)

    # def _system_handler(self, data, ts):
    #     """Distributes system messages to the appropriate handler.
    #     System messages include everything that arrives as a dict,
    #     or a list containing a heartbeat.
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_system_handler(): Received a system message: %s", data)
    #     # Unpack the data
    #     event = data.pop('event')
    #     if event == 'pong':
    #         self.log.debug("_system_handler(): Distributing %s to _pong_handler..",
    #                        data)
    #         self._pong_handler()
    #     elif event == 'info':
    #         self.log.debug("_system_handler(): Distributing %s to _info_handler..",
    #                        data)
    #         self._info_handler(data)
    #     elif event == 'error':
    #         self.log.debug("_system_handler(): Distributing %s to _error_handler..",
    #                        data)
    #         self._error_handler(data)
    #     elif event in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
    #         self.log.debug("_system_handler(): Distributing %s to "
    #                        "_response_handler..", data)
    #         self._response_handler(event, data, ts)
    #     else:
    #         self.log.error("Unhandled event: %s, data: %s", event, data)

    #     if event_name in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
    #         try:
    #             self._response_handlers[event_name](event_name, data, ts)
    #         except KeyError:
    #             self.log.error("Dtype '%s' does not have a response "
    #                            "handler! (%s)", event_name, message)
    #     elif event_name == 'data':
    #         try:
    #             channel_id = data[0]
    #             if channel_id != 0:
    #                 # Get channel type associated with this data to the
    #                 # associated data type (from 'data' to
    #                 # 'book', 'ticker' or similar
    #                 channel_type, *_ = self.channel_directory[channel_id]
    #
    #                 # Run the associated data handler for this channel type.
    #                 self._data_handlers[channel_type](channel_type, data, ts)
    #                 # Update time stamps.
    #                 self.update_timestamps(channel_id, ts)
    #             else:
    #                 # This is data from auth channel, call handler
    #                 self._handle_account(data=data, ts=ts)
    #         except KeyError:
    #             self.log.error("Channel ID does not have a data handler! %s",
    #                            message)
    #     else:
    #         self.log.error("Unknown event_name on queue! %s", message)
    #         continue

    #     self._response_handlers = {'unsubscribed': self._handle_unsubscribed,
    #                                'subscribed': self._handle_subscribed,
    #                                'conf': self._handle_conf,
    #                                'auth': self._handle_auth,
    #                                'unauth': self._handle_auth}
    #     self._data_handlers = {'ticker': self._handle_ticker,
    #                            'book': self._handle_book,
    #                            'raw_book': self._handle_raw_book,
    #                            'candles': self._handle_candles,
    #                            'trades': self._handle_trades}

    # https://github.com/Crypto-toolbox/btfxwss/blob/master/btfxwss/queue_processor.py

    # def _handle_subscribed(self, dtype, data, ts,):
    #     """Handles responses to subscribe() commands.
    #     Registers a channel id with the client and assigns a data handler to it.
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_handle_subscribed: %s - %s - %s", dtype, data, ts)
    #     channel_name = data.pop('channel')
    #     channel_id = data.pop('chanId')
    #     config = data
    #
    #     if 'pair' in config:
    #         symbol = config['pair']
    #         if symbol.startswith('t'):
    #             symbol = symbol[1:]
    #     elif 'symbol' in config:
    #         symbol = config['symbol']
    #         if symbol.startswith('t'):
    #             symbol = symbol[1:]
    #     elif 'key' in config:
    #         symbol = config['key'].split(':')[2][1:]  #layout type:interval:tPair
    #     else:
    #         symbol = None
    #
    #     if 'prec' in config and config['prec'].startswith('R'):
    #         channel_name = 'raw_' + channel_name
    #
    #     self.channel_handlers[channel_id] = self._data_handlers[channel_name]
    #
    #     # Create a channel_name, symbol tuple to identify channels of same type
    #     if 'key' in config:
    #         identifier = (channel_name, symbol, config['key'].split(':')[1])
    #     else:
    #         identifier = (channel_name, symbol)
    #     self.channel_handlers[channel_id] = identifier
    #     self.channel_directory[identifier] = channel_id
    #     self.channel_directory[channel_id] = identifier
    #     self.log.info("Subscription succesful for channel %s", identifier)
    #
    # def _handle_unsubscribed(self, dtype, data, ts):
    #     """Handles responses to unsubscribe() commands.
    #     Removes a channel id from the client.
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_handle_unsubscribed: %s - %s - %s", dtype, data, ts)
    #     channel_id = data.pop('chanId')
    #
    #     # Unregister the channel from all internal attributes
    #     chan_identifier = self.channel_directory.pop(channel_id)
    #     self.channel_directory.pop(chan_identifier)
    #     self.channel_handlers.pop(channel_id)
    #     self.last_update.pop(channel_id)
    #     self.log.info("Successfully unsubscribed from %s", chan_identifier)
    #
    # def _handle_auth(self, dtype, data, ts):
    #     """Handles authentication responses.
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     # Contains keys status, chanId, userId, caps
    #     if dtype == 'unauth':
    #         raise NotImplementedError
    #     channel_id = data.pop('chanId')
    #     user_id = data.pop('userId')
    #
    #     identifier = ('auth', user_id)
    #     self.channel_handlers[identifier] = channel_id
    #     self.channel_directory[identifier] = channel_id
    #     self.channel_directory[channel_id] = identifier

    # def _handle_trades(self, dtype, data, ts):
    #     """Files trades in self._trades[chan_id].
    #     :param dtype:
    #     :param data:
    #     :param ts:
    #     :return:
    #     """
    #     self.log.debug("_handle_trades: %s - %s - %s", dtype, data, ts)
    #     channel_id, *data = data
    #     channel_identifier = self.channel_directory[channel_id]
    #     entry = (data, ts)
    #     self.trades[channel_identifier].put(entry)

    def _send_auth(self):
        # Generate nonce
        auth_nonce = str(int(time.time() * 10000000))
        # Generate signature
        auth_payload = "AUTH" + auth_nonce
        auth_sig = hmac.new(self._api_secret.encode(), auth_payload.encode(),
                            hashlib.sha384).hexdigest()

        payload = {"event": "auth", "apiKey": self._api_key, "authSig": auth_sig,
                   "authPayload": auth_payload, "authNonce": auth_nonce}

        self._send(payload)


# # Auth v1:
# import hmac
# import hashlib
# import time
#
# nonce = int(time.time() * 1000000)
# auth_payload = "AUTH" + str(nonce)
# signature = hmac.new(
#     API_SECRET.encode(),
#     msg = auth_payload.encode(),
#     digestmod = hashlib.sha384
# ).hexdigest()
#
# payload = {
#     "apiKey": API_KEY,
#     "event": "auth",
#     "authPayload": auth_payload,
#     "authNonce": nonce,
#     "authSig": signature
# }
#
# ws.send(json.dumps(payload))

# https://github.com/bitfinexcom/bitfinex-api-node
# How do te and tu messages differ?
# A te packet is sent first to the client immediately after a trade has been
# matched & executed, followed by a tu message once it has completed processing.
# During times of high load, the tu message may be noticably delayed, and as
# such only the te message should be used for a realtime feed.

# check if CANDLE data line
    ADDCHANNEL=lambda self, buffer: buffer['channel']=='addChannel'
# check if CANDLE data line
    IS_CANDLE=lambda self, buffer: buffer['channel'].find('_kline_')>=0
# check if TRADE data line
    IS_TRADE =lambda self, buffer: buffer['channel'].find('_deals')>=0

# Extract symbol name from input string
    getSymbol=lambda self,buffer: buffer['channel'].split('_')[3]+'_'+buffer['channel'].split('_')[4]




    # Parse received line (CANDLE)
    def _parse_line_candle(self, buffer):


# Must be parsed line like this
#                                                       type operation          time             O            H             L             C             V
#        [{'binary': 0, 'channel': 'ok_sub_spot_eth_btc_kline_1min', 'data': [['1551033540000', '0.0367391', '0.03677986', '0.03673082', '0.03677985', '191.512646']]}]
#    data={'data': {'s': 'eth_btc', 'e': 'kline','E':'1551052980000','o':'0.03552913','h':'0.03552913','l':'0.03546483','c':'0.03546483','v':'93.779403'}}
#         {'data': {'s': 'eth_btc', 'e': 'kline', 't': '1551088500000', 'o': '3789.5341', 'h': '3790.2388', 'l': '3789.0003', 'c': '3789.3534', 'v': '3789.3534'}}
#      data=buffer['data']

# Grab T OHLCV data from input buffer
      Type='kline'
      Symbol=self.getSymbol(buffer)
      Time=buffer['data'][0][0]
      O=   buffer['data'][0][1]
      H=   buffer['data'][0][2]
      L=   buffer['data'][0][3]
      C=   buffer['data'][0][4]
      V=   buffer['data'][0][4]

 # Create dictionary from input list
      return {'data': {'s': Symbol, 'e': Type,'E':Time,'o':O,'h':H,'l':L,'c':C,'v':V}}





    # Parse received line (TRADE)
    def _parse_line_trade(self, buffer):
#      print(buffer)
# [tid, price, amount, time, type]
# [string, string, string, string, string]
#                                                   [tid,         price,         amount,     time,       type]
# {'channel': 'ok_sub_spot_eth_btc_deals', 'data': [['536389569', '0.03607331', '0.038413', '19:30:45', 'ask']], 'binary': 0}
# Request
#{'event':'addChannel','channel':'ok_sub_spot_bch_btc_deals'}
# Response
#[
#   {
#    "channel":"ok_sub_spot_bch_btc_deals",
#    "data":[["1001","2463.86","0.052","16:34:07","ask"]]
#}
# {'data': {'s': 'btc_usdt', 'T': '20:00:58', 'E': '1057320577', 'p': '3791.8527', 'q': '0.01626833'}}


# Grab  data from input buffer
      Type='trade'
      Symbol=self.getSymbol(buffer)
      tid=    buffer['data'][0][0]
      price=  buffer['data'][0][1]
      amount= buffer['data'][0][2]
      time=   buffer['data'][0][3]
      typeop= buffer['data'][0][4]

      # Convert json "_deals" time to miliseconds
      date_today_time=datetime.today().strftime('%Y-%m-%d')+" "+time
      seconds=datetime.strptime(date_today_time, '%Y-%m-%d %H:%M:%S').timestamp() * 1000

 # Create dictionary from input list
      var1={'data': {'s': Symbol, 'e': Type, 'T':seconds, 'E': tid, 'p':price,'q':amount}}
      return var1


        
#   Handler for parsing WS messages
    def _on_message(self, message):
        self.logger.debug("On message: %s", message[:200])
        # str -> json
        try:
            data = json.loads(inflate(message))[0]
        except json.JSONDecodeError:
            self.logger.error("Wrong JSON is received! Skipped. message: %s", message)
            return
# trade
#        data={'stream': 'btcusdt@trade', 'data': {'e': 'trade', 'E': 1551048914408, 's': 'BTCUSDT', 't': 102869432, 'p': '3775.86000000', 'q': '0.18071500', 'b': 268959750, 'a': 268959749, 'T': 1551048914403, 'm': False, 'M': True}}

# candle
#
#        data={'stream': 'ethbtc@kline_1m', 'data': {'e': 'kline', 'E': 1551048846552, 's': 'ETHBTC', 'k': {'t': 1551048840000, 'T': 1551048899999, 's': 'ETHBTC', 'i': '1m', 'f': 108856813, 'L': 108856822, 'o': '0.03654800', 'c': '0.03654800', 'h': '0.03655800', 'l': '0.03654600', 'v': '5.15300000', 'n': 10, 'x': False, 'q': '0.18834029', 'V': '1.16200000', 'Q': '0.04247473', 'B': '0'}}}
#        print("data:",data)
#        data: {'binary': 0, 'channel': 'ok_sub_spot_eth_btc_kline_1min', 'data': [['1551052980000', '0.03552913', '0.03552913','0.03546483', '0.03546483', '93.779403']]}
#        data1={ 'data': {'e': 'kline', 'E': 1551048846552, 's': 'ETHBTC', 'k': {'t': 1551048840000, 'T': 1551048899999, 's': 'ETHBTC', 'i': '1m', 'f': 108856813, 'L': 108856822, 'o': '0.03654800', 'c': '0.03654800', 'h': '0.03655800', 'l': '0.03654600', 'v': '5.15300000', 'n': 10, 'x': False, 'q': '0.18834029', 'V': '1.16200000', 'Q': '0.04247473', 'B': '0'}}}
#        data1={ 'data': {'e': 'kline', 'E': 1551048846552, 's': 'EURUSD', 'o': '0.03654800', 'c': '0.03654800', 'h': '0.03655800', 'l': '0.03654600', 'v': '5.15300000'}}
#        data={'data': {'s': 'eth_btc', 'e': 'kline','E':'1551052980000','o':'0.03552913','h':'0.03552913','l':'0.03546483','c':'0.03546483','v':'93.779403'}}

# Escape if not suitable type of data
        if self.ADDCHANNEL(data): return data
        
# Pre- parse lines by own parser
        if self.IS_CANDLE(data):  data1=self._parse_line_candle(data)
        if self.IS_TRADE(data):   data1=self._parse_line_trade(data)

        # json -> items
        result = self._parse(None, data1)

        # Process items
        self._data_buffer = []

        if result and isinstance(result, list):
            for item in result:
                self.on_item_received(item)
        else:
            self.on_item_received(result)

        if self.on_data and self._data_buffer:
            self.on_data(self._data_buffer)
      

    '''
    def _on_message(self, message):
         """Handler for parsing WS messages."""
         self.logger.debug(message)
         message = json.loads(inflate(message))

         # Create converter for parsing message
#         converter = self.get_or_create_converter(version)

         #  Parse message
#         result=self._parse_line(self, message)
         result=self._parse_line(message)
    
         # Place message to output stream
         self.on_data_item(result)
    '''



# subscribe to channel
    # State:
#    def _send_subscribe(self, subscriptions, interval):
    def _send_subscribe(self, subscriptions, interval):

#       print(subscriptions,interval)

       for channel, symbol in subscriptions:
# Create request for candle
        if (channel=='candle'): 
          event_data = {'event':'addChannel','channel':'ok_sub_spot_'+symbol+'_kline_'+OKEX_WS_TimeFrames[interval] }

# Create request for trade
        if (channel=='trade'):  
          event_data = {'event':'addChannel','channel':'ok_sub_spot_'+symbol+'_deals'}
#        print(event_data)

# Send request
# ok_sub_spot_X_deals   Subscribe Trade Records
#      event_data = {'event':'addChannel','channel':'ok_sub_spot_bch_btc_deals'}
# Request
#{'event':'addChannel','channel':'ok_sub_spot_bch_btc_deals'}
# Response
#[
#   {
#    "channel":"ok_sub_spot_bch_btc_deals",
#    "data":[["1001","2463.86","0.052","16:34:07","ask"]]
#}
#]
#
#[  
#   {  
#      "channel":"ok_sub_spot_btc_usdt_deals",
#      "data":[  
#         [  
#            "1047223997",
#            "4133.2569",
#            "0.169",
#            "04:54:14",
#            "ask"
#         ]
#      ],
#      "binary":0
#   }
#]
#
#
#      event_data = {'event':'addChannel','channel':'ok_sub_spot_bch_btc_kline_1min'}
# Request
#{'event':'addChannel','channel':'ok_sub_spot_bch_btc_kline_1min'}
# Response
#[{
#    "channel":"ok_sub_spot_bch_btc_kline_1min",
#    "data":[
#        ["1490337840000","995.37","996.75","995.36","996.75","9.112"],
#        ["1490337840000","995.37","996.75","995.36","996.75","9.112"]
#    ]
#}]
#var1= b'[{"binary":1,"channel":"ok_sub_spot_eth_btc_kline_1min","data":[["1550955240000","0.03836507","0.03838636","0.03836507","0.03837616","52.762306"]]}]'

        self._send(event_data)




    def _subscribe(self, subscriptions, interval):
        # Call subscribe command with "subscriptions" param or reconnect with
        # "self.current_subscriptions" in URL - depending on platform
        self.logger.debug(" Subscribe to subscriptions: %s", subscriptions)
        if not self.is_started or not self.IS_SUBSCRIPTION_COMMAND_SUPPORTED:
            # Connect on first subscribe() or reconnect on the further ones
            self.reconnect()
        else:
            self._send_subscribe(subscriptions, interval)


    # Subscription
    def subscribe(self, endpoints=None, symbols=None, interval=None, **params):

        self.logger.debug("Subscribe on endpoints: %s and symbols: %s prev: %s %s",
                          endpoints, symbols, self.endpoints, self.symbols)

        # Save interval to global variable
        if (interval==None):
          interval=self.interval
        else:
          self.interval=interval

        # if not endpoints and not symbols:
        #     subscriptions = self.prev_subscriptions
        # else:
        if not endpoints:
            endpoints = self.endpoints or self.converter.supported_endpoints
        else:
            endpoints = set(endpoints).intersection(self.converter.supported_endpoints)
            self.endpoints = self.endpoints.union(endpoints) if self.endpoints else endpoints

        if not symbols:
            symbols = self.symbols or self.converter.supported_symbols
        else:
            self.symbols = self.symbols.union(symbols) if self.symbols else set(symbols)
        if not endpoints:
            return

        subscriptions = self.converter.generate_subscriptions(endpoints, symbols, **params)

        self.current_subscriptions = self.current_subscriptions.union(subscriptions) \
            if self.current_subscriptions else subscriptions

        self._subscribe(subscriptions, interval)




# This function needed for unzip returned value (data) from on_message event
def inflate(data):
  decompress = zlib.decompressobj(-zlib.MAX_WBITS )
  inflated = decompress.decompress(data)
  inflated += decompress.flush()
  return inflated


