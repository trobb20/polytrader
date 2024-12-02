from agents.polymarket.polymarket import Polymarket
from agents.utils.objects import SimpleMarket
import json
import websockets
import asyncio
from typing import Callable, Dict, List, TypedDict, Union, Literal
import ast

class OrderSummary(TypedDict):
    price: str
    size: str

class BookMessage(TypedDict):
    event_type: Literal['book']
    asset_id: str
    market: str
    bids: List[OrderSummary]
    asks: List[OrderSummary]
    timestamp: str
    hash: str

class PriceChange(TypedDict):
    price: str
    side: Literal['BUY', 'SELL']
    size: str

class PriceChangeMessage(TypedDict):
    event_type: Literal['price_change']
    asset_id: str
    market: str
    changes: List[PriceChange]
    timestamp: str
    hash: str

class LastTradePriceMessage(TypedDict):
    event_type: Literal['last_trade_price']
    asset_id: str
    market: str
    price: str
    timestamp: str
    side: Literal['BUY', 'SELL']
    size: str

class TickSizeChangeMessage(TypedDict):
    event_type: Literal['tick_size_change']
    asset_id: str
    market: str
    old_tick_size: str
    new_tick_size: str
    timestamp: str

MarketMessage = Union[BookMessage, PriceChangeMessage, TickSizeChangeMessage, LastTradePriceMessage]

class MarketChannel:
    def __init__(self, market_obj: SimpleMarket):
        self.ws_url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
        self.websocket = None
        self.callbacks: Dict[str, List[Callable[[str, MarketMessage], None]]] = {}
        self.running = False
        self.outcomes = [str(outcome) for outcome in market_obj.outcomes]
        self.outcomes_id = {
            outcome: str(market_obj.clob_token_ids[index]) for index, outcome in enumerate(market_obj.outcomes)
        }
        self.id_to_outcome = {v: k for k, v in self.outcomes_id.items()}

    async def connect(self):
        """Connect and subscribe to market updates for given asset IDs"""
        self.websocket = await websockets.connect(self.ws_url)
        self.running = True

        # Send subscription message
        subscribe_message = {
            "type": "Market",
            "assets_ids": list(self.outcomes_id.values())
        }
        await self.websocket.send(json.dumps(subscribe_message))
        
        # Start message handler
        asyncio.create_task(self._message_handler())

    async def _message_handler(self):
        """Handle incoming WebSocket messages"""
        try:
            while self.running and self.websocket:
                message = await self.websocket.recv()
                data_list = ast.literal_eval(message)
                
                # Handle both single messages and lists of messages
                messages = data_list if isinstance(data_list, list) else [data_list]
                
                for data in messages:
                    # Ensure data is a MarketMessage
                    if isinstance(data, dict) and 'asset_id' in data:
                        asset_id = data['asset_id']
                        # Get the outcome for this asset_id
                        outcome = self.id_to_outcome.get(asset_id)
                        if outcome and (callbacks := self.callbacks.get(asset_id)):
                            for callback in callbacks:
                                callback(outcome, MarketMessage(**data))

        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
        except Exception as e:
            print(f"Error in message handler: {e}")
            print(f"Message that caused error: {message}")

    def add_outcome_callback(self, outcome: str, callback: Callable[[str, MarketMessage], None]):
        """Add a callback for a specific outcome"""
        asset_id = self.outcomes_id[outcome]
        if asset_id not in self.callbacks:
            self.callbacks[asset_id] = []
        self.callbacks[asset_id].append(callback)

    def remove_outcome_callback(self, outcome: str, callback: Callable[[str, MarketMessage], None]):
        """Remove a callback for a specific outcome"""
        asset_id = self.outcomes_id[outcome]
        if asset_id in self.callbacks and callback in self.callbacks[asset_id]:
            self.callbacks[asset_id].remove(callback)
            if not self.callbacks[asset_id]:
                del self.callbacks[asset_id]

    async def close(self):
        """Close the WebSocket connection"""
        self.running = False
        if self.websocket:
            await self.websocket.close()