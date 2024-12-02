from agents.polymarket.polymarket import Polymarket
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
    buys: List[OrderSummary]
    sells: List[OrderSummary]
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

class TickSizeChangeMessage(TypedDict):
    event_type: Literal['tick_size_change']
    asset_id: str
    market: str
    old_tick_size: str
    new_tick_size: str
    timestamp: str

MarketMessage = Union[BookMessage, PriceChangeMessage, TickSizeChangeMessage]

class MarketChannel:
    def __init__(self):
        self.ws_url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
        self.websocket = None
        self.callbacks: Dict[str, List[Callable[[MarketMessage], None]]] = {}
        self.running = False

    async def connect(self, assets_ids: List[str]):
        """Connect and subscribe to market updates for given asset IDs"""
        self.websocket = await websockets.connect(self.ws_url)
        self.running = True

        # Send subscription message
        subscribe_message = {
            "type": "Market",
            "assets_ids": assets_ids
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
                    if isinstance(data, dict) and 'market' in data:
                        # Call all callbacks registered for this asset
                        if callbacks := self.callbacks.get(data['asset_id']):
                            for callback in callbacks:
                                callback(data)

        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
        except Exception as e:
            print(f"Error in message handler: {e}")
            print(f"Message that caused error: {message}")

    def add_asset_callback(self, asset_id: str, callback: Callable[[MarketMessage], None]):
        """Add a callback for a specific asset"""
        if asset_id not in self.callbacks:
            self.callbacks[asset_id] = []
        self.callbacks[asset_id].append(callback)

    def remove_asset_callback(self, asset_id: str, callback: Callable[[MarketMessage], None]):
        """Remove a callback for a specific asset"""
        if asset_id in self.callbacks and callback in self.callbacks[asset_id]:
            self.callbacks[asset_id].remove(callback)
            if not self.callbacks[asset_id]:
                del self.callbacks[asset_id]

    async def close(self):
        """Close the WebSocket connection"""
        self.running = False
        if self.websocket:
            await self.websocket.close()

async def main():
    p=Polymarket()
    market_obj = p.get_market_by_slug('nfl-phi-bal-2024-12-01')
    # Initialize market channel
    market_channel = MarketChannel()

    # Example callback function
    def handle_market_update(message: MarketMessage):
        if message['event_type'] == 'book':
            print(f"Book update for market {message['market']}:")
            print(f"Buys: {message['buys']}")
            print(f"Sells: {message['sells']}")
        elif message['event_type'] == 'price_change':
            print(f"Price changes for market {message['market']}:")
            print(f"Changes: {message['changes']}")
        elif message['event_type'] == 'tick_size_change':
            print(f"Tick size changed for market {message['market']}:")
            print(f"From {message['old_tick_size']} to {message['new_tick_size']}")

    # Connect with asset IDs you want to track
    asset_ids = [
       str(market_obj.clob_token_ids[0]),
       str(market_obj.clob_token_ids[1])
    ]
    await market_channel.connect(asset_ids)

    # Add callback for specific assets
    market_channel.add_asset_callback(str(market_obj.clob_token_ids[0]), handle_market_update)

    try:
        # Keep connection alive
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await market_channel.close()

if __name__ == "__main__":
    asyncio.run(main())