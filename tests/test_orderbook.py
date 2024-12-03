import sys
sys.path.append('../')
from agents.trackers.order_book import OrderBook
from typing import List
import random
from datetime import datetime, timedelta
from pprint import pprint

def generate_random_orderbook_messages(
    num_messages: int = 10,
    min_price: float = 0.45,
    max_price: float = 0.55,
    min_size: int = 1000,
    max_size: int = 5000,
    num_price_levels: int = 5,
    start_time: datetime = None
) -> List[dict]:
    """
    Generate an array of random orderbook messages for testing.
    
    Args:
        num_messages: Number of messages to generate
        min_price: Minimum price level
        max_price: Maximum price level
        min_size: Minimum order size
        max_size: Maximum order size
        num_price_levels: Number of price levels for asks and bids
        start_time: Starting timestamp (defaults to current time)
    
    Returns:
        List of orderbook messages
    """
    if start_time is None:
        start_time = datetime.now()

    messages = []
    
    for i in range(num_messages):
        # Generate random asks (lower prices)
        asks = []
        ask_prices = set(sorted([
            round(random.uniform(min_price, (min_price + max_price) / 2), 2)
            for _ in range(num_price_levels)
        ]))

        for price in ask_prices:
            asks.append({
                "price": str(price),
                "size": str(random.randint(min_size, max_size))
            })
        
        # Generate random bids (higher prices)
        bids = []
        bid_prices = set(sorted([
            round(random.uniform((min_price + max_price) / 2, max_price), 2)
            for _ in range(num_price_levels)
        ], reverse=True))
        
        for price in bid_prices:
            bids.append({
                "price": str(price),
                "size": str(random.randint(min_size, max_size))
            })
        
        # Create message
        timestamp = int((start_time + timedelta(seconds=i)).timestamp() * 1000)
        message = {
            "event_type": "book",
            "asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
            "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
            "asks": asks,
            "bids": bids,
            "timestamp": str(timestamp),
            "hash": "0x" + "".join(random.choices("0123456789abcdef", k=64))
        }
        
        messages.append(message)
    
    return messages

def generate_random_price_updates(
    num_updates: int = 10,
    min_price: float = 0.45,
    max_price: float = 0.55,
    min_size: int = 1000,
    max_size: int = 5000,
    num_changes: int = 3,
    start_time: datetime = None
) -> List[dict]:
    """
    Generate random price update messages for testing.
    
    Args:
        num_updates: Number of update messages to generate
        min_price: Minimum price level
        max_price: Maximum price level
        min_size: Minimum order size
        max_size: Maximum order size
        num_changes: Number of price changes per update
        start_time: Starting timestamp (defaults to current time)
    
    Returns:
        List of price update messages
    """
    if start_time is None:
        start_time = datetime.now()

    updates = []
    
    for i in range(num_updates):
        changes = []
        for _ in range(num_changes):
            changes.append({
                "price": str(round(random.uniform(min_price, max_price), 2)),
                "side": random.choice(["SELL", "BUY"]),
                "size": str(random.randint(min_size, max_size))
            })
            
        timestamp = int((start_time + timedelta(seconds=i)).timestamp() * 1000)
        update = {
            "asset_id": "71321045679252212594626385532706912750332728571942532289631379312455583992563",
            "changes": changes,
            "event_type": "price_change",
            "market": "0x5f65177b394277fd294cd75650044e32ba009a95022d88a0c1d565897d72f8f1",
            "timestamp": str(timestamp),
            "hash": "0x" + "".join(random.choices("0123456789abcdef", k=40))
        }
        
        updates.append(update)
    
    return updates

def main():
    book_messages = generate_random_orderbook_messages(num_messages=2, start_time=datetime.now())
    price_updates = generate_random_price_updates(num_updates=5, start_time=datetime.now() + timedelta(minutes=1))
    pprint(book_messages)
    pprint(price_updates)
    orderbook = OrderBook()

    for message in book_messages:
        orderbook.update_from_book(message)
    for message in price_updates:
        orderbook.update_from_price_change(message)

    print(orderbook.asks_history)
    print(orderbook.bids_history)

if __name__ == "__main__":
    main()
