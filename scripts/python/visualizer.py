from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def load_book_from_csv(f: Path) -> pd.DataFrame:
    # Load the order book data from CSV files
    asks = pd.read_csv(f.parent / f"{f.stem}_asks.csv", index_col=0)
    bids = pd.read_csv(f.parent / f"{f.stem}_bids.csv", index_col=0) 
    trades = pd.read_csv(f.parent / f"{f.stem}_trades.csv")

    # Convert column names to datetime
    new_ask_cols = list(pd.to_datetime(pd.to_numeric(asks.columns), unit='ms'))
    new_bid_cols = list(pd.to_datetime(pd.to_numeric(bids.columns), unit='ms'))

    # Assign new column names
    asks.columns = new_ask_cols
    bids.columns = new_bid_cols

    # Convert timestamp columns from milliseconds to pandas datetime
    trades['timestamp'] = pd.to_datetime(trades['timestamp'], unit='ms')
    
    return {
        'asks': asks,
        'bids': bids, 
        'trades': trades
    }

def plot_trade_price(book: dict[str, pd.DataFrame]):
    # Get best ask prices over time
    best_asks = []
    best_bids = []
    for col in book['asks'].columns:
        col_data = book['asks'][col].dropna()
        if not col_data.empty:
            best_asks.append((col, col_data.index.min()))

    for col in book['bids'].columns:
        col_data = book['bids'][col].dropna()
        if not col_data.empty:
            best_bids.append((col, col_data.index.max()))
    
    best_asks = np.array(best_asks)
    best_bids = np.array(best_bids)

    buys = book['trades'][book['trades']['side'] == 'BUY']
    sells = book['trades'][book['trades']['side'] == 'SELL']

    plt.figure(figsize=(10, 5))
    plt.plot(best_asks[:, 0], best_asks[:, 1], color='blue', label='Best Ask')
    plt.plot(best_bids[:, 0], best_bids[:, 1], color='orange', label='Best Bid')
    plt.scatter(buys['timestamp'], buys['price'], color='green', label='Buy')
    plt.scatter(sells['timestamp'], sells['price'], color='red', label='Sell')
    plt.title('Book and Trades')
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.legend()
    plt.show()

def main():
    book = load_book_from_csv(Path().cwd().parent.parent / 'data/nhl-las-ana-2024-12-04/Golden Knights_book_history')
    plot_trade_price(book)

    ## TODO 
    """
    Best Bid (P_bid)
Best Ask (P_ask)
Mid Price ((P_bid + P_ask) / 2)
Spread (P_ask - P_bid)
Total Bid Volume (V_bid)
Total Ask Volume (V_ask)
Volume at Best Bid (V_bid_best)
Volume at Best Ask (V_ask_best)
Imbalance Ratios at multiple levels
Rolling Averages of Price (e.g., 5, 15, 30 timestamps)
Rate of Change in Volume or Price
Exponential Moving Averages (EMA) of prices or volumes
    """

if __name__ == '__main__':
    main()
