class Settings:
    # run options
    read_from = "csvFiles"
    read_from_csv_path = ""
    log_folder = "LOGS"
    log_name = "test.log"
    save_temp_parquet = "parq/temp"

    # simulation options
    start_amount = 10000

    # which col to use for calc buy/sell price
    buy_on = "Close"
    sell_on = "Close"
    short_on = "Close"
    cover_on = "Close"

    # number of bars to delay entry/exit
    buy_delay = 0
    sell_delay = 0
    short_delay = 0
    cover_delay = 0

    pct_invest = 0.1
    round_to_decimals = 0

    position_size_type = "pct"
    position_size_value = 0.1 # placeholder, will be used for "share" or "amount"

    generate_ranks = False
    rank_file_name = "rank.csv"
    order_ranks_desc = False

    path_to_mapping = r"auto_trading/mapping.json"

    backtest_engine = "pandas"
    use_complete_candles_only = True
