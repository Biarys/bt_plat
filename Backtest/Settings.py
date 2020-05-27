# run options
read_from = "csvFiles"
read_from_csv_path = ""
log_folder = r"D:\bt_plat_logs"
log_name = "test.log"
save_temp_parquet = r"D:\parq\temp"

# data_frequency = "D"

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
# min_bars_hold
# max_bars_hold or exit_after_bars ?

# ? replace with dict?
pct_invest = 0.1
round_to_decimals = 0

# not implemented yet
# max_open_positions = None
# max_open_long = None
# max_open_short = None
# set_margin = 100

# # position size
# min_shares = 0
# min_position_value = 0
# max_shares = 0
# max_position_value = 0

generate_ranks = False
rank_file_name = "rank.csv"
order_ranks_desc = False

path_to_mapping = r"auto_trading/mapping.json"

backtest_engine = "pandas"