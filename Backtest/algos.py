import pandas as pd

def time_frame_set(df, to):
    """
    Converts dataframe to a desired frequency, then restores original indices, then ffill()
    """
    # ! currently converting to weekly uses Friday as last day. Might give bugs if friday does not exist in original dataframe.
    # TODO: when converting minute to daily, time drops, which results in NaNs when restoring index. Gotta find better TimeFrameExpand
    # for now hardcoded +9 hours and 30 mins
    if to=="W":
        from pandas.tseries.offsets import Week
        to = Week(weekday=4)

    temp = pd.DataFrame(columns=df.columns)
    temp.index.name = "Date"
    temp["Open"] = df["Open"].resample(to).first()
    temp["High"] = df["High"].resample(to).max()
    temp["Low"] = df["Low"].resample(to).min()
    temp["Close"] = df["Close"].resample(to).last()

    # TODO:
    # volume need to be chage for forex, etc cuz gives volume of -1
    # because of that, summing volume will produce wrong result
    temp["Volume"] = df["Volume"].resample(to).sum() 

    temp.index = temp.index + pd.Timedelta(hours=9, minutes=30)

    return temp

def time_frame_restore(current_asset, df_modif):
    restore_orig_index = pd.DataFrame(index=current_asset.index)
    df_modif.name = "Column1"
    restore_orig_index = restore_orig_index.join(df_modif, how="left").ffill()
    restore_orig_index = restore_orig_index["Column1"]

    return restore_orig_index

def stop_time(df, hour=0, minute=0, second=0):
    """
    For now supports only hour, minute, and second. 
    """
    # TODO: impove flexibility. Now requires all 3 parameters to be passed.
    import datetime as dt
    stop = df.index.time == dt.time(hour, minute, second)
    stop = pd.Series(stop, index=df.index)
    return stop

# def apply_stop(buy_or_short, indic, stop_type="fixed"):
#     if stop_type.lower()=="fixed":
#         # if buy -> sell_cond = C - indic <- since entry <- need to expand entry till exit
#         # if short -> cover_cond = C + indic <- since entry <- need to expand entry till exit
#         pass

# class Stop:
#     def __init__(self, buy_or_short, indic, stop_type="fixed"):
#         self.type = stop_type.lower()
#         self.buy_or_short = buy_or_short
#         self.values = indic
        