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
    # ? mgiht need to change to calc single column only
    temp["Open"] = df["Open"].resample(to).first()
    temp["High"] = df["High"].resample(to).max()
    temp["Low"] = df["Low"].resample(to).min()
    temp["Close"] = df["Close"].resample(to).last()

    temp.dropna(how="all", inplace=True)
    # TODO:
    # volume need to be chage for forex, etc cuz gives volume of -1
    # because of that, summing volume will produce wrong result
    temp["Volume"] = df["Volume"].resample(to).sum() 

    # temp.index = temp.index + pd.Timedelta(hours=9, minutes=30)

    return temp

def time_frame_restore(current_asset, df_modif):
    restore_orig_index = pd.DataFrame(index=current_asset.index)
    temp = pd.DataFrame(df_modif.values, index=df_modif.index.date)
    temp.reset_index(inplace=True)
    restore_orig_index["index"] = current_asset.index.date
    restore_orig_index = restore_orig_index.merge(temp, how="left", on="index")
    restore_orig_index.set_index(current_asset.index, inplace=True)
    restore_orig_index.drop("index", axis=1, inplace=True)
    if type(df_modif) == pd.Series:
        # restore_orig_index.columns = [df_modif.name]
        restore_orig_index = pd.Series(restore_orig_index[0])
        return restore_orig_index
    else:
        restore_orig_index.columns = df_modif.columns
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
        