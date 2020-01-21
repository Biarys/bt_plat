import pandas as pd

def time_frame_adj(df, to):
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

    restore_orig_index = pd.DataFrame(index=df.index)
    restore_orig_index = restore_orig_index.join(temp, how="left").ffill()

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