import multiprocessing as mp
import pandas as pd
import h5py
import os

def read_hdf(path, key):
    print(f"Reading stock: {key}, process_id: {os.getpid()}")
    df = pd.read_hdf(path, key)
    return df

if __name__ == "__main__":
    path = r"D:\HDF5\stocks.h5"
    f = h5py.File(path, "r")
    avail_stocks = list(f.keys())

    print(avail_stocks[:20])
    pool = mp.Pool(12)
    results = pd.DataFrame()

    # print("Number of cpu : ", mp.cpu_count())

    for key in avail_stocks[:20]:
        results = pd.concat([results, pool.apply_async(read_hdf, args=(path, key)).get()])
        # results.append(pool.apply_async(read_hdf, args=(path, key)).get())

    print(len(results))
    print(results.Symbol)
    
