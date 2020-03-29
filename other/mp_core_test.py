"""
Find the number of optimum cores to be used in a Pool.
Run the script. The faster the result, the better.
"""
import multiprocessing
import time
import os

def sleep_a_second():
    print("Sleepin'")
    print(os.getpid())
    time.sleep(1)


def timing_test(num_processes):
    pool = multiprocessing.Pool(num_processes)
    start_time = time.time()
    results = [pool.apply_async(sleep_a_second) for _ in range(10)]
    for r in results:
        r.wait()
    stop_time = time.time()
    print(f"OK, with {num_processes} it took {stop_time - start_time} seconds")
    print(num_processes * (stop_time - start_time))

if __name__ == "__main__":
    # change number of cores to be tested here
    for i in range(1, 20):
        timing_test(i)