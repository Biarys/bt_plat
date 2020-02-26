import pstats

with open(r'D:\port_raw.txt', 'w') as stream:
    stats = pstats.Stats(r'D:\profiling2', stream=stream)
    stats.sort_stats('cumtime').print_stats()