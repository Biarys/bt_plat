import sys
import os 
# sys.path.insert(0, "../")
# sys.path.insert(0, "")
# sys.path.append(os.path.realpath('../'))
sys.path.append(sys.path[0] + "/..")
# from ..Core import platform_core as bt

import Core.platform_core

def test_single_stocks():
    pass

def test_portfolio():
    pass


if __name__=="__main__":
    s = bt.Strategy()
    
    s.run()