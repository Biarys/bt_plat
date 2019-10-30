from datetime import datetime as dt

if __name__ == "__main__":
    while True:
        if dt.now().second == 5:
            print(dt.now())
