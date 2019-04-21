class DataReader:
    def __init__(self):
        self.data = {}

    def csvFile(self, path):
        assert os.path.isfile(
            path), "You need to specify a file or the path doesnt exist."
        self.data = pd.read_excel(
            path,
            index_col="Date",
            nrows=100,
            names=["Open", "High", "Low", "Close", "Volume"],
        )

    def readFiles(self, path):
        assert os.path.isdir(
            path), "You need to specify a folder or the path doesnt exist."
        for file in os.listdir(path)[:2]:
            self._fileName = file.split(".txt")[0]
            _temp = pd.read_csv(path + file, nrows=100, index_col="Date")
            _temp.index.name = "Date/Time"
            _temp.index = pd.to_datetime(_temp.index)
            self.data[self._fileName] = _temp

data = DataReader()

data.readFiles(r"D:\AmiBackupeSignal")

# data.data["AAAP"].index