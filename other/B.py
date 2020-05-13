# define backtest settings
settings.1
settings.2

# define strategy
class Strategy():
    ...

# create strate object
s = Strategy()

# run backtest
s.run()

# define broker settings
settings.broker1
settings.broker2
settings.run_every(n)

# connect to broker
app = IBApp()
app.connect("127.0.0.1", 7497, 0) #4002 for gateway, 7497 for TWS
app.start(s)


# to create spark df
# spark = pyspark.sql.SparkSession.builder.master("local").appName("test").getOrCreate()
# sdf1 = spark.createDataFrame(df1, FloatType()) # need to specify type, otherwise error