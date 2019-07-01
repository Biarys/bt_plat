# # old version
# class TransPrice_1(TradeSignal):
#     # inheriting from tradeSingal cuz of inTrade
#     """
#     Raw transaction price meaning only initial buy and sell prices are recorded without forward fill
#     """

#     def __init__(self, rep, buyOn="Close", sellOn="Close"):
#         # buy price & sell price
#         rep = rep
#         super().__init__(rep)
#         self.buyPrice = rep.data[buyOn][self.buyCond == 1]
#         self.sellPrice = rep.data[sellOn][self.sellCond == 1]

#         self.buyPrice.name = rep.name
#         self.sellPrice.name = rep.name

#         cond = [
#             (self.buyCond == 1),
#             (self.sellCond == 1)
#         ]
#         out = ["Buy", "Sell"]
#         self.inTrade = np.select(cond, out, default=0)
#         self.inTrade = pd.DataFrame(
#             self.inTrade, index=rep.data.index, columns=[rep.name])
#         self.inTrade = self.inTrade.replace("0", np.NAN)
#         self.inTrade = self.inTrade.ffill().dropna()
#         self.inTrade = self.inTrade[self.inTrade == "Buy"]

#         self.inTradePrice = rep.data["Close"].loc[self.inTrade.index]
#         self.inTradePrice.name = rep.name
