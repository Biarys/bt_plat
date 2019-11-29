# SKIP FOR NOW
# PROBABLY NOT NEEDED

# import pandas as pd


# #############################################
# # Calculate portfolio part
# #############################################


# def runPortfolio():
#     """
#     Calculate profit and loss for the stretegy
#     """
#     t.weights = pd.DataFrame(
#         index=t.inTradePrice.index, columns=t.inTradePrice.columns)
#     t.priceChange = t.inTradePrice - t.inTradePrice.shift()

#     # Fill with 0s, otherwise results in NaN for port.availAmount
#     t.priceChange.fillna(0, inplace=True)
#     # calc portfolio change
#     port.value = pd.DataFrame(
#         index=t.inTradePrice.index, columns=["Portfolio value"])
#     port.value.iloc[0] = port.startAmount

#     port.availAmount = pd.DataFrame(
#         index=t.inTradePrice.index, columns=["Available amount"])
#     port.availAmount.iloc[0] = port.startAmount
#     # port.availAmount.ffill(inplace=True)

#     port.invested = pd.DataFrame(
#         index=t.inTradePrice.index, columns=t.weights.columns)
#     port.invested.iloc[0] = 0
#     # put trades in chronological order
#     # t.trades.sort_values("Date/Time_entry", inplace=True)
#     # t.trades.reset_index(drop=True, inplace=True)

#     # set weights to 0 when exit
#     # t.weights.loc[atp.sellPrice.index] = 0

#     # change change to avoid error
#     atp.buyPrice.columns = t.weights.columns
#     atp.sellPrice.columns = t.weights.columns

#     # atp.buyPrice2 = pd.DataFrame(index=t.inTradePrice.index)
#     # atp.buyPrice2 = pd.concat([atp.buyPrice2, atp.buyPrice], axis=1)
#     # atp.buyPrice2.ffill(inplace=True)

#     # allocate weights
#     for ix, row in t.weights.iterrows():
#         # weight = port value / entry
#         prev_bar = port.availAmount.index.get_loc(ix) - 1

#         # not -1 cuz it will replace last value
#         if prev_bar != -1:
#             # update avail amount
#             port.availAmount.loc[ix] = port.availAmount.iloc[prev_bar]

#             # update invested amount
#             port.invested.loc[ix] = port.invested.iloc[prev_bar]

#             # update weight anyway cuz if buy, the wont roll for other stocks
#             t.weights.loc[ix] = t.weights.iloc[prev_bar]

#         # if there was an entry on that date
#         # allocate weight
#         # update avail amount
#         if ix in atp.buyPrice.index:
#             toInvest = port.availAmount.loc[ix, "Available amount"] * 0.1
#             stocksAffected = atp.buyPrice.loc[ix].dropna().index.values
#             t.weights.loc[ix, stocksAffected] = (
#                 toInvest / atp.buyPrice.loc[ix, stocksAffected])
#             port.invested.loc[ix, stocksAffected] = toInvest
#             port.availAmount.loc[ix] -= port.invested.loc[ix].sum()

#         # if there was an exit on that date
#         # set weight to 0
#         # update avail amount
#         if ix in atp.sellPrice.index:
#             # prob need to change this part for scaling implementation
#             stocksAffected = atp.sellPrice.iloc[0].dropna().index.values
#             # amountRecovered = t.weights.loc[ix, stocksAffected] * atp.buyPrice2.loc[ix, stocksAffected]
#             port.availAmount.loc[ix] += port.invested.loc[
#                 ix, stocksAffected].sum()
#             port.invested.loc[ix, stocksAffected] = 0
#             t.weights.loc[ix, stocksAffected] = 0

#         # # if no new trades/exits
#         # # update weight
#         # else:
#         #     t.weights.loc[ix] = t.weights.iloc[prev_bar]
#         #     pass
#         #         prev_bar = port.availAmount.index.get_loc(ix) - 1
#         #         if prev_bar != -1:
#         #             port.availAmount.loc[ix] = port.availAmount.iloc[prev_bar]
#         # update avail amount for gains/losses that day
#         # done in the end to avoid factroing it in before buy
#         # if != -1 to skip first row
#         if prev_bar != -1:
#             port.availAmount.loc[ix] += (
#                 t.priceChange.loc[ix] * t.weights.loc[ix]).sum()

#         # profit = weight * chg
#         # portfolio value += profit


# runPortfolio()
