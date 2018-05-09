
# coding: utf-8

# In[41]:


# import quandl to get AAPL data
import quandl
import pandas as pd


# In[38]:


df = quandl.get('WIKI/AAPL')


# In[ ]:


# alternatively can use the sample file provided
df = pd.read_csv("path/to/the/file.csv")


# In[39]:


df


# In[31]:


# calculates moving average
def ma(data, n):
    return data.rolling(n).mean()


# In[42]:


# takes close and 20 periods in this case
df["ma"] = ma(df['Adj. Close'], 20)


# In[43]:


df

