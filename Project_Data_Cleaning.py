
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[122]:


df = pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')


# In[14]:


df.head(10) #Displays first 10 observations of dataframe df


# In[123]:


df.shape #Row and columns counts


# In[7]:


df.describe() #Displays the stats for all numeric variables


# In[16]:


#1. Get the Metadata from the above files.

df.info()


# In[20]:


df['Indicator'].value_counts() #Count of values for indicator


# In[23]:


df1 = pd.read_csv('https://raw.githubusercontent.com/kjam/data-wranglingpycon/master/data/berlin_weather_oldest.csv')
# There is an error reading the file - 404 error - file does not exist


# In[24]:


#2. Get the row names from the above files.
df.index.values


# In[27]:


#3. Change the column name from any of the above file.
df.rename(columns = {'Indicator' : 'Indicator_id'})


# In[30]:


#4. Change the column name from any of the above file and store the changes made permanently.
df.rename(columns = {'Indicator' : 'Indicator_id'},inplace = True)
df.head(2)


# In[31]:


# 5. Change the names of multiple columns.

df.rename(columns = { 'PUBLISH STATES' : 'Publication status', 'WHO region' : 'WHO Region'}, inplace = True)
df.head(2)


# In[38]:


# 6. Arrange values of a particular column in ascending order.

df = df.sort_values('Year')


# In[40]:


#7. Arrange multiple column values in ascending order.

df.sort_values(['Indicator_id', 'Country', 'Year', 'WHO Region', 'Publication status'])


# In[109]:


#8. Make country as the first column of the dataframe.

# List of all columns
cols = list(df)

# Pop function removes the item/variable at the index mentioned - here it is index of country
# Insert function inserts the variable from old position to new position
cols.insert(0, cols.pop(cols.index('Country')))
# Rearranges the columns in the order
df = df.loc[:, cols]
df.head(3)


# In[117]:


#9. Get the column array using a variable
df.iloc[:,[4]].as_matrix()


# In[133]:


#10. Get the subset rows 11, 24, 37
df_subset = df.iloc[[11,24,37], : ]
df_subset.head(10)

#OR as below


# In[141]:


#10. Get the subset rows 11, 24, 37

df_subset_method2 = df[df.index.isin([11,24,37])]
df_subset_method2.head()


# In[137]:


# 11. Get the subset rows excluding 5, 12, 23, and 56
df_subset = df.drop(df.index[[5,12, 23, 56]])
df_subset.head(60)


# In[237]:


users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')


# In[144]:


users.head()


# In[145]:


sessions.head()


# In[146]:


products.head()


# In[147]:


transactions.head()


# In[149]:


# 12. Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)
Left_Join = pd.merge(transactions, users, on='UserID', how='left')
Left_Join


# In[173]:


# 13. Which transactions have a UserID not in users?
Missing_Users = Left_Join[Left_Join['User'].isnull()]
Missing_Users.drop(['User', 'Gender', 'Registered', 'Cancelled'],axis=1)


# In[177]:


# 14. Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)
Inner_Join = pd.merge(transactions, users, on="UserID", how="inner")
Inner_Join


# In[253]:


# 15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)
Full_Join = pd.merge(transactions, users, on="UserID", how="outer")
Full_Join


# In[187]:


# 16. Determine which sessions occurred on the same day each user registered
Left_Join = pd.merge(users, sessions, how = "left", on="UserID")
Left_Join

# Registered date is same as Session date
Required_Output = Left_Join [Left_Join['Registered'] == Left_Join['SessionDate']]
Required_Output


# In[208]:


# However, if we are looking for no cancellation on the same day as registration, then
Required_Output = Left_Join [ (Left_Join['Registered'] == Left_Join['SessionDate']) & (Left_Join['Cancelled'].isnull()) & (Left_Join['Cancelled'] == Left_Join['Registered']) ]
Required_Output


# In[233]:


# Approach 1
#17. Build a dataset with every possible (UserID, ProductID) pair (cross join)
Combinations = users.assign(foo=1).merge(products.assign(foo=1))
Combinations.filter(['UserID', 'ProductID'])


# In[246]:


# Approach 2s
#17. Build a dataset with every possible (UserID, ProductID) pair (cross join)
users['temp'] = 1
products['temp'] = 1
users.head(2)
products.head(2)
Users = users.filter(['UserID', 'temp'])
Products = products.filter(['ProductID', 'temp'])
Combinations = pd.merge(Users, Products, on= 'temp')
Combinations.drop(['temp'],axis=1)


# In[257]:


#18. Determine how much quantity of each product was purchased by each user

All_Combinations = pd.merge(Combinations[['UserID', 'ProductID']], Full_Join[['UserID', 'ProductID', 'Quantity']], how="left", on = ['UserID', 'ProductID'])
All_Combinations.groupby(['UserID', 'ProductID'])['Quantity'].aggregate('sum')


# In[ ]:


# 19. For each user, get each possible pair of pair transactions (TransactionID1, TransacationID2)


# In[265]:


# 20. Join each user to his/her first occuring transaction in the transactions table

#First, sort the users based on their transaction date
transacttions_sorted = transactions.sort_values(['UserID', 'TransactionDate'])
transacttions_sorted


# In[267]:


# REmove all the duplicate occurance of user id based
Dedup = transacttions_sorted.drop_duplicates(subset ="UserID") 
Dedup


# In[270]:


# Joining the users with their first occurance
First = pd.merge(users, Dedup, how="left", on="UserID")
First

