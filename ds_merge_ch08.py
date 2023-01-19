#!/usr/bin/env python
# coding: utf-8

# # 데이터 전처리 
# ## 데이터 병합 ch08
# 1. 주요 함수 및 메서드 소개
# - pandas - reset_index()
# - pandas - set_index()
# 

# In[2]:


import pandas as pd


# In[ ]:


import pandas as pd


# In[5]:


bike = pd.read_csv("bike.csv")
bike.head(2)


# In[4]:


bike


# In[7]:


bike_sub = bike.sample(n = 4, random_state = 123)
bike_sub


# In[8]:


bike_sub.reset_index()


# In[9]:


bike_sub.reset_index(drop = True)


# In[10]:


bike_sub


# In[11]:


bike_sub.reset_index()


# In[12]:


bike_sub


# In[13]:


bike_sub.set_index("datetime")


# In[14]:


bike_sub


# In[15]:


bike


# In[20]:


bike_1 = bike.iloc[:3, :4]
bike_2 = bike.iloc[5:8, :4]
print(len(bike_1))
print(len(bike_2))


# In[21]:


bike_1


# In[22]:


bike_2


# In[23]:


pd.concat([bike_1, bike_2]) # row


# In[24]:


pd.concat([bike_1, bike_2], axis =  1)


# In[25]:


pd.concat([bike_1, bike_2.reset_index(drop = True)], axis = 1)


# In[26]:


pd.concat([bike_1.reset_index(drop = True), bike_2.reset_index(drop = True)], axis = 1)


# In[27]:


df_A = pd.read_csv("join_data_group_members.csv")
df_B = pd.read_csv("join_data_member_room.csv")


# In[28]:


df_A.head(2)


# In[29]:


df_B.head(2)


# In[30]:


pd.merge(left = df_A, right = df_B, left_on = "member", right_on = "name", how = "inner")


# In[32]:


pd.merge(left = df_A, right = df_B, left_on = "member", right_on = "name", how = "left")


# In[33]:


df_join = pd.merge(left = df_A, right = df_B, left_on = "member", right_on = "name", how = "left")


# In[34]:


df_join


# In[35]:


df_join.isna().sum()


# In[36]:


df_join.isna()


# In[37]:


bike


# In[38]:


bike["datetime"] = pd.to_datetime(bike["datetime"])


# In[39]:


bike["datetime"]


# In[40]:


bike


# In[41]:


bike = pd.read_csv("bike.csv")


# In[42]:


bike


# In[43]:


bike["datetime"]


# In[46]:


bike["datetime"] = pd.to_datetime(bike["datetime"])
bike["hour"] = bike["datetime"].dt.hour


# In[48]:


bike_s2 = bike.loc[bike["season"] == 2]


# In[49]:


bike_s2


# In[50]:


bike_s4 = bike.loc[bike["season"] == 4]


# In[51]:


bike_s4


# In[55]:


print(bike_s2["season"].unique())


# In[57]:


print(bike_s4["season"].unique())


# In[58]:


bike_s2.groupby("hour")["registered"].mean().reset_index()


# In[59]:


bike_s2.groupby("hour").mean()


# In[62]:


bike_s2_agg = bike_s2.groupby("hour")["registered"].mean().reset_index()
bike_s4_agg = bike_s4.groupby("hour")["registered"].mean().reset_index()
bike_s2_agg


# In[65]:


bike_agg_bind = pd.concat([bike_s2_agg, bike_s4_agg], axis = 1)
bike_agg_bind.head()


# In[67]:


bike_agg_bind = bike_agg_bind.iloc[:,[0, 1, 3]]
bike_agg_bind.head()


# In[71]:


bike_agg_bind.rename(columns = {"registered" : "reg_s2", "registered" : "reg_s4"}, inplace = True)


# In[72]:


bike_agg_bind


# In[73]:


bike_agg_bind.rename(columns = {"reg_s4":"reg_s2"}, inplace = True)


# In[74]:


bike_agg_bind.head()


# In[75]:


bike_agg_bind.columns = ["hour", "reg_s2", "reg_s4"]


# In[77]:


bike_agg_bind.head()


# In[78]:


bike_agg_bind["diff"] = bike_agg_bind["reg_s2"] - bike_agg_bind["reg_s4"]


# In[79]:


bike_agg_bind.head()


# In[80]:


bike_agg_bind.iloc[[bike_agg_bind["diff"].idxmax()]]


# In[81]:


bike_agg_bind.iloc[bike_agg_bind["diff"].idxmax()]


# In[82]:


bike_agg_bind.iloc[[bike_agg_bind["diff"].idxmax()]]


# In[84]:


bike.head()


# In[85]:


bike.loc[(bike["humidity"] == 100) & bike["temp"] > 30]["count"].mean()


# In[87]:


bike.loc[(bike["humidity"] == 100) | bike["temp"] > 30]


# In[88]:


bike.loc[bike["humidity"] == 100]


# In[89]:


bike.loc[bike["temp"] > 30]


# In[90]:


bike.loc[(bike["temp"] > 30) & (bike["humidity"] == 100)]


# In[92]:


bike = pd.read_csv("bike.csv")
bike.head()


# In[93]:


bike["datetime"] = pd.to_datetime(bike["datetime"])
bike["date"] = bike["datetime"].dt.date
bike.head()


# In[94]:


bike_h100 = bike.groupby("date")["humidity"].max().reset_index()
bike_h100.head()


# In[96]:


bike_h100 = bike_h100.loc[bike_h100["humidity"] == 100]
bike_h100.head()


# In[97]:


len(bike_h100)


# In[98]:


bike_join = pd.merge(left = bike, right = bike_h100, how = "inner", left_on = "date", right_on = "date")
bike_join.head()


# In[99]:


bike_join_temp_up30 = bike_join.loc[bike_join["temp"] > 30]
bike_join_temp_up30.head()


# In[100]:


bike_join_temp_up30["count"].mean()


# In[102]:


bike = pd.read_csv("bike.csv")
bike.head()


# In[104]:


bike["datetime"] = pd.to_datetime(bike["datetime"])
bike["date"] = bike["datetime"].dt.date
bike.head()


# In[107]:


bike_h100 = bike.groupby(["date"])["humidity"].max().reset_index()
bike_h100.head()


# In[108]:


bike_h100 = bike_h100.loc[bike_h100["humidity"] == 100]
bike_h100.head()


# In[109]:


bike_join = pd.merge(left = bike, right = bike_h100, left_on = "date", right_on = "date", how = "inner")
bike_join.head()


# In[110]:


bike_join_temp_30up = bike_join.loc[bike_join["temp"] > 30]
bike_join_temp_30up.head()


# In[111]:


bike_join_temp_30up["count"].mean()


# In[112]:


bike = pd.read_csv("bike.csv")
bike.head()


# In[113]:


bike["datetime"] = pd.to_datetime(bike["datetime"])
bike["date"] = bike["datetime"].dt.date
bike.head()


# In[119]:


bike_h100 = bike.groupby(["date"])["humidity"].max().reset_index()
bike_h100.head()


# In[117]:


bike_h100 = bike_h100.loc[bike_h100["humidity"] == 100]
bike_h100 = bike_h100.loc[bike_h100["humidity"] == 100]x
bike_h100.head()


# In[121]:


bike_h100 = bike_h100.loc[bike_h100["humidity"] == 100]
bike_h100.head()


# In[122]:


bike_join = pd.merge(left = bike, right = bike_h100, left_on = "date", right_on = "date", how = "inner")
bike_join.head()


# In[123]:


bike_join_temp_up30 = bike_join.loc[bike_join["temp"] > 30]
bike_join_temp_up30.head()


# In[124]:


bike_join_temp_up30["count"].mean()


# ## 여름과 겨울의 시간대별 registered 평균을 비교할 때 가장 차이가 많이 나는 시각은?
# - bike.csv 파일 사용
# - hour attribute 활용

# In[126]:


bike = pd.read_csv("bike.csv")
bike.head()


# In[128]:


bike["datetime"] = pd.to_datetime(bike["datetime"])
bike["hour"] = bike["datetime"].dt.hour
bike.head()


# In[143]:


bike_s2 = bike.loc[bike["season"] == 2]
bike_s2.head()


# In[144]:


print(bike_s2["season"].unique())


# In[145]:


bike_s4 = bike.loc[bike["season"] == 4]
bike_s4.head()


# In[146]:


print(bike_s4["season"].unique())


# In[147]:


bike_s2_avg = bike_s2.groupby(["hour"])["registered"].mean().reset_index()
bike_s2_avg.head()


# In[148]:


bike_s2_avg.rename(columns = {"registered" : "reg_s2"}, inplace = True)
bike_s2_avg.head()


# In[149]:


bike_s4_avg = bike_s4.groupby(["hour"])["registered"].mean().reset_index()
bike_s4_avg.head()


# In[150]:


bike_s4_avg.rename(columns = {"registered" : "reg_s4"}, inplace = True)
bike_s4_avg.head()


# In[169]:


bike_bind = pd.concat([bike_s2_avg, bike_s4_avg], axis = 1)
bike_bind.head()


# In[170]:


bike_bind


# In[ ]:





# In[156]:


bike_bind = bike_bind.iloc[:, [0, 1, 3]]
bike_bind.head()


# In[158]:


bike_bind["diff"] = bike_bind["reg_s2"] - bike_bind["reg_s4"]
bike_bind.head()


# ## idxmax()는 각각 축에서 최대값의 인덱스를 반환하는 메소드
# ## iloc[단일정수] 단일정수로 인덱싱하는 경우 시리즈 형식으로 반환
# ## iloc[[list]] 리스트형식으로 인덱싱하는 경우 데이터프레임 형식으로 반환

# In[165]:


bike_bind.iloc[[bike_bind["diff"].idxmax()]]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




