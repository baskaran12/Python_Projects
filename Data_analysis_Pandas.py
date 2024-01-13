#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
data=pd.read_excel('Sales-Distribution-Practice-File.xlsx','Input Data')
df=pd.DataFrame(data)
df=df.groupby(['MONTH','REGION','PRODUCT'])['ACTUAL'].sum().reset_index()

df['RN'] = df.sort_values('ACTUAL', ascending=False).groupby(['MONTH','REGION']).cumcount() + 1
#df[(df.RN<=3)* (df.MONTH=='Mar') * (df.REGION=='North')]
df[(df.MONTH=='Mar')*(df.REGION=='Export')].sort_values(['MONTH','REGION','RN'])
df[df.RN<=3]


# In[ ]:


import pandas as pd
###athlete_df
athelete_df=pd.read_csv('Excels/athletes.csv')
athelete_df=pd.DataFrame(athelete_df)
###event_df
event_df=pd.read_csv('Excels/athlete_events.csv')

event_df=pd.DataFrame(event_df)
event_df
##joined_df  



# In[ ]:


athelete_df['athlete_id']=athelete_df.id
athelete_df
joined_df = pd.merge(athelete_df, event_df, on = 'athlete_id')

##which team has won the maximum gold medals over the years
country_wise_medcnt=joined_df[joined_df.medal=='Gold'].groupby('team')['medal'].count().reset_index().sort_values('medal',ascending=False)
country_wise_medcnt[country_wise_medcnt.medal==country_wise_medcnt.medal.max()]


# In[ ]:


##2 for each team print total silver medals and year in which they won maximum silver medal..output 3 columns
##team,total_silver_medals, year_of_max_silver
joined_df.medal.unique()

silver_cnt_year=joined_df[joined_df.medal=='Silver'].groupby(['team','year'])['medal'].count().reset_index().sort_values('medal',ascending=False)
silver_cnt_year['rn']=silver_cnt_year.sort_values('medal', ascending=False).groupby(['team']).cumcount() + 1

silver_cnt_df=silver_cnt_year[silver_cnt_year.rn==1].rename(columns={'medal':'silver_medal_cnt'})
silver_cnt_df=silver_cnt_df[['team','year','silver_medal_cnt']]
silver_cnt_df

country_total_silver_df=joined_df[joined_df.medal=='Silver'].groupby(['team'])['medal'].count().reset_index().sort_values('medal',ascending=False)

silver_cnt_df.merge(country_total_silver_df,on='team')


# In[ ]:


#3 which player has won maximum gold medals  amongst the players 
#which have won only gold medal (never won silver or bronze) over the years
joined_df.medal.unique()

ath_won_brz_silver_df=event_df[(event_df.medal=='Bronze')|(event_df.medal=='Silver')]
ath_won_brz_silver_df


# In[ ]:


import pandas as pd
ath_won_brz_silver_df=event_df[(event_df.medal=='Bronze')|(event_df.medal=='Silver')]
ath_won_brz_silver_df=ath_won_brz_silver_df['athlete_id'].reset_index().rename(columns={'athlete_id':'ath_id'})
ath_won_brz_silver_df
gold_ath_df=event_df[joined_df.medal=='Gold'].groupby(['athlete_id'])['medal'].count().reset_index().sort_values('medal',ascending=False)
final_df=pd.merge(gold_ath_df,ath_won_brz_silver_df, how='left',left_on='athlete_id', right_on='ath_id')
final_df[final_df.ath_id.isnull()].sort_values('medal',ascending=False)


# In[ ]:


#5 in which event and year India has won its first gold medal,first silver medal and first bronze medal
#print 3 columns medal,year,sport
india_gold_df=joined_df[(joined_df.medal=='Gold')*(joined_df.team=='India')].sort_values('year').head(1)


# In[ ]:





# In[ ]:





# In[ ]:


#4 in each year which player has won maximum gold medal . Write a query to print year,player name 
#and no of golds won in that year . In case of a tie print comma separated player names.
athlete_medal_cnt_df=event_df.groupby(['year','athlete_id'])['medal'].count().reset_index().rename(columns={'medal':'medal_cnt'})
#athlete_medal_cnt_df['Rank'] = athlete_medal_cnt_df.medal_cnt.rank(method='dense',ascending=False).astype(int).groupby('year')

##df['Rank'] = df.groupby('Year', sort=True).ngroup()+1
athlete_medal_cnt_df['rank']=athlete_medal_cnt_df.groupby('year')['medal_cnt'].rank(method='dense',ascending=False).astype(int)
athlete_medal_cnt_df[athlete_medal_cnt_df.rank==1]


# In[ ]:


#6 find players who won gold medal in summer and winter olympics both.

summer_gold_df=event_df[(event_df.season=='Summer')*(event_df.medal=='Gold')]
winter_gold_df=event_df[(event_df.season=='Winter')*(event_df.medal=='Gold')]

pd.merge(summer_gold_df['athlete_id'],winter_gold_df['athlete_id'],on='athlete_id')


# In[ ]:


summer_gold_df

