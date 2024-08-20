import pandas as pd
df=pd.read_csv('orders.txt')
dfr=pd.read_csv('returns.txt')
dfr.columns
df[['order_id','category','sales']]
df_merge=pd.merge(left=df,right=dfr,how='left',left_on='order_id',right_on='Order Id')

#city wise count of return orders
returned_orders=df_merge[~df_merge['Return Reason'].isna()]
count=returned_orders.city.value_counts()
count

#cities where we have all 3 kinds of returns (others,bad quality,wrong items)
city_wise=returned_orders.groupby('city')['Return Reason'].nunique()
#city_wise.values
city_wise_df=pd.DataFrame({'city':city_wise.index,'count_of_reason':city_wise.values})
city_wise_df[city_wise_df['count_of_reason']==3]


#cities where not even a single order was returned
returned_orders
non_returned_orders=df_merge[df_merge['Return Reason'].isna()]
city_nosingle_return=non_returned_orders[~non_returned_orders.city.isin(returned_orders['city'])]
#fc=returned_orders['city']=='Albuquerque'
city_nosingle_return.city.count()


city_return = df_merge.groupby('city',as_index=False)['Return Reason'].count()
city_return=city_return[city_return.values==0]
city_return
# fc1=city_return['city']=='Albuquerque'
# city_return[fc1]
city_return[~city_return.city.isin(city_nosingle_return['city'])]

#top 3 cities by sales
city_wise_total_sale=df_merge.groupby('city',as_index=False)['sales'].sum()
city_wise_total_sale.sort_values(by='sales',ascending=False).head(3)


#order ids whos return reason is not known (nan)
dfr[dfr['Return Reason'].isna()]


fc=dfr['Return Reason']=='others'
#dfr.loc[fc,'Return Reason']='others'
dfr[fc]


df_merge['Return Reason'].fillna('Not Returned')


df.groupby('category').agg({'sales':'mean','profit':'mean'})

import pandas as pd
df=pd.read_csv('orders.txt')
df['order_date']=pd.to_datetime(df['order_date'])
df.dtypes