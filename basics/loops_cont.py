# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 18:58:04 2024

@author: 2076284
"""

#looping over dict, only keys assigned to loop var
#


ipl={"CSK":{"Name":"Chennai Super Kings","Captain":["MSD","Rohit"]},
     "MI":{"Name":"Mumbai Indians","Captain":"Rohit"},
     (1,2):{"Name":"Chennai Super Kings","Captain":"Rohit"}
     }

team1,team2=("csk","mi")

# for i in ipl:
#     print (i)
#     for j in ipl[i]:
#         print (ipl[j])
    
# #unpackq

# for team,name in ipl:
#     print (team)
#     print(name)


#list comprehension


list_num=[1,2,3,4,5,6]
list_even=[i for i in list_num if i%2==0]



#exception handling



