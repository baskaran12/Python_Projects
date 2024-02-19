# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 13:10:21 2024

@author: 2076284
"""

#list
ipl_team=['csk','mi','rcb','kkr',1,2]
ipl_team.append('kxip')
ipl_team.insert(4, 'kxip')

a=[]

##2nd way using split

country="india,japan,us,china"
country=country.split(",")

#3 method

python_list=list("python")

num_list=["1","2","mi","csk"]
num_copy=num_list.copy()

#list of list
list_of_list=[[1,2],[3,4],['mi',]]

#shallow copy

list_copy=list_of_list.copy()

list_copy[0][0]=2


#tuple
a=(1,2,3)
a=(5,6)


#dict

ipl={}
ipl={"CSK":"Chennai Super Kings",
     "MI":"Mumbai Indians"
     }

ipl["RCB"]="Royal Challeng Bangalore"



del ipl["CSK"]


ipl={"CSK":{"Name":"Chennai Super Kings","Captain":["MSD","Rohit"]},
     "MI":{"Name":"Mumbai Indians","Captain":"Rohit"},
     (1,2):{"Name":"Chennai Super Kings","Captain":"Rohit"}
     }

ipl["CSK"]['Captain'][0]

ipl.pop()
ipl.pop((1,2))

#key cant be a list



#control flow



