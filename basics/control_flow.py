# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 15:52:02 2024

@author: 2076284
"""

# mark=int(input())
# if mark <40:
#     print('fail')
# elif mark >=40 and mark <60:
#     print('passed 2nd')
# elif mark>=60 and mark<80:
#     print ('passed 1st')
# else:
#     print ('Passed distinction')

# print ('outside if')


#loops

num_list=[1,2,3,4,5]
num_odd=[]
num_even=[]
# n=0
# while n<len(num_list):
#     if n%2==0:
#        num_even.append(num_list[n])
#     else :
#         num_odd.append(num_list[n])
#     n=n+1


for n in range(len(num_list)):
      if n%2==0:
         num_even.append(num_list[n])
      else :
          num_odd.append(num_list[n])
      n=n+1

    


