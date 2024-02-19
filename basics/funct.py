# # -*- coding: utf-8 -*-
# """
# Created on Thu Feb  1 18:01:20 2024

# @author: 2076284
# """


import os

#os.listdir()

# def getsum(a,b,c):
#     return a+b+c

# #out=getsum(1, 2)


# out=getsum(1, 2,3)
# print(out)

# #print (getsum(1, 2,m))
# #print (getsum(1,,m))


# def calc(*args):
#     print (type(args)) #its a tuple
#     num=0
#     for i in args:
#         num=num+i
#     return num
    
# print (calc(1,2,3))
# print (calc(1,2,3,4,5,6))


# #keyword arg

# def calc(**args):
#     print(args,type(args))
#     print (id,args['name'])

# calc(id=24,name='Baskaran')



# import module_funct as m
# print (m.msum(1, 2))


# from Mypackage import calc as c
# print (c.msum(1, 2))

    
# from Mypackage.calc import mprod as p
# print (p(1, 2))
    
    
#global

c=10

def addsum(a,b):
    global c
    x=a+b+c
    print(x)
    return x

x=addsum(3,2)
print(x,c)


