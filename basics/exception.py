# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 10:46:31 2024

@author: 2076284
"""

# try:
#     a=1/2
#     #a=open()
#     print(a)
# except Exception as e:
#     print ("this is exception")
#     print (e)
# else:
#     print ("this is else block")
# finally:
#     print ("this is final block")
    

#file

# f=open("C:/Users/2076284/Python_Course/Test.txt",'r')
# for line in f:
#     print (line)
# f.close()

line_list=[]
with open("C:/Users/2076284/Python_Course/Test.txt",'r') as f:
    print (f.read())
    for line in f:
        line_list.append(line)


with open("C:/Users/2076284/Python_Course/Test.txt",'w') as f:
    f.write("this is the first line \n")        
    with open("C:/Users/2076284/Python_Course/Test.txt",'r') as f:
        print (f.read())
        

with open("C:/Users/2076284/Python_Course/Test.txt",'a') as f:
    f.write("this is the second line ")


with open("C:/Users/2076284/Python_Course/Test.txt",'r') as f:
    
    line_list.append(f.readlines())













