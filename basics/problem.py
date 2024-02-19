# -*- coding: utf-8 -*-
"""
Created on Sat Feb  3 12:54:12 2024

@author: 2076284
"""

#class and objects

# a=int(input())
# n=a
# cube=0
# no_digit=0
# for i in str(a):
#     no_digit=no_digit+1

# while n>0:
#     rem=n%10
#     print(rem)
#     cube=cube+pow(rem,no_digit)
#     print(cube)
#     n=int(n/10)
#     print(n)

# if cube==a:
#     print (f"{a} is a armstrong")
# else:
#     print("else part")
#     print(cube)
#     print (f"{a} is not a armstrong")    


# a=input()

# if a==a[::-1]:
#     print(f'{a} is a palindrome')
# else:
#     print(f'{a} is not a palindrome')    


a=int(input())
cnt=0
div=0
if a==0 or a==2:
    print(f'{a} is a prime number')
else:
    for i in range(2,a):
        if a%i==0:
            cnt=cnt+1
            div=i
            break
        else:
            continue
if cnt==0:
    print(f'{a} is a prime number')
else:
    print(f'{a} is not a prime number and the divisor is {div}')
    























