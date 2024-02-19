def f_armstrong(a):

    n=a
    cube=0
    no_digit=0
    for i in str(a):
        no_digit=no_digit+1
    
    while n>0:
        rem=n%10
        cube=cube+pow(rem,no_digit)
        n=int(n/10)
    
    if cube==a:
        print (f"{a} is a armstrong")
    else:
        print("else part")
        print(cube)
        print (f"{a} is not a armstrong")
        
f_armstrong(4356)