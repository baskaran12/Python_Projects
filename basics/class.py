# -*- coding: utf-8 -*-
"""
Created on Sun Feb  4 18:41:22 2024

@author: 2076284
"""

class car:
    def __init__(self,make,model,year,speed):
        self.make=make
        self.model=model
        self.year=year
        self.speed=speed
    def start(self,inc):
    
        self.speed=self.speed+inc
    def stop(self):
        self.speed=0

car1=car('mahindra','abc',2020,10)
a=car1.start(10)