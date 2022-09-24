import pandas as pd
from pandas import DataFrame

data = DataFrame({'food':['bacon','pulled pork','bacon','Pastrami',
            'corned beef','Bacon','pastrami','honey ham','nova lox'],
                  'ounces':[4,3,12,6,7.5,8,3,5,6]})
meat_to_animal = {
    'bacon':'pig',
    'pulled pork':'pig',
    'pastrami':'cow',
    'corned beef':'cow',
    'honey ham':'pig',
    'nova lox':'salmon'    }

data['animal'] = data['food'].map(str.lower).map(meat_to_animal)

pd_series2 = data['food'].map(lambda x: meat_to_animal[x.lower()])

pd_series = data['food']

print((pd_series2[0], pd_series[0]))
for animal in pd_series2:
    print(animal)
for i in range(len(pd_series2)):
    print((pd_series[i], pd_series2[i]))


