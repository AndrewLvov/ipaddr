#!/usr/bin/env python

from itertools import repeat
from time import time
import requests

COUNT = 10000
results = []
print('Starting tests for {} requests...'.format(COUNT))
start = time()
for i in range(1, COUNT):
    data = {
        'user_1': i,
        'user_2': COUNT+i,
    }
    response = requests.post('http://localhost:8888', data=data)
    results.append(response.text)
    # print('User 1: {}, User 2: {}, result: {}\n'.format(data['user_1'], data['user_2'], response.text))
end = time()
# output = open('output.txt', 'w')
# output.write(str(results))
# print(results)
time_range = end-start
print('Time elapsed: {}s'.format(time_range))
print('i.e. {}ms per request'.format(time_range*1000/COUNT))
# output.close()
