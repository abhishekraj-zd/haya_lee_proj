from thread_test import test_1
from thread_test import test_2
from thread_test import test_3
from thread_test import test_4
# from thread_test import test_5
# from thread_test import test_6
from thread_test import test_7
from thread_test import test_8
from thread_test import test_9
from thread_test import test_10
# from thread_test import test_11
# from thread_test import test_12
from thread_test import test_13
from thread_test import test_14
from thread_test import test_15
from thread_test import test_16
# from thread_test import test_17
# from thread_test import test_18
from thread_test import test_csv
# from celery import group
# import time
# import threading
# from celery import chain
# from celery import group
# from multi_main import multi_main_fn
# start = time.time()
try :
    data_1 = test_1.delay(0,0,1500)
    # data_1 = test_1.delay(0,0,5)
except Exception as e:
    print("Error >>>>> ",e)
try:
    data_2 = test_2.delay(0,1500,3000)
    # data_2 = test_2.delay(0,6,10)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_3 = test_3.delay(0,3000,4500)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_4 = test_4.delay(0,4500,6000)
except Exception as e:
    print("Error >>>>> ", e)
# try:
#     data_5 = test_5.delay(0,20,25)
# except Exception as e:
#     print("Error >>>>> ", e)
# try:
#     data_6 = test_6.delay(0,25,30)
# except Exception as e:
#     print("Error >>>>> ", e)


try :
    data_7 = test_7.delay(1,0,1500)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_8 = test_8.delay(1,1500,3000)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_9 = test_9.delay(1,3000,4500)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_10 = test_10.delay(1,4500,6000)
except Exception as e:
    print("Error >>>>> ", e)
# try:
#     data_11 = test_11.delay(1,21,25)
# except Exception as e:
#     print("Error >>>>> ", e)
# try:
#     data_12 = test_12.delay(1,26,30)
# except Exception as e:
#     print("Error >>>>> ", e)


try :
    data_13 = test_13.delay(2,0,1500)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_14 = test_14.delay(2,1500,3000)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_15 = test_15.delay(2,3000,4500)
except Exception as e:
    print("Error >>>>> ", e)
try:
    data_16 = test_16.delay(2,4500,6000)
except Exception as e:
    print("Error >>>>> ", e)
# try:
#     data_17 = test_17.delay(2,21,25)
# except Exception as e:
#     print("Error >>>>> ", e)
# try:
#     data_18 = test_18.delay(2,26,30)
# except Exception as e:
#     print("Error >>>>> ", e)

data_1.get()
data_2.get()
data_3.get()
data_4.get()
# data_5.get()
# data_6.get()
data_7.get()
data_8.get()
data_9.get()
data_10.get()
# data_11.get()
# data_12.get()
data_13.get()
data_14.get()
data_15.get()
data_16.get()
# data_17.get()
# data_18.get()

try:
    data_csv = test_csv.delay()
except Exception as e:
    print("Error >>>>> ", e)
