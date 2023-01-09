from celery import Celery
import time
import multi_main
import aa_tax_main
import mnt_tax_main
import pg_tax_main
import logging
import test_excel_formating


app = Celery('task',backend='redis://127.0.0.1:6379', broker='redis://127.0.0.1:6379')


@app.task(bind=True)
def test_1(self,county,start_index,end_index):
    try:
        # time.sleep(5)
        multi_main.multi_main_fn(county,start_index,end_index)
        aa_tax_main.aa_tax_main_fn(start_index*50,end_index*50)
        return "test 1 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=60,max_retries=20)

@app.task(bind=True)
def test_2(self,county,start_index,end_index):
    try:
        # time.sleep(5)
        multi_main.multi_main_fn(county,start_index,end_index)
        aa_tax_main.aa_tax_main_fn(start_index*50,end_index*50)
        return "test 2 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=60,max_retries=20)

@app.task(bind=True)
def test_3(self,county,start_index,end_index):
    try:
        # time.sleep(10)
        multi_main.multi_main_fn(county,start_index,end_index)
        aa_tax_main.aa_tax_main_fn(start_index*50,end_index*50)
        return "test 3 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=60,max_retries=20)

@app.task(bind=True)
def test_4(self,county,start_index,end_index):
    try:
        # time.sleep(15)
        multi_main.multi_main_fn(county,start_index,end_index)
        aa_tax_main.aa_tax_main_fn(start_index*50,end_index*50)
        return "test 4 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=60,max_retries=20)

# @app.task(bind=True)
# def test_5(self,county,start_index,end_index):
#     try:
#         # time.sleep(20)
#         multi_main.multi_main_fn(county,start_index,end_index)
#         aa_tax_main.aa_tax_main_fn(start_index*50,end_index*50)
#         return "test 5 done"
#     except Exception as e:
#         print('exception raised, it would be retry after 5 seconds')
#         raise self.retry(exc=e, countdown=60,max_retries=20)

# @app.task(bind=True)
# def test_6(self,county,start_index,end_index):
#     try:
#         # time.sleep(25)
#         multi_main.multi_main_fn(county,start_index,end_index)
#         aa_tax_main.aa_tax_main_fn(start_index*50,end_index*50)
#         return "test 6 done"
#     except Exception as e:
#         print('exception raised, it would be retry after 5 seconds')
#         raise self.retry(exc=e, countdown=60,max_retries=20)




@app.task(bind=True)
def test_7(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        mnt_tax_main.mnt_tax_main_fn(start_index*50,end_index*50)
        return "test 7 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

@app.task(bind=True)
def test_8(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        mnt_tax_main.mnt_tax_main_fn(start_index*50,end_index*50)
        return "test 8 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

@app.task(bind=True)
def test_9(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        mnt_tax_main.mnt_tax_main_fn(start_index*50,end_index*50)
        return "test 9 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

@app.task(bind=True)
def test_10(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        mnt_tax_main.mnt_tax_main_fn(start_index*50,end_index*50)
        return "test 10 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

# @app.task(bind=True)
# def test_11(self,county,start_index,end_index):
#     try:
#         # time.sleep(25)
#         multi_main.multi_main_fn(county,start_index,end_index)
#         mnt_tax_main.mnt_tax_main_fn(start_index*50,end_index*50)
#         return "test 11 done"
#     except Exception as e:
#         print('exception raised, it would be retry after 5 seconds')
#         raise self.retry(exc=e, countdown=5,max_retries=20)

# @app.task(bind=True)
# def test_12(self,county,start_index,end_index):
#     try:
#         # time.sleep(25)
#         multi_main.multi_main_fn(county,start_index,end_index)
#         mnt_tax_main.mnt_tax_main_fn(start_index*50,end_index*50)
#         return "test 12 done"
#     except Exception as e:
#         print('exception raised, it would be retry after 5 seconds')
#         raise self.retry(exc=e, countdown=5,max_retries=20)




@app.task(bind=True)
def test_13(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        pg_tax_main.pg_tax_main_fn(start_index*50,end_index*50)
        return "test 13 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

@app.task(bind=True)
def test_14(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        pg_tax_main.pg_tax_main_fn(start_index*50,end_index*50)
        return "test 14 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

@app.task(bind=True)
def test_15(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        pg_tax_main.pg_tax_main_fn(start_index*50,end_index*50)
        return "test 15 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

@app.task(bind=True)
def test_16(self,county,start_index,end_index):
    try:
        # time.sleep(25)
        multi_main.multi_main_fn(county,start_index,end_index)
        pg_tax_main.pg_tax_main_fn(start_index*50,end_index*50)
        return "test 16 done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=5,max_retries=20)

# @app.task(bind=True)
# def test_17(self,county,start_index,end_index):
#     try:
#         # time.sleep(25)
#         multi_main.multi_main_fn(county,start_index,end_index)
#         pg_tax_main.pg_tax_main_fn(start_index*50,end_index*50)
#         return "test 17 done"
#     except Exception as e:
#         print('exception raised, it would be retry after 5 seconds')
#         raise self.retry(exc=e, countdown=5,max_retries=20)

# @app.task(bind=True)
# def test_18(self,county,start_index,end_index):
#     try:
#         # time.sleep(25)
#         multi_main.multi_main_fn(county,start_index,end_index)
#         pg_tax_main.pg_tax_main_fn(start_index*50,end_index*50)
#         return "test 18 done"
#     except Exception as e:
#         print('exception raised, it would be retry after 5 seconds')
#         raise self.retry(exc=e, countdown=5,max_retries=20)

@app.task(bind=True)
def test_csv(self):
    try:
        # time.sleep(25)
        test_excel_formating.test_excel_formating_fn()
        return "test csv done"
    except Exception as e:
        print('exception raised, it would be retry after 5 seconds')
        raise self.retry(exc=e, countdown=300,max_retries=5)

# celery -A thread_test worker --pool=gevent --concurrency=500 --loglevel=INFO -f celery.logs