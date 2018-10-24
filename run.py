#!/usr/bin/env python

import pandas as pd
import multiprocessing
import numpy as np
import time

def gen_read_write(R=10):
    gen=np.zeros(R)
    write=np.zeros(R)
    read=np.zeros(R)
    for r in range(R):
        t0=time.time()
        x=np.random.rand(1000000,10,50)
        x=np.mean(x,axis=2)
        d=np.random.randn(1000000,10)
        y=np.exp(d)+x
        y=pd.DataFrame(y)
        gen[r]=time.time()-t0

        t0=time.time()
        y.to_csv('y.csv',index=False)
        write[r]=time.time()-t0

        t0=time.time()
        y=pd.read_csv('y.csv')
        read[r]=time.time()-t0
    print('average data generation time: %.1f seconds' % np.mean(gen))
    print('average data write time: %.1f seconds' % np.mean(write))
    print('average data read time: %.1f seconds' % np.mean(read))

def big_calc(i):
    x_norm=np.mean(np.random.randn(1000,50,10),axis=2)
    x_unif=np.mean(np.random.rand(1000,50,10),axis=2)
    return(i+np.mean(np.abs(x_norm)**np.abs(x_unif)))

def single(R=10):
    single_time=np.zeros(R)
    for r in range(R):
        t0=time.time()
        result=list(map(big_calc,range(1000)))
        single_time[r]=time.time()-t0
    print('average computing time (single-process): %.1f seconds' % np.mean(single_time))

def multi(R=10):
    multi_time=np.zeros(R)
    for r in range(R):
        t0=time.time()
        pool=multiprocessing.Pool()
        result=pool.map(big_calc,range(1000))
        multi_time[r]=time.time()-t0
    # print('# processes used: %d' % multiprocessing.cpu_count())
    print('average computing time (%d-process): %.1f seconds' % (multiprocessing.cpu_count(),np.mean(multi_time)))

if (__name__=='__main__'):
    gen_read_write()
    single()
    multi()
