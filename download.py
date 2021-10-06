import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime
import pickle as pkl
import multiprocessing as mp
import matplotlib.pyplot as plt
from tqdm import tqdm
import pyarrow.feather as feather
import time


# ###################
# # Connect to WRDS #
# ###################
conn = wrds.Connection()


# Download data and store locally



# fundq by country (headquarter)

fundq = conn.raw_sql("""
                      select *
                      from comp.g_fundq
                      where exchg = 170
                      """)

fundq = fundq.sort_values(['gvkey','datadate','iid','isin','sedol']) # order by gvkey, date, issue id, other id's
with open('./fundq_hkg.feather', 'wb') as f:
   feather.write_feather(fundq, f)


# secd by country (headquarter)


secd = conn.raw_sql("""
                      select *
                      from comp.g_secd
                      where datadate >= '01/01/1990'
                      and exchg = 170
                      """)
secd = secd.sort_values(['gvkey','datadate','iid','isin','sedol']) # order by gvkey, date, issue id, other id's
with open('./secd_hkg.feather', 'wb') as f:
    feather.write_feather(secd, f)



funda = conn.raw_sql("""
                      select *
                      from comp.g_funda
                      where datadate >= '01/01/1990'
                      and exchg = 170
                      """)
funda = funda.sort_values(['gvkey','datadate','exchg','isin','sedol'])
with open('./funda_hkg.feather', 'wb') as f:
   feather.write_feather(funda, f)

