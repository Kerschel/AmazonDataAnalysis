import pandas as pd
import gzip
import time
from  math import sqrt
from sendemail import send
import sys
def parse(path):
  g = gzip.open(path, 'rb')
  for l in g:
    yield eval(l)



def getmetadata(path):
  i=0
  prices ={}

  for d in parse(path):


    try:
      if 'brand' not in d:
        prices[d['asin']] = { "price":d['price']
                              }
      # else:
      #   prices[d['asin']] = { "price":d['price'],
      #                         "ID":d['asin'],
      #                         "categories":d['categories'][0][0],
      #                         "brand":1
      #                         }
      i=i+1
    except Exception:
      print(d)

  # df = pd.DataFrame.from_dict(prices, orient='index').sort_values(by=['price'], ascending=False)
  return prices




def getDF(path,items):
  i = 0
  df = {}
  rate={}
  average={}
  notusefull = {}
  usefull = {}
  usecount ={}
  time ={}
  unixtime ={}

  for d in parse(path):

    try:
        usefull[d['asin']] is not None
        usefull[d['asin']] += d['helpful'][0]
    except Exception:
        usefull[d['asin']] = d['helpful'][0]

    try:
      unixtime[d['asin']]=d['unixReviewTime']
      time[d['asin']] = d['reviewTime']
    except Exception:
      unixtime[d['asin']]=None
      time[d['asin']] = None
    try:
        notusefull[d['asin']] is not None
        notusefull[d['asin']] += d['helpful'][1]
    except Exception:
        notusefull[d['asin']] = d['helpful'][1]
    try:
        usecount[d['asin']] is not None
        usecount[d['asin']] += 1
    except Exception:
        usecount[d['asin']] = 1


    try:
          rate[d['asin']] is not None
          rate[d['asin']] += d['overall']
    except Exception:
          rate[d['asin']] = d['overall']

    average[d['asin']] = rate[d['asin']]/usecount[d['asin']]
  
  for item in usefull :
    if time[item] != None:
      try:
        df[i]={
            'Product': item,
            'Helpful' : usefull[item],
            'NotHelpful':notusefull[item],
            'Average':average[item],
            'Reviews':usecount[item],
            'Category':items[item]['categories'],
            'Price':items[item]['price'],
            'Brand':items[item]['brand'],
            'Time':time[item],
            'UnixTime':unixtime[item]
        }
      except Exception:
        pass
    i=i+1
  return pd.DataFrame.from_dict(df, orient='index')



if __name__ == "__main__":
  start_time = time.clock()
  print ("Processing Meta data")
  frame = getmetadata('metadata.json.gz')
  print ("Processing amazon")
  df = getDF('aggressive_dedup.json.gz',frame)
  df.to_csv("exploratory.csv", sep=',', encoding='utf-8')
  mins = ( (time.clock() - start_time)/60, "minutes")
  print (mins)
