import pandas as pd
import gzip
import time
from  math import sqrt
from sendemail import send
import sys
import statistics
def parse(path):
  g = gzip.open(path, 'rb')
  for l in g:
    yield eval(l)



def getmetadata(path):
  i=0
  prices ={}
  for d in parse(path):
        
    try:
      prices[d['asin']] = { "price":d['price'],
                            "ID":d['asin']
                            }
      i=i+1
    except Exception:
      pass  
   
  df = pd.DataFrame.from_dict(prices, orient='index').sort_values(by=['price'], ascending=False)
  return df,prices


def findMedianReview(dfa):
  dfa = dfa.sort_values(by=['Reviews_Amt'], ascending=True)
  size = dfa.shape[0]
  with open("Results.txt", "a") as myfile:

    if size % 2 == 0:
      half =int(size /2)
      myfile.write ("\nMedian item is " +  dfa.iloc[half]['Product'] +"\n")
    else:
      half =int(size /2)
      myfile.write("\nMedian items are " + dfa.iloc[half]['Product'] + "and "+dfa.iloc[half+1]['Product'] + "\n")

def writetofile(text):
  with open("Results.txt", "a") as myfile:
    myfile.write("\n"+text)


def getDF(path,items):
  i = 0
  df = {}
  vals={}
  sums={}
  rate={}
  average={}
  person = {}
  person_help = {}
  mode =[0,0,0,0,0,0]
  x_sqred = 0
  y_sqred = 0
  x_sum_sq = 0
  y_sum_sq = 0
  SumY = 0
  SumX =0
  SumXbyY =0
  count = 0
  modereview = {}
  median = {}
  kmeans = {}

  for d in parse(path):

    vals[d['asin']] = d['asin']
    try:
      kmeans[d['reviewerID']] = { "spending":round(kmeans[d['reviewerID']]['spending'] + items.loc[d["asin"]]['price'],2),
                                  "quantity":kmeans[d['reviewerID']]['quantity'] +1,
                                  "rateperitem":kmeans[d['reviewerID']]['rateperitem']+ d['overall']
                                }
    except Exception:
      if d["asin"] in items['ID'] :
        kmeans[d['reviewerID']] = { "spending":round(items.loc[d["asin"]]['price'],2),
                                    "quantity":1,
                                    "rateperitem":d['overall']

                                  }
    # Check if we get a error if we dont that means there is stuff in it
    try:
      person[d['reviewerID']] is not None
      person[d['reviewerID']] += 1
      # person[d['reviewerID']][1] += d['helpful'][0]
      # print (person[d['reviewerID']][1])
    except Exception:
      person[d['reviewerID']] = 1


    try:
      person_help[d['reviewerID']] is not None
      person_help[d['reviewerID']] += d['helpful'][0]
    except Exception:
      person_help[d['reviewerID']] = d['helpful'][0]

    mode[int(d['overall'])] +=1

    try:
        rate[d['asin']] is not None
        rate[d['asin']] += d['overall']
    except Exception:
        rate[d['asin']] = d['overall']
    try:
        sums[d['asin']]  
        sums[d['asin']] += 1
    except Exception:
        sums[d['asin']] = 1
    
    try:
      modereview[d['asin']][int(d['overall'])] +=1
    except Exception:
      modereview[d['asin']] =[0,0,0,0,0,0]
      modereview[d['asin']][int(d['overall'])] +=1

    try:
      median[d['asin']] += "," + str(int(d['overall'])) 
    except Exception:
      median[d['asin']] =str(int(d['overall']))

    average[d['asin']] = rate[d['asin']]/sums[d['asin']]

  # Pearson's r
  # Ex^2 and Ey^2
    x_sqred += (d['overall'] * d['overall'])
    y_sqred += (d['helpful'][0]**2)

#   to be squared lower down 
    x_sum_sq += d['overall'] 
    y_sum_sq += d['helpful'][0]
    
    # print (x_sqred,x_sum_sq)
# E x*y
    SumXbyY += d['overall'] * d['helpful'][0]


# Ex and Ey
    SumX = x_sum_sq
    SumY = y_sum_sq
    count = count +1



# (Ex)^2 and (Ey)^2
  x_sum_sq = x_sum_sq * x_sum_sq
  y_sum_sq = y_sum_sq ** 2
  

  PearsonR = (SumXbyY - ((SumX*SumY)/count)) / ( sqrt( (x_sqred  -(x_sum_sq/count))  ) * (sqrt( (y_sqred  - (y_sum_sq/count) )) ))
  writetofile("Peason's r coefficient = " + str(PearsonR))

  highest =0
  most_reviewed=""
  for k in kmeans:
    kmeans[k]['rateperitem'] = round(kmeans[k]['rateperitem']/kmeans[k]['quantity'],2)
    kmeans[k]['spending'] = round(kmeans[k]['spending']/kmeans[k]['quantity'],2)

  for item in vals :
    df[i]={
        'Product': item,
        'Reviews_Amt' : sums[item],
        'Average':average[item],
        'Mode':modereview[item].index(max(modereview[item])),
        # convert strings to list then convert str in list to int then find median
        'Median': statistics.median(list(map(int, (median[item].split(",") ))))
    }
    if (sums[item] > highest):
      highest = sums[item]
      most_reviewed = item
    i=i+1

  most_useful =0
  most = 0
  useful_person =""
  review_person =""
  for p in person:
    #  finding which person reviewed the most items
    if(person[p] > most):
      most = person[p]
      review_person = p

    #  
    if(person_help[p] > most_useful):
      most_useful = person_help[p]
      useful_person = p
  
  # freeing memory from these unwanted stuff
  person.clear()
  sums.clear()
  rate.clear()
  average = pd.DataFrame.from_dict(average, orient='index')
# find mode of review ratings
  hi =0
  loc =0
  for i in range(1,6):
    if mode[i] > hi:
      hi = mode[i]
      loc = i
  kmeans = pd.DataFrame.from_dict(kmeans, orient='index')
  kmeans.to_csv("plot.csv", sep=',', encoding='utf-8')

  return pd.DataFrame.from_dict(df, orient='index'),most_reviewed,highest,review_person,useful_person,average,loc


if __name__ == "__main__":
  print (sys.argv[0])

  start_time = time.clock()
  print ("Processing Meta data")
  frame,prices = getmetadata('meta_Musical_Instruments.json.gz')
  print ("Processing Amazon data")
  df,bestitem,count,review_person,usefulperson,average,modereview = getDF('reviews_Musical_Instruments_5.json.gz',frame)

  df.to_csv("products.csv", sep=',', encoding='utf-8')
  # Mean of the mean values
  mean = df['Average'].mean()
  writetofile("Average of all ratings = "+  str(mean))

  # Mode of the review rating
  writetofile("Mode review of the ratings = "+  str(modereview))

  if mean > modereview :
    writetofile("The distribution is positively Skewed")
  else:
    if mean < modereview :
      writetofile("The distribution is Negatively Skewed")
    else:
      writetofile("The distribution is Symmetrical")

  mins = ( (time.clock() - start_time)/60, "minutes")

  findMedianReview(df)
  

  # list of products with average rating > 4  
  avg_filtered = df[df[['Product','Average']]['Average'] >= 4]

  avg_filtered = avg_filtered['Product'].tolist()
  writetofile (str(df.shape[0])+ " Records")
  # 1 c i
  writetofile(str(bestitem)+" Was reviewed the most with count "+str(count))
  # 1 c ii
  writetofile("The person who review the most items was " + review_person)
  # 1 c iii
  writetofile("The person who gave the most useful reviews was " + usefulperson)




  neg = -1
  big_stop = False
  small_stop = False
  big = ""
  small =""
  for x in range (0,frame.shape[0]):
# 1 c iv    
    IDbig  = frame.iloc[x]['ID'] 
    if big_stop == False and IDbig in avg_filtered:
      big_stop =True
      big = IDbig
# 1 c v
    IDsmall  = frame.iloc[neg]['ID'] 
    if small_stop == False and IDsmall in avg_filtered:
      small_stop = True
      small = IDsmall

    if small_stop and big_stop:
      break
    neg = neg -1

  with open("Results.txt", "a") as myfile:
    myfile.write("\nMost expensive item with high review = " + big + "\n")
    myfile.write("Cheapest item with high review = " + small +"\n")


  print (mins)
# send email when finish processing
  # send("kerschels@hotmail.com")

