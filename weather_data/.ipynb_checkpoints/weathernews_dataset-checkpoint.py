import pandas as pd
import requests
from bs4 import BeautifulSoup 
import numpy as np
import os

def get_WN_Forecast(latitude,longitude):

    base_url = "https://weathernews.jp/onebox/%s/%s/"

    r = requests.get(base_url%(latitude,longitude))
    r.encoding = r.apparent_encoding

    soup = BeautifulSoup(r.text,features="lxml")
    main = soup.findAll('div',class_='weather-day')

    df = []
    for i in main:
        s = BeautifulSoup(str(i),features="lxml")

        #date #####################
        date = s.findAll('div',class_='weather-day__day')[0]
        date = ( BeautifulSoup(str(date),features="lxml").findAll('p') )[0]
        date = str(date)
        date = date.replace("<p>","")
        date = date.replace("</p>","")
        date = date.split(", ")[-1]
        month,day = date.split("  ")


        #header #####################
        head = s.findAll('div',class_='weather-day__head')[0]
        head = ( BeautifulSoup(str(head),features="lxml").findAll('p') )
        head = [str(h).replace("<p>","").replace("</p>","") for h in head]
        

        #body #####################
        body = s.findAll('div',class_='weather-day__body')[0]
        body = ( BeautifulSoup(str(body),features="lxml").findAll("div",'weather-day__item') )
        body_table = []
        for row in body:
            tmp = row.findAll('p')
            tmp = [str(a) for a in tmp]

            r = []
            r.append( tmp[0].replace("<p class=\"weather-day__time\">","").replace("</p>","") )
            r.append( tmp[1].replace("<p class=\"weather-day__icon\">","").replace("</p>","") )
            r.append( tmp[2].replace("<p class=\"weather-day__r\">","").replace("</p>","").replace("mm/h","") )
            r.append( tmp[3].replace("<p class=\"weather-day__t\">","").replace("</p>","").replace("°F","") )
            r.append( tmp[4].replace("<p class=\"weather-day__w\">","").replace("</p>","") )

            body_table.append(r)

        tmp = pd.DataFrame(columns=head,data=body_table)
        tmp["Month"] = month
        tmp["Day"] = day

        df.append(tmp)

    df = pd.concat(df)
    df = df[["Month","Day","Hour","Precip.","Temp."]]
    df = df.rename(columns={"Precip.":"降水量（mm/h）","Temp.":"気温（°F）"})
    
    return df


if __name__ == "__main__":
    
    import datetime as dt
    DIR = "WN_forecast"
    
    # 東京の予報
    df = get_WN_Forecast(35.6895014, 139.6917337)
    
    now = dt.datetime.now()
    filename = "%04d-%02d-%02d_%02d"%(now.year,now.month,now.day,now.hour)+".csv"
    filename = os.path.join(DIR,filename)
    
    df.to_csv(filename,index=False)    