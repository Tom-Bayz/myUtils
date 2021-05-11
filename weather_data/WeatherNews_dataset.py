import pandas as pd
import requests
from bs4 import BeautifulSoup 
import numpy as np
import os
import datetime as dt
from JapanMeteorologicalAgency_dataset import get_JMA_placelist


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
        head = [str(h.text) for h in head]
        print(head)


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
    df = df[["Month","Day","Hour","Precip.","Temp.","Wind"]]
    df = df.rename(columns={"Precip.":"降水量（mm/h）","Temp.":"気温（F）","Wind":"風"})
    
    return df

def get_WN_forecast_4_Allplace(place_list,out_dir):
    
    for idx,place in place_list.iterrows():
        
        dir_name = place["pref_name"]+"_"+place["block_name"]
        dir_name = os.path.join(OUT_DIR,dir_name)

        if not(os.path.exists(dir_name)):
            os.makedirs(dir_name)
        
        
        latitude = place["latitude"]
        longitude = place["longitude"]
        now = dt.datetime.now()
        
        print(">> ",place["pref_name"],"-",place["block_name"]," date:",now)
        try:
            df = get_WN_Forecast(latitude, longitude)
            
            filename = "%04d-%02d-%02d_%02d.csv"%(now.year,now.month,now.day,now.hour)
            df.to_csv(os.path.join(dir_name,filename),
                      index=False,
                      encoding="shift-jis")
        except:
            df = None
            pass
        
        print(df)
        


if __name__ == "__main__":
    
    # 場所のリスト
    print("getting place list...")
    place_list = get_JMA_placelist()
    out_dir = "D:\weather_data\WN_forecast"
    
    get_WN_forecast_4_Allplace(place_list,out_dir)
