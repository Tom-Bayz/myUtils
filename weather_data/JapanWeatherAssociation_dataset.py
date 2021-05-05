import pandas as pd
import requests
from bs4 import BeautifulSoup 
import numpy as np
import os
import datetime as dt
from xml.sax.saxutils import unescape
import re
from tqdm import tqdm

def get_JWA_placelist():
    
    base_url = "https://tenki.jp/"
    r = requests.get(base_url)
    r.encoding = r.apparent_encoding
    soup = BeautifulSoup(r.text,features="lxml")
    top = soup.findAll("area",shape="poly")

    place_list = []
    for i in tqdm(top):
        
        _,area_name,_,pref_link = str(i).split(" ")[:4]
        area_name = ( area_name.split("=")[1] ).replace("\"","")
        pref_link = ( pref_link.split("=")[1] ).replace("\"","")


        # 県ループ
        pref_url = base_url+pref_link
        r = requests.get(pref_url)
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text,features="lxml")
        pref = soup.findAll("area",shape="rect",onfocus="this.blur();")

        if len(pref) == 0:
            pref = soup.findAll("area",shape="poly",onfocus="this.blur();")


        for j in pref:

            _,pref_name,_,city_link = str(j).split(" ")[:4]
            pref_name = ( pref_name.split("=")[1] ).replace("\"","")
            city_link = ( city_link.split("=")[1] ).replace("\"","")


            # 市ループ
            city_url = base_url+city_link
            r = requests.get(city_url)
            r.encoding = r.apparent_encoding
            soup = BeautifulSoup(r.text,features="lxml")
            city = soup.findAll("a",class_="forecast-map-entry")

            for k in city:

                city_name = re.sub("[0-9]","",k.text)
                city_name = city_name.replace("/","")
                city_name = city_name.replace("%","")

                link = str(k).split(" ")[2]
                link = ( link.split("=")[1] ).replace("\"","")

                #print(area_name,pref_name,city_name,link)

                place_list.append([area_name,pref_name,city_name,link])

    place_list = pd.DataFrame(data=place_list,
                              columns=["地方","県","市","url"])   

    place_list["link_len"] = [len(s.split("/")) for s in place_list["url"]]
    place_list = place_list[place_list["link_len"] == 7]

    place_list["url"] = [base_url+s+"1hour.html" for s in place_list["url"].values]
    place_list = place_list[["地方","県","市","url"]]
    
    return place_list



def get_JWA_Forecast(url):

    r = requests.get(url)
    r.encoding = r.apparent_encoding
    soup = BeautifulSoup(r.text,features="lxml")
    tables = soup.findAll("table",class_="forecast-point-1h")

    for table in tables:
        table = str(table)

        soup = BeautifulSoup(table,features="lxml")

        # 日時
        head = soup.findAll("tr",class_="head")
        date = head[0].text.split("\n")[4]
        date = re.findall(r"\d+",date)
        date = "-".join(date)

        # 時刻
        hour = soup.findAll("tr",class_="hour")[0]
        hour = hour.text.split("\n")
        hour = [int(s) for s in hour[1:25]]
        #print(hour)

        # 天気
        weather = soup.findAll("tr",class_="weather")[0]
        weather = weather.text.split("\n")
        weather = weather[1:25]
        #print(weather)

        # 気温
        temp = soup.findAll("tr",class_="temperature")[0]
        temp = temp.text.split("\n")
        temp = [float(s) for s in temp[1:25]]
        #print(temp)

        # 降水確率
        prob = soup.findAll("tr",class_="prob-precip")[0]
        prob = prob.text.split("\n")
        prob = prob[2:26]
        #print(prob)

        # 降水量
        prec = soup.findAll("tr",class_="precipitation")[0]
        prec = prec.text.split("\n")
        prec = [float(s) for s in prec[1:25]]
        #print(prec)

        # 湿度
        hum = soup.findAll("tr",class_="humidity")[0]
        hum = hum.text.split("\n")
        hum = [float(s) for s in hum[2:26]]
        #print(hum)

        # 風向
        blow = soup.findAll("tr",class_="wind-blow")[0]
        blow = blow.text.split("\n")
        blow = blow[2:-1]
        blow = [s for i,s in enumerate(blow) if i%3 == 1]
        #print(blow)

        # 風速
        speed = soup.findAll("tr",class_="wind-speed")[0]
        speed = speed.text.split("\n")
        speed = [float(s) for s in speed[1:25]]
        #print(speed)

        data = np.array([[date]*24,hour,weather,temp,prob,prec,hum,blow,speed]).T
        col = ["date",
               "time",
               "天気",
               "気温（℃）",
               "降水確率（%）",
               "降水量（mm/h）",
               "湿度（%）",
               "風向",
               "風速（m/s）"]
        df = pd.DataFrame(data=data,
                          columns=col)

        return df
    
    
def get_JWA_forecast_4_Allplace(place_list):
    
    for idx,place in place_list.iterrows():
        
        OUT_DIR = "D:\weather_data\JWA_forecast"
        dir_name = place["地方"]+"_"+place["市"]
        dir_name = os.path.join(OUT_DIR,dir_name)

        if not(os.path.exists(dir_name)):
            os.makedirs(dir_name)
            
        now = dt.datetime.now()
        print(">> ",place["地方"],"-",place["市"]," date:",now)
        #try:
        df = get_JWA_Forecast(place["url"])

        filename = "%04d-%02d-%02d_%02d.csv"%(now.year,now.month,now.day,now.hour)
        df.to_csv(os.path.join(dir_name,filename),
                  index=False,
                  encoding="shift-jis")
        #except:
        #    df = None
        #    pass
        
        print(df)

if __name__ == "__main__":
    
    # 場所のリスト
    print("getting place list...")
    place_list = get_JWA_placelist()
    
    get_JWA_forecast_4_Allplace(place_list)  
    