import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import os
from xml.sax.saxutils import unescape
from tqdm import tqdm


import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import os
from xml.sax.saxutils import unescape
from tqdm import tqdm


def get_JMA_placelist():
    base_url = "https://www.data.jma.go.jp/obd/stats/etrn/select/%s"

    top_url = base_url%("prefecture00")
    r = requests.get(top_url)
    r.encoding = r.apparent_encoding

    soup = BeautifulSoup(r.text,features="lxml")
    main = soup.findAll('area')

    place_list = []

    for i in tqdm(main):
        i = str(i)
        i = unescape(i)
        _,pref_name,_,pref_url,_ = i.split(" ")

        pref_name = pref_name.split("=")[1].replace("\"","")  

        pref_url = pref_url.split("\"")[1]
        pref_url = base_url%(pref_url)

        sub_r = requests.get(pref_url)
        sub_r.encoding = sub_r.apparent_encoding
        soup = BeautifulSoup(sub_r.text,features="lxml")
        area = soup.findAll("area")
        
        for j in area:
            j = str(j)
            j = unescape(j)
            
            try:
                area_data = j.split("javascript:viewPoint")[1]

                area_type,_,area_name,katakana,lat1,lat2,long1,long2,elev = area_data.split(",")[:9]
                area_type = area_type.replace("\'","").replace("(","")
                area_name = area_name.replace("\'","")
                katakana = katakana.replace("\'","")
                latitude = int(lat1.replace("\'",""))+float(lat2.replace("\'",""))/100
                longitude = int(long1.replace("\'",""))+float(long2.replace("\'",""))/100
                elev = float(elev.replace("\'",""))

                area_data = j.split("?")[1]
                pref_no,block_no = area_data.split("&")[:2]
                pref_no,block_no = pref_no.split("=")[1], block_no.split("=")[1]

                place_list.append([area_type,pref_name,area_name,katakana,latitude,longitude,elev,pref_no,block_no])

            except:
                pass

    place_list = pd.DataFrame(data = np.array(place_list),
                              columns = ["area_type","pref_name","block_name","katakana_name","latitude","longitude","elevation","pref_no","block_no"])
    place_list = place_list[place_list["block_no"] != ""]
    place_list = place_list.drop_duplicates()
    place_list = place_list.reset_index(drop=True)
    place_list[["longitude","latitude","elevation"]] = place_list[["longitude","latitude","elevation"]].astype("float")
    
    return place_list


###############################
# pref_no,block_no: get_placelistで得られる番号
# year,month,day: ほしい年月日を指定
# 引数に与えた年月日の10分ごとの気象ログをDataFrameで返す
###############################

def get_JMA_record(area_type,pref_no,block_no,year,month,day):
    
    if area_type == "s":
        base_url = "https://www.data.jma.go.jp/obd/stats/etrn/view/10min_s1.php?prec_no=%s&block_no=%s&year=%d&month=%d&day=%d&view="
        # table head
        col = ["time",
               "気圧-現地（hPa）",
               "気圧-海面（hPa）",
               "降水量（mm）",
               "気温（℃）",
               "相対湿度（％）",
               "風速-平均（m/s）",
               "風向-平均",
               "風速-最大瞬間（m/s）",
               "風向-最大瞬間",
               "日照時間（分）"]
    
    if area_type == "a":
        base_url = "https://www.data.jma.go.jp/obd/stats/etrn/view/10min_a1.php?prec_no=%s&block_no=%s&year=%d&month=%d&day=%d&view="
        # table head
        col = ["time",
               "降水量（mm）",
               "気温（℃）",
               "風速-平均（m/s）",
               "風向-平均",
               "風速-最大瞬間（m/s）",
               "風向-最大瞬間",
               "日照時間（分）"]
        
    url = unescape(base_url%(pref_no,block_no,year,month,day))
    r = requests.get(url)
    r.encoding = r.apparent_encoding
    soup = BeautifulSoup(r.text,features="lxml")

    table = soup.findAll("table",class_="data2_s")[0]
    table_soup = BeautifulSoup(str(table),features="lxml")        
        

    # table body
    body = []
    for row in table_soup.findAll("tr",class_="mtx",style="text-align:right;"):
        tr_soup = BeautifulSoup(str(row),features="lxml")
        row = [i.text for i in tr_soup.findAll("td")]
        body.append(row)

    df = pd.DataFrame(data=body,
                      columns=col)
    df["date"] = "%04d-%02d-%02d"%(year,month,day)
    
    return df[["date"]+col]


def get_JMA_record_4_Allplace(place_list):
    
    # place loop
    for idx, place in place_list.iterrows():
    
        date_list = pd.date_range(start="2020-01-01",
                                  end="2020-12-31",
                                  freq="1D")

        # dirがなければ作成
        OUT_DIR = "D:\weather_data\JMA_record"
        dir_name = place["pref_name"]+"_"+place["block_name"]
        dir_name = os.path.join(OUT_DIR,dir_name)

        if not(os.path.exists(dir_name)):
            os.makedirs(dir_name)


        # date loop
        for date in date_list:
            print(">> ",place["pref_name"],"-",place["block_name"]," date:",date)

            try:
                df = get_JMA_record(area_type=place["area_type"],
                                    pref_no=place["pref_no"],
                                    block_no=place["block_no"],
                                    year=date.year,
                                    month=date.month,
                                    day=date.day)

                filename = "%04d-%02d-%02d.csv"%(date.year,date.month,date.day)
                df.to_csv(os.path.join(dir_name,filename),
                          index=False,
                          encoding="shift-jis")
            except:
                df = None

            print(df)


if __name__ == "__main__":
    
    # 場所のリスト
    print("getting place list...")
    place_list = get_JMA_placelist()
    
    #place_list = place_list[place_list["block_name"]=="西米良"]
    
    get_JMA_record_4_Allplace(place_list)  
        
        
           
        
        