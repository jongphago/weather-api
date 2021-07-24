import datetime as dt
import os
import pandas as pd
import numpy as np

# API
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import xml.etree.ElementTree as ET

# SUN
from astral import LocationInfo
import datetime
from astral.sun import sun

# GPS distance
# from math import sin, cos, sqrt, atan2, radians
import geopy.distance


def add_hours(_time:dt.datetime, hours:int):
    _time += dt.timedelta(hours=hours)
    return _time

def change_args(dictionary:dict, args:str, value)->dict:
    dictionary[args] = value
    if args=='_startDt' or args=='_endDt':
        _startDt = dictionary['_startDt']
        _endDt = dictionary['_endDt']
        _numOfRows = num_of_rows(_startDt, _endDt)
        dictionary['_numOfRows'] = _numOfRows
    return dictionary

def num_of_rows(_s:str, _e:str)->int:
    s = dt.datetime.strptime(_s, '%Y%m%d%H%M')
    e = dt.datetime.strptime(_e, '%Y%m%d%H%M')
    # e += dt.timedelta(hours=23)
    _numOfRows = int((e-s).total_seconds() / 3600 + 1)
    return _numOfRows

def make_arg_dict(_stnIds:int, _startDt:str, _endDt:str, _dataCd='ASOS', 
                    _dateCd='HR', _startHh:str='00', _endHh:str='23')->dict:
    # day + hour
    s = _startDt + _startHh + '00'
    e = _endDt + _endHh + '00'
    # _numOfRows
    _numOfRows = num_of_rows(s, e)

    # arg_dict
    arg_dict = \
    {
    '_pageNo':'1', 
    '_numOfRows':_numOfRows,
    '_dataType':'XML', 
    '_dataCd':_dataCd, 
    '_dateCd':_dateCd, 
    '_startDt':_startDt, 
    '_startHh':_startHh, 
    '_endDt':_endDt, 
    '_endHh':_endHh, 
    '_stnIds':str(_stnIds)
    }
    return arg_dict

def get_service_key(path:str):
    os.chdir(path)
    import service_key
    return service_key.ServiceKey

def weather_api(service_key:str, arg_dict:dict):
    url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
    ServiceKey = service_key
    queryParams = '?' + urlencode({ 
                                   quote_plus('ServiceKey') : ServiceKey, 
                                   quote_plus('pageNo') : arg_dict['_pageNo'], 
                                   quote_plus('numOfRows') : str(arg_dict['_numOfRows']), 
                                   quote_plus('dataType') : arg_dict['_dataType'], 
                                   quote_plus('dataCd') : arg_dict['_dataCd'], 
                                   quote_plus('dateCd') : arg_dict['_dateCd'], 
                                   quote_plus('startDt') : arg_dict['_startDt'], 
                                   quote_plus('startHh') : arg_dict['_startHh'], 
                                   quote_plus('endDt') : arg_dict['_endDt'], 
                                   quote_plus('endHh') : arg_dict['_endHh'], 
                                   quote_plus('stnIds') : arg_dict['_stnIds'] 
                                  })
    request = Request(url + queryParams)
    request.get_method = lambda: 'GET'
    response_body = urlopen(request).read()
    return response_body

def start_time(arg_dict:dict)->str:
    st = arg_dict['_startDt'] + arg_dict['_startHh'] + '00'
    return st

def end_time(arg_dict:dict)->str:
    et = arg_dict['_endDt'] + arg_dict['_endHh'] + '00'
    return et

def num_of_rows(_s:str, _e:str)->int:
    s = dt.datetime.strptime(_s, '%Y%m%d%H%M')
    e = dt.datetime.strptime(_e, '%Y%m%d%H%M')
    _numOfRows = int((e-s).total_seconds() / 3600 + 1)
    return _numOfRows

def update_nor(arg_dict:dict):
    _s = start_time(arg_dict)
    _e = end_time(arg_dict)
    _numOfRows = num_of_rows(_s, _e)
    arg_dict['_numOfRows'] = _numOfRows

def change_end(arg_dict:dict, et:str)->dict:
    arg_dict['_endDt'] = et[:8]
    arg_dict['_endHh'] = et[8:10]
    update_nor(arg_dict)

def change_start(arg_dict:dict, st:str)->dict:
    arg_dict['_startDt'] = st[:8]
    arg_dict['_startHh'] = st[8:10]
    update_nor(arg_dict)

item_tag = \
[ 
  'tm', 'rnum', 'stnId', 'stnNm', 'ta', 'taQcflg', 'rn', 'rnQcflg', 'ws', 'wsQcflg', 'wd', 
  'wdQcflg', 'hm', 'hmQcflg', 'pv', 'td', 'pa', 'paQcflg', 'ps', 'psQcflg', 'ss', 'ssQcflg', 
  'icsr', 'dsnw', 'hr3Fhsc', 'dc10Tca', 'dc10LmcsCa', 'clfmAbbrCd', 'lcsCh', 'vs', 'gndSttCd', 
  'dmstMtphNo', 'ts', 'tsQcflg', 'm005Te', 'm01Te', 'm02Te', 'm03Te'
]

def checker(xtree):
    if int(xtree[0][0].text):
        raise ValueError
        
def parse(xtree:ET.Element)->list:
    global item_tag
    item_list = []
    items = xtree.find('body').find('items')
    for item in items:
        tag_list = []
        for index, tag in enumerate(item_tag):
            value = item[index].text
            tag_list.append(value)
        item_list.append(tag_list)
    return item_list

def strptime(string, _format='%Y-%m-%d %H:%M %z'):
    time = dt.datetime.strptime(string, _format)
    return time

def call(service_key:str, arg_dict:dict, is_datetime:bool=False)->pd.DataFrame:
    global item_tag
    master_df = pd.DataFrame(columns=item_tag)

    while arg_dict['_numOfRows'] > 999:
        # Original arg_dict
        s = start_time(arg_dict)
        e = end_time(arg_dict)

        # Temporary arg_dict
        dts = dt.datetime.strptime(s, '%Y%m%d%H%M') # datetime_startDt
        dte = add_hours(dts, 999-1)              # datetime_endDt
        te = dt.datetime.strftime(dte, '%Y%m%d%H%M')# temp_endDt:str
        change_end(arg_dict, te)

        # API 호출
        sub_response_body = weather_api(service_key, arg_dict)

        # Parse XML
        xtree = ET.fromstring(sub_response_body)
        checker(xtree)
        item_list = parse(xtree)

        # Concatenate dataframe
        sub_df = pd.DataFrame(item_list, columns=item_tag)
        master_df = pd.concat([master_df, sub_df], ignore_index=True)

        # Update arg_dict
        dtns = add_hours(dts, 1000-1)             # datetime_new_startDt
        ns = dt.datetime.strftime(dtns, '%Y%m%d%H%M')# new_startDt:str
        change_start(arg_dict, ns)
        change_end(arg_dict, e)

    # API 호출
    response_body = weather_api(service_key, arg_dict)

    # Parse XML
    xtree = ET.fromstring(response_body)
    checker(xtree)
    item_list = parse(xtree)

    # Concatenate dataframe
    sub_df = pd.DataFrame(item_list, columns=item_tag)
    master_df = pd.concat([master_df, sub_df], ignore_index=True)

    # 'tm' 컬럼을 datetime 객체로 형변환
    if is_datetime:
        master_df['tm'] = master_df['tm'].apply(strptime)

    return master_df
    
#################################################
# weather_dict library에서 넘어온 함수들 입니다.# 
#################################################

def get_label(dictionary, key:str)->int:
    return dictionary[key]['label']

def get_index(dictionary, index:int)->str:
    return dictionary[index]

def split_value(value:str)->list:
    if value == None:
        return None
    value_list = []
    while len(value) >= 2:
        sub_value = value[-2:]
        value_list.append(sub_value)
        value = value[:-2]
    return value_list
    
def get_one_hot(dictionary:dict, value_list:list, count:int)->list:
    one_hot = np.zeros(count, dtype=int)
    while len(value_list):
        value = value_list.pop()
        if dictionary != None:
            label = get_label(dictionary, value)
        else:
            label = int(value)-1
        one_hot[label] = 1
    return one_hot

def to_decimal(index_list:list)->int:
    binary_string = ''.join([str(i) for i in index_list])
    return int(binary_string, 2)

def converter(value:str, dictionary:dict, count:int, is_decimal:bool):
    if value == None:
        return np.zeros(count)
    value_list = split_value(value)
    label_list = get_one_hot(dictionary, value_list, count)
    if is_decimal:
        return to_decimal(label_list)
    return label_list

def translate(dictionary:dict, column_name:str)->str:
    """
    translate(asos_dict, 'ts')
    >>> '지면온도'
    """
    return  dictionary[column_name]['항목명']


def get_city(dictionary:dict, site_index:int):
    city = LocationInfo(region="Korea", 
                        timezone="Asia/Seoul", 
                        name=dictionary[site_index]['stnNm'], 
                        latitude=dictionary[site_index]['위도'], 
                        longitude=dictionary[site_index]['경도'])
    return city

def get_suninfo(year:int, month:int, day:int, city):
    s = sun(city.observer, date=datetime.date(year, month, day), tzinfo=city.timezone)
    return s["sunrise"], s["sunset"]
    
def sun_converter(values, _city):
    _timestamp, sun_value = values
    timestamp = strptime(_timestamp + ' +0900')
    _year = timestamp.year
    _month = timestamp.month
    _day = timestamp.day

    sunrise, sunset = get_suninfo(_year, _month, _day, _city)

    if timestamp <= sunrise:
        return 0.
    elif timestamp >= sunset:
        return 0.
    else:
        return sun_value

def lalo(station_dict:dict, index:int):
    station = station_dict[index]
    latitude = station['위도']
    longitude = station['경도']
    return latitude,longitude

def geo_distance(dictionary, index1, index2):
    coords_1 = lalo(dictionary, index1)
    coords_2 = lalo(dictionary, index2)
    return geopy.distance.vincenty(coords_1, coords_2)

def zero_null(values):
    if all(values.isna()):
        return 0.
    else:
        rn_value = values[0]
        return rn_value

def allna(values):
    if all(values.isna()):
        return True
    return False

def sort_fcst(_df:pd.DataFrame, create_datetime=True)->pd.DataFrame:
    df = _df.sort_values(by=['fcstDate', 'fcstTime', 'category', 'fcstValue'], 
                             ignore_index=True)
    if create_datetime:
        df['fcstDateTime'] = df['fcstDate'] + df['fcstTime']
    return df

def reshape_sdf(sorted_df:pd.DataFrame)->pd.DataFrame:
    indice = sorted_df['fcstDateTime'].unique()
    categories = sorted_df['category'].unique()
    cat_len = len(categories)
    sorted_np = sorted_df[['fcstValue']].to_numpy()
    sorted_np = sorted_np.reshape(-1, cat_len)
    df = pd.DataFrame(sorted_np, columns=categories, index=indice)
    return df