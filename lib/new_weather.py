import datetime as dt
import os
import pandas as pd

# API
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import xml.etree.ElementTree as ET

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

def make_arg_dict(_stnIds:int, _startDt:str, _endDt:str, _dataCd='ASOS', _dateCd='HR',
                    _startHh:str='00', _endHh:str='23')->dict:
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

def call(service_key:str, arg_dict:dict)->pd.DataFrame:
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
    return master_df