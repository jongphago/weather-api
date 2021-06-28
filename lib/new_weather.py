import datetime as dt
import os

# API
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import xml.etree.ElementTree as ET

def add_hours(_time:dt.datetime, hours:int):
    _time += dt.timedelta(hours=hours)
    return _time

def change_args(dictionary:dict, args:str, value):
    dictionary[args] = value
    if args=='_startDt' or args=='_endDt':
        _startDt = dictionary['_startDt']
        _endDt = dictionary['_endDt']
        _numOfRows = num_of_rows(_startDt, _endDt)
        dictionary['_numOfRows'] = _numOfRows
    return dictionary

def num_of_rows(_startDt, _endDt):
    s = dt.datetime.strptime(_startDt, '%Y%m%d')
    e = dt.datetime.strptime(_endDt, '%Y%m%d')
    e += dt.timedelta(hours=23)
    _numOfRows = int((e-s).total_seconds() / 3600 + 1)
    return _numOfRows

def make_arg_dict(_stnIds:int, _startDt:str, _endDt:str, _dataCd='ASOS', _dateCd='HR'):
    # _numOfRows
    _numOfRows = num_of_rows(_startDt, _endDt)

    # arg_dict
    arg_dict = \
    {
    '_pageNo':'1', 
    '_numOfRows':_numOfRows,
    '_dataType':'XML', 
    '_dataCd':_dataCd, 
    '_dateCd':_dateCd, 
    '_startDt':str(_startDt), 
    '_startHh':'00', 
    '_endDt':str(_endDt), 
    '_endHh':'23', 
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