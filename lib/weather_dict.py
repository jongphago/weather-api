import numpy as np

call_dict = \
    {
        'serviceKey':{'항목명':'인증키', '항목크기':100, '항목구분':1, '샘플데이터':'인증키', '항목설명':'공공데이터포털에서 발급받은 인증키(URL_Encode)'},
        'numOfRows':{'항목명':'한_페이지_결과_수', '항목크기':4, '항목구분':0, '샘플데이터':'10', '항목설명':'한 페이지 결과 수(Default: 10)'},
        'pageNo':{'항목명':'페이지_번호', '항목크기':4, '항목구분':0, '샘플데이터':'1', '항목설명':'페이지 번호(Default: 1)'},
        'dataType':{'항목명':'응답자료형식', '항목크기':4, '항목구분':0, '샘플데이터':'XML', '항목설명':'요청자료형식(XML/JSON)(Default: XML)'},
        'dataCd':{'항목명':'자료_코드', '항목크기':4, '항목구분':1, '샘플데이터':'ASOS', '항목설명':'자료 분류 코드'},
        'dateCd':{'항목명':'날짜_코드', '항목크기':3, '항목구분':1, '샘플데이터':'HR', '항목설명':'날짜 분류 코드'},
        'startDt':{'항목명':'시작일', '항목크기':8, '항목구분':1, '샘플데이터':'20100101', '항목설명':'조회 기간 시작일'},
        'startHh':{'항목명':'시작시', '항목크기':2, '항목구분':1, '샘플데이터':'1', '항목설명':'조회 기간 시작시'},
        'endDt':{'항목명':'종료일', '항목크기':8, '항목구분':1, '샘플데이터':'20100601', '항목설명':'조회 기간 종료일((전일(D-1) 까지 제공))'},
        'endHh':{'항목명':'종료시', '항목크기':2, '항목구분':1, '샘플데이터':'1', '항목설명':'조회 기간 종료시'},
        'stnIds':{'항목명':'지점_번호', '항목크기':3, '항목구분':1, '샘플데이터':'108', '항목설명':'종관기상관측 지점 번호'}
    }
    
error_dict = \
    {
        0:{'msg':'NORMAL_SERVICE', 'discription':'정상'},
        1:{'msg':'APPLICATION_ERROR', 'discription':'어플리케이션 에러'},
        2:{'msg':'DB_ERROR', 'discription':'데이터베이스 에러'},
        3:{'msg':'NODATA_ERROR', 'discription':'데이터없음 에러'},
        4:{'msg':'HTTP_ERROR', 'discription':'HTTP 에러'},
        5:{'msg':'SERVICETIME_OUT', 'discription':'서비스 연결실패 에러'},
        10:{'msg':'INVALID_REQUEST_PARAMETER_ERROR', 'discription':'잘못된 요청 파라메터 에러'},
        11:{'msg':'NO_MANDATORY_REQUEST_PARAMETERS_ERROR', 'discription':'필수요청 파라메터가 없음'},
        12:{'msg':'NO_OPENAPI_SERVICE_ERROR', 'discription':'해당 오픈API서비스가 없거나 폐기됨'},
        20:{'msg':'SERVICE_ACCESS_DENIED_ERROR', 'discription':'서비스 접근거부'},
        21:{'msg':'TEMPORARILY_DISABLE_THE_SERVICEKEY_ERROR', 'discription':'일시적으로 사용할 수 없는 서비스 키'},
        22:{'msg':'LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR', 'discription':'서비스 요청제한횟수 초과에러'},
        30:{'msg':'SERVICE_KEY_IS_NOT_REGISTERED_ERROR', 'discription':'등록되지 않은 서비스키'},
        31:{'msg':'DEADLINE_HAS_EXPIRED_ERROR', 'discription':'기한만료된 서비스키'},
        32:{'msg':'UNREGISTERED_IP_ERROR', 'discription':'등록되지 않은 IP'},
        33:{'msg':'UNSIGNED_CALL_ERROR', 'discription':'서명되지 않은 호출'},
        99:{'msg':'UNKNOWN_ERROR', 'discription':'기타에러'}
    }

station_dict = \
    {
        90:{'stnNm':'속초', 'manager':'강원지방기상청'},
        93:{'stnNm':'북춘천', 'manager':'춘천기상대'},
        95:{'stnNm':'철원', 'manager':'강원지방기상청'},
        98:{'stnNm':'동두천', 'manager':'수도권기상청'},
        99:{'stnNm':'파주', 'manager':'수도권기상청'},
        100:{'stnNm':'대관령', 'manager':'강원지방기상청'},
        101:{'stnNm':'춘천', 'manager':'춘천기상대'},
        102:{'stnNm':'백령도', 'manager':'수도권기상청'},
        104:{'stnNm':'북강릉', 'manager':'강원지방기상청'},
        105:{'stnNm':'강릉', 'manager':'강원지방기상청'},
        106:{'stnNm':'동해', 'manager':'강원지방기상청'},
        108:{'stnNm':'서울', 'manager':'수도권기상청'},
        112:{'stnNm':'인천', 'manager':'수도권기상청'},
        114:{'stnNm':'원주', 'manager':'강원지방기상청'},
        115:{'stnNm':'울릉도', 'manager':'대구지방기상청'},
        119:{'stnNm':'수원', 'manager':'수도권기상청'},
        121:{'stnNm':'영월', 'manager':'강원지방기상청'},
        127:{'stnNm':'충주', 'manager':'청주기상지청'},
        129:{'stnNm':'서산', 'manager':'홍성기상대'},
        130:{'stnNm':'울진', 'manager':'안동기상대'},
        131:{'stnNm':'청주', 'manager':'청주기상지청'},
        133:{'stnNm':'대전', 'manager':'대전지방기상청'},
        135:{'stnNm':'추풍령', 'manager':'청주기상지청'},
        136:{'stnNm':'안동', 'manager':'안동기상대'},
        137:{'stnNm':'상주', 'manager':'대구지방기상청'},
        138:{'stnNm':'포항', 'manager':'대구지방기상청'},
        140:{'stnNm':'군산', 'manager':'전주기상지청'},
        143:{'stnNm':'대구', 'manager':'대구지방기상청'},
        146:{'stnNm':'전주', 'manager':'전주기상지청'},
        152:{'stnNm':'울산', 'manager':'울산기상대'},
        155:{'stnNm':'창원', 'manager':'창원기상대'},
        156:{'stnNm':'광주', 'manager':'광주지방기상청'},
        159:{'stnNm':'부산', 'manager':'부산지방기상청'},
        162:{'stnNm':'통영', 'manager':'부산지방기상청'},
        165:{'stnNm':'목포', 'manager':'목포기상대'},
        168:{'stnNm':'여수', 'manager':'광주지방기상청'},
        169:{'stnNm':'흑산도', 'manager':'광주지방기상청'},
        170:{'stnNm':'완도', 'manager':'목포기상대'},
        172:{'stnNm':'고창', 'manager':'전주기상지청'},
        174:{'stnNm':'순천', 'manager':'광주지방기상청'},
        177:{'stnNm':'홍성', 'manager':'홍성기상대'},
        184:{'stnNm':'제주', 'manager':'제주지방기상청'},
        185:{'stnNm':'고산', 'manager':'제주지방기상청'},
        188:{'stnNm':'성산', 'manager':'제주지방기상청'},
        189:{'stnNm':'서귀포', 'manager':'제주지방기상청'},
        192:{'stnNm':'진주', 'manager':'창원기상대'},
        201:{'stnNm':'강화', 'manager':'인천기상대'},
        202:{'stnNm':'양평', 'manager':'수도권기상청'},
        203:{'stnNm':'이천', 'manager':'수도권기상청'},
        211:{'stnNm':'인제', 'manager':'강원지방기상청'},
        212:{'stnNm':'홍천', 'manager':'춘천기상대'},
        216:{'stnNm':'태백', 'manager':'강원지방기상청'},
        217:{'stnNm':'정선군', 'manager':'강원지방기상청'},
        221:{'stnNm':'제천', 'manager':'청주기상지청'},
        226:{'stnNm':'보은', 'manager':'청주기상지청'},
        232:{'stnNm':'천안', 'manager':'대전지방기상청'},
        235:{'stnNm':'보령', 'manager':'대전지방기상청'},
        236:{'stnNm':'부여', 'manager':'대전지방기상청'},
        238:{'stnNm':'금산', 'manager':'대전지방기상청'},
        239:{'stnNm':'세종', 'manager':'대전지방기상청'},
        243:{'stnNm':'부안', 'manager':'전주기상지청'},
        244:{'stnNm':'임실', 'manager':'전주기상지청'},
        245:{'stnNm':'정읍', 'manager':'전주기상지청'},
        247:{'stnNm':'남원', 'manager':'전주기상지청'},
        248:{'stnNm':'장수', 'manager':'전주기상지청'},
        251:{'stnNm':'고창군', 'manager':'전주기상지청'},
        252:{'stnNm':'영광군', 'manager':'광주지방기상청'},
        253:{'stnNm':'김해시', 'manager':'부산지방기상청'},
        254:{'stnNm':'순창군', 'manager':'전주기상지청'},
        255:{'stnNm':'북창원', 'manager':'창원기상대'},
        257:{'stnNm':'양산시', 'manager':'울산기상대'},
        258:{'stnNm':'보성군', 'manager':'광주지방기상청'},
        259:{'stnNm':'강진군', 'manager':'목포기상대'},
        260:{'stnNm':'장흥', 'manager':'목포기상대'},
        261:{'stnNm':'해남', 'manager':'목포기상대'},
        262:{'stnNm':'고흥', 'manager':'광주지방기상청'},
        263:{'stnNm':'의령군', 'manager':'창원기상대'},
        264:{'stnNm':'함양군', 'manager':'창원기상대'},
        266:{'stnNm':'광양시', 'manager':'광주지방기상청'},
        268:{'stnNm':'진도군', 'manager':'목포기상대'},
        271:{'stnNm':'봉화', 'manager':'대구지방기상청'},
        272:{'stnNm':'영주', 'manager':'안동기상대'},
        273:{'stnNm':'문경', 'manager':'안동기상대'},
        276:{'stnNm':'청송군', 'manager':'대구지방기상청'},
        277:{'stnNm':'영덕', 'manager':'대구지방기상청'},
        278:{'stnNm':'의성', 'manager':'대구지방기상청'},
        279:{'stnNm':'구미', 'manager':'대구지방기상청'},
        281:{'stnNm':'영천', 'manager':'대구지방기상청'},
        283:{'stnNm':'경주시', 'manager':'대구지방기상청'},
        284:{'stnNm':'거창', 'manager':'울산기상대'},
        285:{'stnNm':'합천', 'manager':'울산기상대'},
        288:{'stnNm':'밀양', 'manager':'울산기상대'},
        289:{'stnNm':'산청', 'manager':'창원기상대'},
        294:{'stnNm':'거제', 'manager':'부산지방기상청'},
        295:{'stnNm':'남해', 'manager':'부산지방기상청'}
    }

asos_dict = \
    {
        'numOfRows':{'항목명':'한 페이지 결과 수', '항목크기':4, '샘플데이터':'1', '설명':'한 페이지당 표출 데이터 수'},
        'pageNo':{'항목명':'페이지 번호', '항목크기':4, '샘플데이터':'1', '설명':'페이지 수'},
        'totalCount':{'항목명':'데이터 총 개수', '항목크기':10, '샘플데이터':'1', '설명':'데이터 총 개수'},
        'resultCode':{'항목명':'응답메시지 코드', '항목크기':2, '샘플데이터':'0', '설명':'응답 메시지코드'},
        'resultMsg':{'항목명':'응답메시지 내용', '항목크기':100, '샘플데이터':'NORMAL SERVICE', '설명':'응답 메시지 설명'},
        'dataType':{'항목명':'데이터 타입', '항목크기':4, '샘플데이터':'XML', '설명':'응답자료형식 (XML/JSON)'},
        'tm':{'항목명':'시간', '항목크기':10, '샘플데이터':'2010-01-01 10', '설명':'일시'},
        'rnum':{'항목명':'목록 순서', '항목크기':5, '샘플데이터':'1', '설명':'목록 순서'},
        'stnId':{'항목명':'지점 번호', '항목크기':3, '샘플데이터':'108', '설명':'종관기상관측 지점 번호'},
        'stnNm':{'항목명':'서울', '항목크기':6, '샘플데이터':'서울', '설명':'종관기상관측 지점명'},
        'ta':{'항목명':'기온', '항목크기':6, '샘플데이터':'23.8', '설명':'기온(°C)'},
        'taQcflg':{'항목명':'기온 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'rn':{'항목명':'강수량', '항목크기':6, '샘플데이터':'10.5', '설명':'강수량(mm)'},
        'rnQcflg':{'항목명':'강수량 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'ws':{'항목명':'풍속', '항목크기':6, '샘플데이터':'1', '설명':'풍속(m/s)'},
        'wsQcflg':{'항목명':'풍속 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'wd':{'항목명':'풍향', '항목크기':3, '샘플데이터':'110', '설명':'풍향(16방위)'},
        'wdQcflg':{'항목명':'풍향 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'hm':{'항목명':'습도', '항목크기':6, '샘플데이터':'36', '설명':'습도(%)'},
        'hmQcflg':{'항목명':'습도 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'pv':{'항목명':'증기압', '항목크기':6, '샘플데이터':'1.1', '설명':'증기압(hPa)'},
        'td':{'항목명':'이슬점온도', '항목크기':6, '샘플데이터':'-21.4', '설명':'이슬점온도(°C)'},
        'pa':{'항목명':'현지기압', '항목크기':6, '샘플데이터':'1012.4', '설명':'현지기압(hPa)'},
        'paQcflg':{'항목명':'현지기압 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'ps':{'항목명':'해면기압', '항목크기':6, '샘플데이터':'1023.6', '설명':'해면기압(hPa)'},
        'psQcflg':{'항목명':'해면기압 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'ss':{'항목명':'일조', '항목크기':6, '샘플데이터':'1', '설명':'일조(hr)'},
        'ssQcflg':{'항목명':'일조 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'icsr':{'항목명':'일사', '항목크기':6, '샘플데이터':'0.73', '설명':'일사(MJ/m2)'},
        'dsnw':{'항목명':'적설', '항목크기':6, '샘플데이터':'2.2', '설명':'적설(cm)'},
        'hr3Fhsc':{'항목명':'3시간신적설', '항목크기':6, '샘플데이터':'0.2', '설명':'3시간신적설(cm)'},
        'dc10Tca':{'항목명':'전운량', '항목크기':6, '샘플데이터':'0', '설명':'전운량(10분위)'},
        'dc10LmcsCa':{'항목명':'중하층운량', '항목크기':6, '샘플데이터':'0', '설명':'중하층운량(10분위)'},
        'clfmAbbrCd':{'항목명':'운형', '항목크기':4, '샘플데이터':'scas', '설명':'운형(운형약어)'},
        'lcsCh':{'항목명':'최저운고', '항목크기':2, '샘플데이터':'8', '설명':'최저운고(100m )'},
        'vs':{'항목명':'시정', '항목크기':4, '샘플데이터':'2300', '설명':'시정(10m)'},
        'gndSttCd':{'항목명':'지면상태', '항목크기':2, '샘플데이터':'17', '설명':'지면상태(지면상태코드)(종료: 2016.7.1.00시)'},
        'dmstMtphNo':{'항목명':'현상번호', '항목크기':4, '샘플데이터':'1904', '설명':'현상번호(국내식)'},
        'ts':{'항목명':'지면온도', '항목크기':6, '샘플데이터':'-3.4', '설명':'지면온도(°C)'},
        'tsQcflg':{'항목명':'지면온도 품질검사', '항목크기':1, '샘플데이터':'0', '설명':'관측값의 정상여부 판별 정보(하단참조)'},
        'm005Te':{'항목명':'5cm 지중온도', '항목크기':6, '샘플데이터':'-4.9', '설명':'5cm 지중온도(°C)'},
        'm01Te':{'항목명':'10cm 지중온도', '항목크기':6, '샘플데이터':'-2.4', '설명':'10cm 지중온도(°C)'},
        'm02Te':{'항목명':'20cm 지중온도', '항목크기':6, '샘플데이터':'-1', '설명':'20cm 지중온도(°C)'},
        'm03Te':{'항목명':'30cm 지중온도', '항목크기':6, '샘플데이터':'0.4', '설명':'30cm 지중온도(°C)'}
    }

cloud_dict = \
{
    'Ci':{'label':0, '층':'상층', '명칭':'권운',   '영문명':'Cirrus'},
    'Cc':{'label':1, '층':'상층', '명칭':'권적운', '영문명':'Cirrocumulus'},
    'Cs':{'label':2, '층':'상층', '명칭':'권층운', '영문명':'Cirrostratus'},
    'Ac':{'label':3, '층':'중층', '명칭':'고적운', '영문명':'Altocumulus'},
    'As':{'label':4, '층':'중층', '명칭':'고층운', '영문명':'Altostratus'},
    'Ns':{'label':5, '층':'중층', '명칭':'난층운', '영문명':'Nimbostratus'},
    'Sc':{'label':6, '층':'하층', '명칭':'층적운', '영문명':'Stratocumulus'},
    'St':{'label':7, '층':'하층', '명칭':'층운',   '영문명':'Stratus'},
    'Cu':{'label':8, '층':'하층', '명칭':'적운',   '영문명':'Cumulus'},
    'Cb':{'label':9, '층':'하층', '명칭':'적란운', '영문명':'Cumulonimbus'}
}

cloud_index = \
{
    0:'Ci',
    1:'Cc',
    2:'Cs',
    3:'Ac',
    4:'As',
    5:'Ns',
    6:'Sc',
    7:'St',
    8:'Cu',
    9:'Cb', 
}

def cloud_label(cloud:str)->int:
    return cloud_dict[cloud]['label']

def cloud_name(index:int)->str:
    return cloud_index[index]

def split_cloud(cloud_value:str)->list:
    if cloud_value == None:
        return None
    cloud_list = []
    while len(cloud_value) >= 2:
        cloud = cloud_value[-2:]
        cloud_list.append(cloud)
        cloud_value = cloud_value[:-2]
    return cloud_list
    
def cloud_converter(cloud_list:list)->list:
    label_list = np.zeros(10, dtype=int)
    while len(cloud_list):
        cloud = cloud_list.pop()
        label = cloud_label(cloud)
        label_list[label] = 1
    return label_list

def to_decimal(idx_list:list)->int:
    bin_str = ''.join([str(i) for i in idx_list])
    return int(bin_str, 2)

def function(cloud_value:str, is_decimal:bool=True):
    if cloud_value == None:
        return None
    cloud_list = split_cloud(cloud_value)
    label_list = cloud_converter(cloud_list)
    if is_decimal:
        return to_decimal(label_list)
    return label_list

def translate(dictionary:dict, column_name:str)->str:
    """
    translate(asos_dict, 'ts')
    >>> '지면온도'
    """
    return  dictionary[column_name]['항목명']

flag_dict = \
    {
        "null":"정상",
        1:"오류",
        9:"결측",
    }

asos_flag = ['ta', 'rn', 'ws', 'wd', 'hm', 'pa', 'ps', 'ss', 'ts']
