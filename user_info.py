import pandas as pd
import numpy as np 
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import os
from google.cloud import storage
from google.oauth2 import service_account

JSON_KEY_PATH = "json 파일 경로"

## 유저 테이블
user = pd.read_parquet(
    'gs://final-project-source-data/votes/accounts_user.parquet',
    storage_options={'token' : JSON_KEY_PATH}
    )

## user_properties
user_properties = pd.read_parquet(
    'gs://final-project-source-data/hackle/user_properties.parquet',
    storage_options={'token' : JSON_KEY_PATH}
    )

## 학교 테이블
school = pd.read_parquet(
    'gs://final-project-source-data/votes/accounts_school.parquet',
    storage_options={'token' : JSON_KEY_PATH}
    )

## 출석 테이블 데이터 읽기
attendance = pd.read_parquet(
    "gs://final-project-source-data/votes/accounts_attendance.parquet",
    storage_options={'token' : JSON_KEY_PATH}
    )

## 유저 신고기록 테이블
timelinereport = pd.read_parquet(
    'gs://final-project-source-data/votes/accounts_timelinereport.parquet',
    storage_options={'token' : JSON_KEY_PATH}
    )

## 차단 테이블 데이터 읽기
blockrecord = pd.read_parquet(
    'gs://final-project-source-data/votes/accounts_blockrecord.parquet',
    storage_options={'token' : JSON_KEY_PATH}
    )


# 유저 정보 테이블 생성

# 일반적인 유저 정보
user_info = user[['id','gender','created_at','group_id','friend_id_list']] # 유저테이블에서 정보 추출
user_info = user_info.rename(columns = {'id':'user_id'}) # 컬럼 재설정
user_info['user_id'] = user_info['user_id'].astype(int) # merge를 위한 데이터 타입 변경

# user_properties에서 merge 준비
user_properties = user_properties[~user_properties['user_id'].str.contains('nhj4wh46MAf5K0IHDu4DGyRsdWn2')] # 데이터 타입에 맞지 않은 로우값 삭제
user_properties = user_properties[~user_properties['user_id'].str.contains('nmbzA4awkiRGXX26fT6wpoxURY43')] # 데이터 타입에 맞지 않은 로우값 삭제
user_properties['user_id'] = user_properties['user_id'].astype(int) # merge를 위한 데이터 타입 변경

# school 테이블에서 merge 준비
school = school.rename(columns={'id':'school_id',
                                'address':'school_address'                                
                                })

# attendance 테이블에서 merge 준비
attendance['Recent_access_history'] = attendance['attendance_date_list'].iloc[-1]
attendance = attendance[['user_id','Recent_access_history']]

# 유저 정보 + user_properties merge
user_info = pd.merge(user_info, user_properties, on='user_id', how='inner')
user_info = user_info.drop(columns='gender_y') # 필요하지 않은 컬럼 드랍
user_info['group_id'] = user_info['group_id'].astype(int) # 소수점 표시 변경
user_info = user_info.rename(columns={'gender_x':'gender'}) 
user_info ['created_at'] = user_info['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S') # 날짜 타입 변경
user_info ['created_at'] = pd.to_datetime(user_info ['created_at'])

# 유저 정보 + school merge
user_info = pd.merge(user_info,school, on='school_id', how='inner')

# 유저 정보 + attendance merge
user_info = pd.merge(user_info,attendance, on='user_id', how='inner')

# 유저 정보 컬럼 재배치
user_info = user_info[['user_id','gender','created_at','grade','class','school_id','school_address','Recent_access_history','friend_id_list']]



# 유저가 신고, 차단한 정보 timelinereport,blockrecord 
# merged_df = pd.merge(df1, df2, on='ID', how='inner')
user_report = pd.merge(timelinereport, blockrecord, on='user_id', how ='outer')
user_report = user_report[['user_id','reported_user_id','reason_x','created_at_x','block_user_id','reason_y','created_at_y']]
user_report= user_report.rename(columns={'reason_x':'report_reason',
                                     'created_at_x':'report_created_at',
                                     'reason_y':'block_reason',
                                     'created_at_y':'block_created_at'
                                     })