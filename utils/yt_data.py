from googleapiclient.discovery import build
from dotenv import load_dotenv
from pprint import pprint # dict 가독성 좋게 보여주는 라이브러리
import os


load_dotenv('/home/ubuntu/airflow/.env') # 해당 위치의 환경 변수 불러오기
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    # youtube api 중 v3버전 사용하겠단 것 >> 사용자 인증 위해 API 키 입력

# 함수1: handle 기준으로 channelID 찾기
def get_channel_id(youtube, handle): # youtube 인스턴스 활용하는 것
    response = youtube.channels().list(part='id', forHandle=handle).execute()
        # 유튜브 채널 리스트 꺼내 실행 >> 이 결과를 response 변수에 저장
    return response['items'][0]['id'] # dict 중에 id 포함돼 있음 -> 이거 추출하는 게 이 함수의 목적
# 실행
target_handle = 'BBCNews'
channel_id = get_channel_id(youtube, target_handle)
# print(channel_id)

# 2. 함수2: channel id 기준, 최신 영상 id 찾기
def get_lastest_video_ids(youtube, channel_id):
    response = youtube.search().list(
        part='id',
        channelId=channel_id,
        maxResults=5,
        order='date' # 날짜 기준 정렬
    ).execute()

    video_ids = []
    for item in response['items']:
        video_ids.append(item['id']['videoId'])
    return video_ids
    # BBC 뉴스의 최신 영상 5개의, 비디오 id 추출

# 실행
lastest_video_ids = get_lastest_video_ids(youtube, channel_id)
# print(lastes_video_id)

# 3. 함수3; video id 기준, comment 수집
def get_comments(youtube, video_id):
    response = youtube.commentThreads().list(
        part='snippet', # 어디를 보고 싶은지
        videoId=video_id,
        textFormat='plainText', # 원본 텍스트로
        maxResults=100, # 상위 100개 댓글 수집
        order='relevance' # 좋아요 기준 정렬
    ).execute()
    
    # 댓글 중 필요 정보만 추출하기
    comments = []
    for item in response['items']:
        comment = {
            'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'], # 작성자
            'text': item['snippet']['topLevelComment']['snippet']['textDisplay'], # 댓글 텍스트
            'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt'], # 댓글 작성 시점
            'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'], # 좋아요 수
            'commentId': item['snippet']['topLevelComment']['id'] # 댓글 id
        }
        comments.append(comment)
    return comments
    
# 4. 함수4: 총 실행하는 함수
def get_handle_to_comments(youtube, handle):
    channel_id = get_channel_id(youtube, handle)
    lastest_video_ids = get_lastest_video_ids(youtube, channel_id)
    
    all_comments = {}
    # 비디오
    for video_id in lastest_video_ids: # 여러 비디오 id 중 하나씩 꺼내기
        comments = get_comments(youtube, video_id)
        all_comments[video_id] = comments # 각 영상별로 해당 댓글 100개 매칭

    return {
        'handle': handle,
        'all_comments': all_comments
        }

get_handle_to_comments(youtube, target_handle)

# 5. 함수5: hdfs에 업로드
def save_to_hdfs(data, path):
    from hdfs import InsecureClient # pypi의 hdfs 라이브러리 쓰는 것
    from datetime import datetime
    import json

    client = InsecureClient('http://localhost:9870', user='ubuntu') # 하둡 주소, 어떤 유저로 들어가는지

    # 수집 시간 기준으로 파일명 설정
    current_date = datetime.now().strftime('%y%m%d%H%M') # 현재 시간 출력, 2504241142 -> 이런 식으로 파일명 하겠단 것
    file_name = f'{current_date}.json' # 파일명 형식 설정

    # 최종 하둡 저장 경로 지정: ex. /input/yt-data + 2504241142.json
    hdfs_path = f'{path}/{file_name}'

    json_data = json.dumps(data, ensure_ascii=False) # all_comment는 dict -> dict을 json으로 만들어줌 | 댓글 내 이모지 관련 설정

    # 하둡에 저장
    with client.write(hdfs_path, encoding='utf-8') as writer:
        writer.write(json_data)
        # json_data를 hdfs_path에 작성해달라 요청 -> hdfs에 파일 업로드 하게 됨
    # with: as writer >> 변수 writer를 해당 코드에서만 사용 가능하게 해주는 것 -> 임시 변수 만든 셈

# 실행 >> utils에는 함수 구현만, 실행은 07_collect_yt_data에서 하기
# data = get_handle_to_comments(youtube, target_handle)
# save_to_hdfs(data, '/input/yt-data') # data를 input 내 yt-data 폴더에 넣어달라