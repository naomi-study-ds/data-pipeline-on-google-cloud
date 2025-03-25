# Parquet → BigQuery Upload Script

Google Cloud Storage(GCS)에 저장된 Parquet 파일을 불러와
타임스탬프 필드를 datetime으로 변환한 후 BigQuery에 자동으로 적재하는 Python 스크립트입니다.

## 📦 사용법

1. GCS에 `.parquet` 파일들을 업로드해둡니다.
2. `upload_parquet_to_bigquery.py` 파일에서 테이블 리스트와 GCP 프로젝트 ID, 버킷/폴더/데이터셋 이름을 수정합니다.
3. Python 환경에서 실행하면 자동으로 BigQuery에 테이블이 생성되고 적재됩니다.

## 🛠 의존 패키지

- pandas
- pandas-gbq
- pyarrow

## 🔐 인증 관련

Colab에서는 로그인 팝업을 통해 자동 인증되며,
로컬에서는 아래 명령어로 인증할 수 있습니다:

```bash
gcloud auth application-default login
```
