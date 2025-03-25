import pandas as pd
from pandas_gbq import to_gbq

def upload_multiple_parquet_tables_to_bq(
    table_names: list,
    folder: str,
    bucket: str,
    dataset: str,
    project_id: str,
    timestamp_cols: list
):
    for table_name in table_names:
        print(f"\n======================\n📦 처리 중: {table_name}")

        gcs_path = f"gs://{bucket}/{folder}/{table_name}.parquet"
        destination_table = f"{dataset}.{table_name}"

        try:
            df = pd.read_parquet(gcs_path)
            print(f"✅ 읽은 데이터 shape: {df.shape}")

            for col in timestamp_cols:
                if col in df.columns:
                    print(f"🕒 {col} → datetime 변환")
                    df[col] = pd.to_datetime(df[col], unit='ns')

            to_gbq(
                dataframe=df,
                destination_table=destination_table,
                project_id=project_id,
                if_exists="replace"
            )

            print(f"✅ BigQuery 적재 완료: {destination_table}")
        except Exception as e:
            print(f"❌ {table_name} 실패: {e}")


if __name__ == "__main__":
    # 예시 실행 코드 (이건 필요 시만 남기세요)
    tables = [
        "accounts_attendance", "accounts_blockrecord", "accounts_failpaymenthistory",
        "accounts_friendrequest", "accounts_group", "accounts_nearbyschool",
        "accounts_paymenthistory", "accounts_pointhistory", "accounts_school",
        "accounts_timelinereport", "accounts_user", "accounts_user_contacts",
        "accounts_userquestionrecord", "accounts_userwithdraw", "event_receipts",
        "events", "polls_question", "polls_questionpiece", "polls_questionreport",
        "polls_questionset", "polls_usercandidate"
    ]

    upload_multiple_parquet_tables_to_bq(
        table_names=tables,
        folder="votes",
        bucket="final-project-source-data",
        dataset="main_votes",
        project_id="your_project_id",  # 🔁 실제 프로젝트 ID로 변경
        timestamp_cols=["created_at", "updated_at", "opening_time", "answer_updated_at"]
    )