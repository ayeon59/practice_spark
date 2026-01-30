# Spark Log Pipeline

본 프로젝트는 Apache Spark를 활용한 데이터 처리 파이프라인 실습을 목적으로 하며,  
**원본 데이터 → 처리 로직 → 가공 결과**의 흐름이 명확히 드러나도록 폴더 구조를 설계했습니다.

## 프로젝트 구조

```
spark-log-pipeline/
├── data/
│   ├── raw/          # 원본 데이터
│   └── processed/    # Spark 결과 데이터
├── src/
│   ├── generate_logs.py   # 더미 로그 생성
│   └── spark_job.py       # Spark 메인 작업
├── requirements.txt
└── README.md
```

### data/raw/

Spark가 읽어들이는 **원본 데이터 영역**입니다.  
로그(JSON) 형태의 데이터가 저장되며, Spark 작업에서는 해당 데이터를 **수정하지 않고 읽기 전용**으로 사용합니다.  
이는 데이터 엔지니어링에서의 *원천 데이터 보존 원칙*을 반영한 구조입니다.

### data/processed/

Spark 처리 결과가 저장되는 영역입니다.  
집계 및 가공된 데이터는 Parquet 포맷으로 저장되며,  
분석 효율성과 이후 데이터 활용을 고려한 출력 구조를 사용합니다.

### src/

데이터 파이프라인의 로직이 위치하는 디렉토리입니다.

- `generate_logs.py`  
  테스트 및 실습을 위해 대량의 더미 로그 데이터를 생성하는 스크립트입니다.

- `spark_job.py`  
  SparkSession 생성부터 데이터 로드, 변환(transformation), 집계, 저장(action)까지  
  **전체 Spark 배치 파이프라인 흐름이 정의된 메인 실행 파일**입니다.

### requirements.txt

프로젝트 실행에 필요한 Python 패키지 목록입니다.  
환경 재현성을 고려하여 최소 의존성만 명시했습니다.

## 설치

```bash
pip install -r requirements.txt
```

## 사용법

### 1. 더미 로그 생성

```bash
python src/generate_logs.py
```

### 2. Spark 작업 실행

```bash
python src/spark_job.py
```

## 분석 결과

- `level_stats/` - 로그 레벨별 분포
- `endpoint_stats/` - 엔드포인트별 통계
- `status_stats/` - HTTP 상태 코드 분포
- `error_logs/` - 에러 로그 필터링 결과
