# Spark Log Pipeline - 작업 이력

이력서 기재용 작업 내역 트래킹 문서

---

## 기술 스택
- **Apache Spark** (PySpark 3.5.0)
- **Python 3.x**
- **Parquet** (컬럼 기반 저장 포맷)

---

## 작업 이력

### Phase 1: 프로젝트 초기 세팅
**커밋:** `2c1128f` initial setting

- 프로젝트 디렉토리 구조 설계 (`data/raw`, `data/processed`, `src/`)
- Python 가상환경 및 의존성 관리 (`requirements.txt`)
- 원본 데이터 보존 원칙을 반영한 폴더 구조 설계

---

### Phase 2: SparkSession 생성 및 DataFrame 기반 데이터 집계
**커밋:** `61239a3` sparksession 생성 및 DF 기반 데이터 집계 연습

**구현 내용:**
- SparkSession 생성 및 로그 레벨 설정
- JSON 로그 데이터 로드 (`spark.read.json`)
- 스키마 확인 및 null 값 카운트 (데이터 품질 점검)
- 서비스별 메트릭 집계:
  - `groupBy()` + `agg()`를 활용한 집계
  - `count()`, `avg()`, `percentile_approx()` 함수 사용
  - 에러율 계산 (`error_count / total_logs`)
- Parquet 포맷으로 결과 저장 (`write.mode("overwrite").parquet()`)

**핵심 Spark 개념:**
- Transformation vs Action 구분
- Lazy Evaluation 이해
- DataFrame API 활용

---

### Phase 3: 파이프라인 모듈화 및 데이터 품질 검증
**커밋:** ETL 모듈 분리 및 데이터 품질 검증 시스템 구현

**추가된 파일:**

| 파일 | 역할 |
|------|------|
| `src/main.py` | 파이프라인 엔트리포인트 (모듈 조합) |
| `src/ingestion.py` | 데이터 로드 레이어 |
| `src/transform.py` | 데이터 변환 로직 |
| `src/quality.py` | 스키마 검증 + 데이터 품질 체크 |
| `src/load.py` | 결과 저장 레이어 |
| `schemas/expected_schema.json` | 기대 스키마 정의 |

**구현 내용:**

#### 1. ETL 파이프라인 모듈화
- 단일 스크립트(`spark_job.py`)를 역할별 모듈로 분리
- Ingestion → Transform → Load 구조
- 유지보수성 및 테스트 용이성 향상

#### 2. 데이터 품질 검증 시스템 (`quality.py`)
- **스키마 검증:**
  - 기대 스키마와 실제 스키마 비교
  - 누락 필드 탐지
  - 추가 필드 탐지
  - 타입 불일치 검출
  - nullable 속성 검증
- **데이터 품질 체크:**
  - 전체 row 수 카운트
  - 필수 컬럼 null 값 카운트 (`service`, `response_time_ms`)
  - 이상값 탐지: 음수 response_time, 10초 초과 response_time
  - 유효하지 않은 log level 탐지 (INFO, WARN, ERROR 외)

#### 3. 서비스 메트릭 변환 (`transform.py`)
- null 데이터 제거 (`dropna`)
- 서비스별 집계 메트릭:
  - 총 로그 수
  - 평균 응답 시간
  - P95 응답 시간 (95 백분위수)
  - 에러 카운트 및 에러율

#### 4. 파이프라인 통합 및 리포트 출력
- `main.py`에서 품질 검증 모듈 연동
- 검증 결과를 JSON 파일로 저장 (`reports/dq_report.json`)
- 실제 100만 건 로그 데이터 대상 실행 완료

**실행 결과 (DQ Report):**
- 총 데이터: 1,000,000 rows
- 발견된 이슈:
  - 예상치 못한 필드: `request_id`
  - 타입 불일치: `response_time_ms` (long→bigint), `user_id` (string→bigint)
  - nullable 불일치: 4개 필드
  - null 값: service 982건, response_time_ms 2,017건

---

## 사용된 Spark 기능 요약

| 카테고리 | 기능 |
|----------|------|
| 세션 관리 | `SparkSession.builder`, `setLogLevel()` |
| 데이터 로드 | `spark.read.json()` |
| 변환 | `groupBy()`, `agg()`, `withColumn()`, `orderBy()`, `filter()`, `dropna()` |
| 집계 함수 | `count()`, `avg()`, `sum()`, `percentile_approx()` |
| 조건 처리 | `F.col()`, `F.expr()`, `cast()`, `isNull()`, `isin()` |
| 저장 | `write.mode().parquet()` |
| 스키마 | `df.schema`, `StructType`, `printSchema()` |

---

## 다음 작업 예정
- [ ] 단위 테스트 추가 (pytest + pyspark.testing)
- [ ] 로깅 시스템 추가
- [ ] CLI 인터페이스 구현 (argparse)
- [ ] 에러 핸들링 강화
