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

### Phase 4: Quality Gate 및 데이터 카탈로그 구현

**추가된 파일:**

| 파일 | 역할 |
|------|------|
| `src/catalog.py` | 데이터 카탈로그 생성 모듈 |

**구현 내용:**

#### 1. Quality Gate (`main.py`)
- DQ 검사 결과를 평가하는 `_evaluate_quality()` 함수 추가
- **비율 기반 임계값 도입**: `NULL_RATE_THRESHOLD = 0.005` (0.5%)
- 검사 항목:
  - 스키마 일치 여부
  - 필수 컬럼 null 비율 (개수가 아닌 비율로 평가)
  - 음수/이상치 response_time 존재 여부
  - 유효하지 않은 log level 존재 여부
- **품질 검사 실패 시 파이프라인 중단** (`sys.exit(1)`)
- 리포트에 null_rate 정보 포함

#### 2. 데이터 카탈로그 (`catalog.py`)
- 데이터셋 메타데이터 자동 생성:
  - 데이터셋 이름/설명
  - 생성 시간 (UTC ISO format)
  - input/output 경로 및 포맷
- **스키마 문서화**: raw 스키마, processed 스키마 자동 추출
- **메트릭 정의**: 각 집계 메트릭의 의미 기술
- **데이터 리니지**: 입력 → 변환 과정 → 출력 추적
- 결과를 `reports/catalog.json`으로 저장

#### 3. 스키마 정합성 개선
- `expected_schema.json`을 실제 데이터와 일치하도록 수정
- 스키마 버전 관리 필드 추가 (`version: "v1"`)
- 타입 및 nullable 속성 실제 데이터 기준으로 보정

#### 4. 스키마 버전 추적 연동
- `quality.py`: 스키마 파일에서 version 읽어서 리포트에 포함
- `catalog.py`: schema_version 파라미터 추가
- `main.py`: quality → catalog로 스키마 버전 전달
- 리포트 간 스키마 버전 일관성 확보 (`dq_report.json`, `catalog.json`)

**실행 결과:**
- `schema_ok: true` - 스키마 검증 통과
- `quality_passed: true` - 품질 검사 통과
- null 비율: service 0.098%, response_time_ms 0.2% (임계값 0.5% 이하)
- 데이터 카탈로그 생성 완료 (`reports/catalog.json`)

---

### Phase 5: 파이프라인 오케스트레이션 및 증분 처리

**추가된 파일:**

| 파일 | 역할 |
|------|------|
| `src/pipeline.py` | 파이프라인 오케스트레이션 (메인 실행 모듈) |
| `src/state.py` | Watermark 기반 상태 관리 |
| `airflow/dags/log_pipeline_dag.py` | Airflow DAG 정의 |

**구현 내용:**

#### 1. 파이프라인 오케스트레이션 (`pipeline.py`)
- `run_pipeline()` 함수로 전체 흐름 통합
- **단계별 실행 로깅**: `_run_step()` - 각 단계 시작/종료 시간 출력
- **경로 파라미터화**: input/output/schema/reports 경로 설정 가능
- **증분 처리**: watermark 기반으로 새 데이터만 처리
- **조기 종료**: 새 데이터 없으면 즉시 종료
- **exit code 반환**: 성공(0), 실패(1)

#### 2. 상태 관리 (`state.py`)
- `load_watermark()`: 마지막 처리 timestamp 로드
- `save_watermark()`: 처리 완료 후 timestamp 저장
- 파일 기반 상태 저장 (`state/last_run.json`)

#### 3. 증분 처리 지원 (`ingestion.py`)
- `since_ts` 파라미터 추가
- `F.to_timestamp()`로 문자열 → timestamp 변환
- watermark 이후 데이터만 필터링

#### 4. Airflow 스케줄링 (`airflow/dags/`)
- DAG ID: `spark_log_pipeline`
- 스케줄: `@daily`
- `BashOperator`로 파이프라인 실행
- 태그: `spark`, `quality`, `catalog`

#### 5. 엔트리포인트 리팩토링 (`main.py`)
- `pipeline.py`의 `run_pipeline()` 래핑
- 4줄로 간소화된 thin wrapper
- `SystemExit`으로 exit code 전달

---

### Phase 6: 외부 RDB 연동 및 사용자 메트릭

**추가된 파일:**

| 파일 | 역할 |
|------|------|
| `src/rdb.py` | PostgreSQL JDBC 연동 모듈 |

**구현 내용:**

#### 1. PostgreSQL JDBC 연동 (`rdb.py`)
- 환경변수 기반 DB 접속 설정 (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`)
- `spark.read.format("jdbc")`로 테이블 로드
- PostgreSQL 드라이버: `org.postgresql:postgresql:42.7.4`

#### 2. 사용자 메트릭 집계 (`transform.py`)
- `build_user_metrics()` 함수 추가
- 로그 DataFrame과 users 테이블 **LEFT JOIN** (`on="user_id"`)
- 사용자 플랜(plan)별 메트릭 집계:
  - 총 로그 수
  - 평균 응답 시간
  - 에러 카운트 및 에러율

#### 3. 파이프라인 확장 (`pipeline.py`)
- JDBC 드라이버 config 추가 (`spark.jars.packages`)
- 새 단계 추가:
  - `rdb_load`: PostgreSQL에서 users 테이블 로드
  - `user_transform`: 사용자별 메트릭 집계
  - `user_load`: 결과 저장 (`data/processed/user_metrics`)
- `user_output_path` 파라미터 추가

#### 4. 데이터 카탈로그 확장 (`catalog.py`)
- `user_metrics_df`, `user_output_path` 파라미터 추가
- `outputs` 배열: service_metrics + user_metrics 모두 포함
- `lineage.outputs`: 다중 출력 추적
- `lineage.transformations`: JOIN 변환 단계 추가
- `user_metrics_schema`: 사용자 메트릭 스키마 자동 문서화

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
| JDBC | `spark.read.format("jdbc")`, PostgreSQL 연동 |
| 조인 | `df.join()`, LEFT JOIN |

---

## 다음 작업 예정
- [ ] 단위 테스트 추가 (pytest + pyspark.testing)
- [ ] 로깅 시스템 추가
- [ ] CLI 인터페이스 구현 (argparse)
- [ ] 에러 핸들링 강화
