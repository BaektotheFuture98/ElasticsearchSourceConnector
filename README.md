# Elasticsearch Source Connector for Apache Kafka

Elasticsearch 인덱스의 문서를 조회해 Apache Kafka 토픽으로 발행하는 Kafka Connect Source Connector입니다.

## 주요 기능

- Elasticsearch `_search` API 기반 데이터 조회
- Kafka Connect `SourceConnector` / `SourceTask` 구현
- `search_after` 기반 페이지네이션
- Kafka Connect source offset을 이용한 재시작 위치 복구
- Elasticsearch 문서 식별자를 Kafka record key로 사용
- Basic Auth 인증 지원
- 커스텀 Elasticsearch query DSL 지원
- 로컬 Kafka Connect / Elasticsearch Docker Compose 구성
- 단위 및 통합 테스트 코드

## 빌드

```bash
mvn clean package
```

빌드가 완료되면 다음 파일이 생성됩니다.

```text
target/ElasticSourceConnector-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 테스트

```bash
mvn test
```

현재 확인된 결과:

```text
Tests run: 68, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## 설정 옵션

| 설정 | 설명 | 필수 | 기본값 |
|---|---|---:|---|
| `connector.class` | 커넥터 클래스명 | 예 | `com.engine.elasticsearch.EsSourceConnector` |
| `name` | 커넥터 이름 | 예 | - |
| `tasks.max` | 최대 task 수 | 아니오 | `1` |
| `elasticsearch.hosts` | Elasticsearch host 목록. 쉼표로 구분 | 예 | - |
| `elasticsearch.index` | 읽을 Elasticsearch 인덱스 | 예 | - |
| `elasticsearch.query` | Elasticsearch query DSL JSON 문자열 | 예 | - |
| `elasticsearch.sort.field` | query에 sort가 없을 때 추가할 정렬 필드 | 아니오 | - |
| `elasticsearch.primary.key` | Kafka record key로 사용할 필드. `_id` 또는 `_source` 필드명 | 아니오 | `_id` |
| `elasticsearch.user` | Elasticsearch 사용자명 | 아니오 | - |
| `elasticsearch.password` | Elasticsearch 비밀번호 | 아니오 | - |
| `elasticsearch.query.size` | 한 번에 조회할 문서 수 | 아니오 | `200` |
| `connector.topic` | 발행할 Kafka 토픽 | 예 | - |

## 설정 예시

```json
{
  "name": "EsSourceConnector",
  "config": {
    "connector.class": "com.engine.elasticsearch.EsSourceConnector",
    "tasks.max": "1",
    "elasticsearch.hosts": "localhost:9200",
    "elasticsearch.index": "my-index",
    "elasticsearch.sort.field": "updated_at",
    "elasticsearch.primary.key": "_id",
    "elasticsearch.query": "{\"query\":{\"match_all\":{}},\"sort\":[{\"updated_at\":{\"order\":\"asc\"}},{\"id\":{\"order\":\"asc\"}}]}",
    "elasticsearch.user": "elastic",
    "elasticsearch.password": "******",
    "elasticsearch.query.size": "100",
    "connector.topic": "elasticsearch-source"
  }
}
```

Kafka Connect REST API로 등록합니다.

```bash
curl -X POST \
  http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json
```

## 동작 방식

1. `EsSourceConnector`가 설정을 검증하고 `EsSourceTask` 설정을 생성합니다.
2. `EsSourceTask`가 Elasticsearch client를 생성하고 이전 source offset을 읽습니다.
3. offset에 저장된 `search_after` 값이 있으면 다음 조회 시작점으로 복원합니다.
4. Elasticsearch `_search` 요청을 실행합니다.
5. 응답 hit의 `sort` 배열 전체를 source offset으로 저장합니다.
6. Elasticsearch `_id` 또는 `elasticsearch.primary.key`로 지정한 `_source` 필드를 Kafka record key로 넣습니다.
7. hit 전체 JSON을 Kafka record value로 발행합니다.

## Offset과 중복 처리

이 커넥터는 Kafka Connect source offset에 Elasticsearch hit의 `sort` 배열 전체를 저장합니다.

예:

```json
{
  "search_after": "[1710498600000,\"doc-123\"]"
}
```

재시작 시 이 값을 다시 Elasticsearch `search_after`에 넣어 이어 읽습니다.

Kafka record key는 기본적으로 Elasticsearch `_id`입니다.

```text
Kafka key   = Elasticsearch _id
Kafka value = Elasticsearch hit JSON
Offset      = Elasticsearch sort 배열 전체
```

주의할 점:

- offset은 읽기 위치를 복구하기 위한 cursor입니다.
- 중복을 100% 제거하는 장치는 아닙니다.
- 장애 시 마지막으로 commit된 offset 이후 일부 데이터가 재처리될 수 있습니다.
- downstream에서는 Kafka key 기준 upsert/idempotent 처리를 권장합니다.

## 정렬 조건 주의사항

`search_after`를 안전하게 사용하려면 sort 조건이 안정적이어야 합니다.

권장:

```json
"sort": [
  { "updated_at": { "order": "asc" } },
  { "id": { "order": "asc" } }
]
```

동일한 `updated_at` 값을 가진 문서가 많다면 고유 필드를 tie-breaker로 함께 정렬해야 누락/중복 위험이 줄어듭니다.

## 로깅

문제 원인 파악을 위해 다음 정보를 로그로 남깁니다.

- Task 시작 설정: topic, index, primary key, source partition
- 복구된 raw offset과 `search_after`
- Elasticsearch client 생성 정보
- Elasticsearch search endpoint, query, `search_after`
- Elasticsearch 응답 hit 수
- sort 누락으로 skip된 문서
- Kafka key가 null인 문서
- poll 결과 요약: fetched, produced, skipped, missing key, next `search_after`

비밀번호 값은 로그에 출력하지 않습니다.

## 로컬 실행 구성

Docker Compose 파일:

```text
docker/kafka_connect/kafka-connect.yml
docker/elasticsearch/elasticsearch-single.yml
docker/elasticsearch/elasticsearch-cluster.yml
```

Kafka Connect plugin 디렉터리에 빌드된 jar를 배치한 뒤 Connect를 실행합니다.

예:

```text
docker/kafka_connect/plugins/elasticsearch-source-connector/
└── ElasticSourceConnector-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 프로젝트 구조

```text
src/main/java/com/engine/elasticsearch/
├── EsSourceConnector.java
├── EsSourceTask.java
├── config/
│   └── EsSourceConnectorConfig.java
└── elasticsearch/
    ├── EsClient.java
    └── QueryUtils.java

src/test/java/com/engine/elasticsearch/
├── EsSourceConnectorTest.java
├── EsSourceTaskTest.java
├── ElasticsearchConnectorIntegrationTest.java
├── config/EsSourceConnectorConfigTest.java
└── elasticsearch/QueryUtilsTest.java
```

## 개발 상태

### 완료

- [x] 기본 Elasticsearch 연결
- [x] Kafka Connect SourceConnector / SourceTask 구현
- [x] Basic Auth 인증
- [x] `search_after` 기반 페이지네이션
- [x] source offset 기반 `search_after` 복구
- [x] Kafka record key에 Elasticsearch 문서 식별자 설정
- [x] Elasticsearch host 파싱 개선
- [x] EsClient 종료 처리
- [x] 원인 분석용 로그 보강
- [x] 단위 및 통합 테스트 추가
- [x] `mvn test` 통과

### 제한사항 / 개선 필요

- [ ] 현재 multi-task partitioning은 지원하지 않음. 중복 수집 방지를 위해 task config는 1개만 생성
- [ ] SSL/TLS 연결 지원
- [ ] 멀티 인덱스 분산 수집
- [ ] Schema Registry 연동
- [ ] 메트릭 수집
- [ ] Elasticsearch PIT(Point In Time) 기반 안정화
- [ ] 에러 핸들링 및 재시도 정책 고도화

## 주의

현재 구현은 source offset과 Kafka key를 분리해 중복/재처리 대응 기반을 갖췄지만, 운영 환경에서는 정렬 필드 설계와 downstream idempotent 처리가 함께 필요합니다.
