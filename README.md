# Elasticsearch Source Connector for Apache Kafka

🚧 **개발 중인 프로젝트입니다** 🚧

이 프로젝트는 현재 개발 중이며, 기능과 API가 변경될 수 있습니다.

## 개요

Elasticsearch에서 데이터를 읽어서 Apache Kafka 토픽으로 스트리밍하는 Kafka Connect Source Connector입니다.

## 주요 기능

- ✅ Elasticsearch 인덱스에서 데이터 읽기
- ✅ Kafka Connect 표준 준수
- ✅ `search_after` 기반 페이지네이션으로 대용량 데이터 처리
- 🚧 오프셋 관리를 통한 중단 지점 복구 (개발 중)
- ✅ Basic Auth 인증 지원
- ✅ 커스텀 Elasticsearch 쿼리 지원


## 빌드 방법

```bash
mvn clean package
```

빌드 완료 후 `target/ElasticSourceConnector-1.0-SNAPSHOT-jar-with-dependencies.jar` 파일이 생성됩니다.

## 설정 옵션

| 설정 키 | 설명 | 필수 | 기본값 |
|---------|------|------|--------|
| `connector.class` | 커넥터 클래스명 | ✅ | com.engine.elasticsearch.EsSourceConnector |
| `elasticsearch.hosts` | Elasticsearch 호스트 주소 (콤마 구분) | ✅ | - |
| `elasticsearch.index` | 읽어올 Elasticsearch 인덱스명 | ✅ | - |
| `elasticsearch.query` | Elasticsearch 쿼리 (JSON 형식) | ✅ | - |
| `elasticsearch.sort.field` | 정렬 필드명 (문서 고유 ID) | ✅ | - |
| `connector.topic` | Kafka 토픽명 | ✅ | - |
| `tasks.max` | 최대 태스크 수 | ✅ | 1 |
| `name` | 커넥터 이름 | ✅ | - |
| `elasticsearch.user` | Elasticsearch 사용자명 | ❌ | - |
| `elasticsearch.password` | Elasticsearch 비밀번호 | ❌ | - |
| `elasticsearch.query.size` | 한 번에 가져올 레코드 수 | ❌ | 200 |

## 사용 예시

### 1. Connector 설정 파일 (connector-config.json)

```json
{
  "name": "EsSourceConnector",
  "config": {
    "connector.class": "com.engine.elasticsearch.EsSourceConnector",
    "tasks.max": "1",
    "elasticsearch.hosts": "localhost:9200",
    "elasticsearch.index": "my-index",
    "elasticsearch.sort.field": "doc_id",
    "elasticsearch.query": "{\"query\": {\"query_string\": {\"query\": \"*\"}}}",
    "elasticsearch.user": "elastic",
    "elasticsearch.password": "******",
    "elasticsearch.query.size": "100",
    "connector.topic": "elasticsearch-source"
  }
}
```

### 2. Connector 시작

```bash
# Kafka Connect REST API 사용
curl -X POST \
  http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connector-config.json
```

## 동작 원리

1. **초기화**: Elasticsearch 클라이언트 생성
2. **데이터 폴링**: 설정된 쿼리로 Elasticsearch에서 데이터를 페이지 단위로 읽기
3. **오프셋 관리**: `search_after` 값을 사용한 페이지 추적 (🚧 개발 중)
4. **Kafka 전송**: 읽어온 데이터를 Kafka 토픽으로 전송
5. **지속적 폴링**: 주기적으로 새로운 데이터 확인

## 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Elasticsearch │ -> │ Source Connector │ -> │  Kafka Topic    │
│                 │    │  (search_after) │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 프로젝트 구조

```
src/
├── main/
│   ├── java/
│   │   └── com/engine/elasticsearch/
│   │       ├── EsSourceConnector.java      # 메인 커넥터 클래스
│   │       ├── EsSourceTask.java           # 실제 작업 수행 태스크
│   │       ├── config/
│   │       │   └── EsSourceConnectorConfig.java  # 설정 관리
│   │       └── elasticsearch/
│   │           ├── EsClient.java           # Elasticsearch 클라이언트
│   │           └── QueryUtils.java         # 쿼리 유틸리티
│   └── resources/
│       └── log4j2.xml                      # 로깅 설정
```

## 개발 상태

### ✅ 완료된 기능
- [x] 기본적인 Elasticsearch 연결
- [x] search_after 기반 페이지네이션
- [x] Kafka Connect 표준 구현
- [x] Basic Auth 인증

### 🚧 개발 중인 기능
- [ ] 오프셋 관리 및 중단 지점 복구
- [ ] SSL/TLS 연결 지원
- [ ] 멀티 인덱스 지원
- [ ] 스키마 레지스트리 지원
- [ ] 메트릭 수집
- [ ] 에러 핸들링 개선

### 📋 향후 계획
- [ ] 단위 테스트 추가
- [ ] 통합 테스트 추가
- [ ] 문서 개선
- [ ] 성능 최적화
- [ ] CI/CD 파이프라인 구축


---

⚠️ **주의사항**: 이 프로젝트는 개발 중이므로 프로덕션 환경에서 사용하기 전에 충분한 테스트를 수행하시기 바랍니다.
