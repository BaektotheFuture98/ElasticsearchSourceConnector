package com.engine.elasticsearch;
import com.engine.elasticsearch.config.EsSourceConnectorConfig;
import com.engine.elasticsearch.elasticsearch.EsClient;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 실제 데이터 수집을 수행하는 SourceTask.
 *
 * 동작 개요:
 * 1) start()에서 설정/클라이언트/이전 offset을 준비
 * 2) poll()에서 Elasticsearch 조회 후 SourceRecord 리스트 생성
 * 3) Kafka Connect가 각 레코드의 offset을 저장
 *
 * 실행 순서(중요):
 * - 첫 poll: searchAfter == null 로 시작 -> 첫 페이지 조회
     * - n번째 poll: 이전 배치 마지막 sort 값을 search_after로 전달
 * - 결과: 중복/누락을 줄이면서 페이지를 순차적으로 전진
 */
public class EsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(EsSourceTask.class);

    // 설정값 및 상태를 저장할 변수들
    private EsClient client;
    private String topic;
    private String index;

    private final static String ES_FIELD = "es_index";
    private final static String OFFSET_KEY = "offset";

    // [중요] Kafka Connect의 핵심 : 이름표(Partition)와 책갈피(Offset)
    private Map<String, Object> sourcePartition;

    // Elasticsearch 페이징을 위한 searchAfter 값 (JsonNode 배열로 저장)
    private ArrayNode searchAfter = null;
    private long offset = 0L;

    @Override
    public String version() {
        return "1.0.0";
    }

    /**
     * start() : Task 시작 시 딱 한 번 호출되는 초기화 단계
     */
    @Override
    public void start(Map<String, String> map) {
        log.info("Starting Elasticsearch SourceTask");
        EsSourceConnectorConfig config = new EsSourceConnectorConfig(map);

        // 설정을 객체로 변환
        this.topic = config.getString(EsSourceConnectorConfig.TOPIC);

        // 실제 API 호출을 담당할 클라이언트 생성
        this.client = new EsClient(config);
        this.index = config.getString(EsSourceConnectorConfig.ES_INDEX);

        // sourcePartition: 이 Task가 읽는 "소스 단위"를 식별하는 이름표.
        // 여기서는 index 이름 하나를 파티션 키로 사용한다.
        this.sourcePartition = Collections.singletonMap(ES_FIELD, index);

        // 동일 sourcePartition에 대해 이전에 저장된 offset이 있으면 재개 지점으로 사용한다.
        Map<String, Object> lastOffset = context.offsetStorageReader().offset(this.sourcePartition);
        if (lastOffset != null && lastOffset.containsKey(OFFSET_KEY)) {
            this.offset = (Long) lastOffset.get(OFFSET_KEY);
            log.info("Recovered offset: {}", this.offset);
        }

        // searchAfter는 null로 초기화 (첫 페이지부터 시작)
        this.searchAfter = null;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // 과도한 폴링 방지용 고정 간격(샘플 구현).
        Thread.sleep(1000);
        try {
            // [1] Elasticsearch 조회 (현재 searchAfter 기준)
            // search_after 기반으로 다음 페이지를 조회한다.
            ArrayNode response = client.search(this.searchAfter);

            // 새 데이터가 없으면 빈 리스트 반환.
            // 예외가 아니므로 Task 실패/재시도 없이 다음 poll 주기로 넘어간다.
            if (response == null || response.isEmpty()) {
                return new ArrayList<>();
            }

            // [2] ES 응답(JSON 문서들)을 Kafka SourceRecord 목록으로 변환
            List<SourceRecord> records = new ArrayList<>();
            // 마지막 문서의 sort 값을 기억해 다음 페이지 시작점(search_after)으로 사용한다.
            JsonNode lastSortValue = null;

            for (JsonNode node : response) {
                JsonNode sortNode = node.get("sort");
                if (sortNode == null || !sortNode.isArray() || sortNode.isEmpty()) {
                    log.warn("Sort field is missing or invalid, skipping record");
                    continue;
                }

                // 문서별 sort 값을 추출한다.
                JsonNode currentSortValue = sortNode.get(0);

                // Kafka Connect offset 저장을 위해 long으로 변환한다.
                // 주의: sort 필드가 숫자가 아닌 경우 hashCode로 대체 저장한다.
                long sortValueAsLong;
                if (currentSortValue.isNumber()) {
                    sortValueAsLong = currentSortValue.asLong();
                } else {
                    // 실제 운영에서는 sort 필드 타입을 명확히 고정하는 편이 안전하다.
                    sortValueAsLong = currentSortValue.asString().hashCode();
                    log.warn("Sort value is not numeric, using hashCode for offset: {}", sortValueAsLong);
                }

                // 각 레코드에 해당 문서의 진행 위치(offset)를 같이 담아 전달한다.
                Map<String, Object> recordOffset = Collections.singletonMap(
                        OFFSET_KEY,
                        sortValueAsLong
                );

                SourceRecord sourceRecord = new SourceRecord(
                        this.sourcePartition,
                        recordOffset,
                        this.topic,
                        null,                    // partition
                        null,                    // keySchema
                        null,                    // key
                        Schema.STRING_SCHEMA,    // valueSchema
                        node.toString()          // value
                );
                records.add(sourceRecord);
                lastSortValue = currentSortValue;
            }

            // [3] 다음 poll을 위한 페이지 포인터 갱신 (search_after)
            // 배치 처리 후 마지막 sort 값을 다음 poll의 search_after로 저장한다.
            if (lastSortValue != null) {
                // searchAfter는 ArrayNode 형식이어야 함
                ObjectMapper mapper = new ObjectMapper();
                this.searchAfter = mapper.createArrayNode().add(lastSortValue);
                log.debug("Updated searchAfter: {}", this.searchAfter);
            }

            // [4] 반환된 레코드는 Connect Worker가 Kafka topic으로 publish
            return records;
        } catch (IOException e) {
            // 일시적인 네트워크/IO 문제는 RetriableException으로 처리해 재시도 가능하게 한다.
            log.warn("An I/O error occurred during the HTTP request. This is likely temporary.", e);
            throw new RetriableException("I/O error during HTTP request.", e);
        } catch (Exception e) {
            // 그 외 예외는 비재시도성 장애로 간주한다.
            log.error("An unexpected error occurred during the HTTP request.", e);
            throw new ConnectException("Unexpected error.");
        }
    }

    @Override
    public void stop() {

    }
}
