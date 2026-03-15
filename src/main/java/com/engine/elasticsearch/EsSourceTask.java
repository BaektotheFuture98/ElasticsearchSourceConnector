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

        // 무슨 책인지 알아볼 수 있도록 이름표(Partition) 생성
        this.sourcePartition = Collections.singletonMap(ES_FIELD, index);

        // 저장된 offset 값 복구
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
        Thread.sleep(1000);
        try {
            ArrayNode response = client.search(this.searchAfter);

            // 응답이 없으면 빈 리스트 반환 (재시도 방지)
            if (response == null || response.isEmpty()) {
                return new ArrayList<>();
            }

            List<SourceRecord> records = new ArrayList<>();
            JsonNode lastSortValue = null;

            for (JsonNode node : response) {
                JsonNode sortNode = node.get("sort");
                if (sortNode == null || !sortNode.isArray() || sortNode.isEmpty()) {
                    log.warn("Sort field is missing or invalid, skipping record");
                    continue;
                }

                // 마지막 sort 값을 추적 (이것이 다음 offset이 됨)
                JsonNode currentSortValue = sortNode.get(0);

                // offset에 sort 값을 저장 (long 타입으로 변환)
                long sortValueAsLong;
                if (currentSortValue.isNumber()) {
                    sortValueAsLong = currentSortValue.asLong();
                } else {
                    // sort 값이 문자열인 경우 hashCode 사용 (참고: 실제로는 sort 필드의 타입을 명확히 해야 함)
                    sortValueAsLong = currentSortValue.asString().hashCode();
                    log.warn("Sort value is not numeric, using hashCode for offset: {}", sortValueAsLong);
                }

                // 각 레코드마다 offset 업데이트
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

            // 루프 완료 후 다음 페이징을 위해 searchAfter 업데이트
            if (lastSortValue != null) {
                // searchAfter는 ArrayNode 형식이어야 함
                ObjectMapper mapper = new ObjectMapper();
                this.searchAfter = mapper.createArrayNode().add(lastSortValue);
                log.debug("Updated searchAfter: {}", this.searchAfter);
            }

            return records;
        } catch (IOException e) {
            log.warn("An I/O error occurred during the HTTP request. This is likely temporary.", e);
            throw new RetriableException("I/O error during HTTP request.", e);
        } catch (Exception e) {
            log.error("An unexpected error occurred during the HTTP request.", e);
            throw new ConnectException("Unexpected error.");
        }
    }

    @Override
    public void stop() {

    }
}
