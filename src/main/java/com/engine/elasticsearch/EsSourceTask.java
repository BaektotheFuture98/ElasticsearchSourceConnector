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
    private Object index;

    private final static String ES_FIELD = "es_index";
    private final static String SEARCH_AFTER_KEY = "search_after";
    private final static String OFFSET_KEY = "offset";

    // [중요] Kafka Connect의 핵심 : 이름표(Partition)와 책갈피(Offset)
    private Map<String, Object> sourcePartition;

    // Elasticsearch 페이징을 위한 searchAfter 값
    private String searchAfter = "";
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
        this.sourcePartition  = Collections.singletonMap(ES_FIELD, index);

        Map<String, Object> lastOffset = context.offsetStorageReader().offset(this.sourcePartition);
        if (lastOffset != null && lastOffset.containsKey(OFFSET_KEY)) {
            // 저장된 searchAfter 값을 문자열로 그대로 복구
            this.offset = (Long) lastOffset.get(OFFSET_KEY);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(1000);
        try {
            ArrayNode response = client.search(this.searchAfter);
            if (response == null || response.isEmpty()) return null;
            List<SourceRecord> records = new ArrayList<>();
            String lastSortValue = null;

            for (JsonNode node : response) {
                JsonNode sortNode = node.get("sort");
                if (sortNode == null || !sortNode.isArray() || sortNode.isEmpty()) {
                    log.warn("Sort field is missing or invalid, skipping record");
                    continue;
                }
                String currentSortValue = sortNode.get(0).asString();
                Map<String, Object> recordOffset = Collections.singletonMap(
                        OFFSET_KEY,
                        this.offset++
                );
                //this(sourcePartition, sourceOffset, topic, (Integer)null, keySchema, key, valueSchema, value);
                SourceRecord sourceRecord = new SourceRecord(
                        this.sourcePartition,
                        recordOffset,
                        this.topic,
                        null,            // partition
                        null,                    // keySchema
                        null,                    // key
                        Schema.STRING_SCHEMA,    // valueSchema
                        node.toString()          // value
                );
                records.add(sourceRecord);
                lastSortValue = currentSortValue;  // 임시 저장만
            }

            // ✅ 루프 완료 후에만 searchAfter 업데이트
            if (lastSortValue != null) {
                this.searchAfter = lastSortValue;
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
