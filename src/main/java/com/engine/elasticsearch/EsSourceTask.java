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

///  즉시 종료를 해야하나?
public class EsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(EsSourceTask.class);

    // 설정값 및 상태를 저장할 변수들
    private EsClient client;
    private String topic;
    private String index;

    private final static String ES_FIELD = "es_index";
    private final static String OFFSET_VALUE_KEY = "last_search_after_value";
    private String searchAfter = "";

    // [중요] Kafka Connect의 핵심 : 이름표(Partition)와 책갈피(Offset)
    private Map<String, String> sourcePartition;
    private Map<String, Object> sourceOffset;


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
        this.sourceOffset  = context.offsetStorageReader().offset(this.sourcePartition);

        Map<String, Object> lastOffset = context.offsetStorageReader().offset(this.sourcePartition);
        if (lastOffset != null && lastOffset.containsKey(OFFSET_VALUE_KEY)) {
            // 저장된 searchAfter 값을 문자열로 그대로 복구
            this.searchAfter = (String) lastOffset.get(OFFSET_VALUE_KEY);
            log.info("Restored searchAfter offset: {}", this.searchAfter);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(1000); // 폴링 주기 조절을 위해 1초 대기
//        List<SourceRecord> records = new ArrayList<>();
        try {
            // 복구된 searchAfter를 사용하여 쿼리
            ArrayNode response = client.search(this.searchAfter);

            if (response == null || response.size() == 0) return null;

            List<SourceRecord> records = new ArrayList<>();
            for (JsonNode node : response) {
                // 현재 레코드의 sort 값을 추출 (예: [1736863200, "doc1"])
                String currentSortValue = node.get("sort").toString();

                // 3. 오프셋 생성: 읽을 때와 똑같은 OFFSET_VALUE_KEY 사용
                Map<String, String> offset = Collections.singletonMap(OFFSET_VALUE_KEY, currentSortValue);

                SourceRecord sourceRecord = new SourceRecord(
                        this.sourcePartition,
                        offset,
                        this.topic,
                        Schema.STRING_SCHEMA,
                        node.get("_id").asText(), // Message Key로 문서 ID 사용 권장
                        null,
                        node.toString()
                );
                records.add(sourceRecord);

                // 다음 쿼리를 위해 상태 업데이트
                this.searchAfter = currentSortValue;
            }
            return records;
        }catch (IOException e) {
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
