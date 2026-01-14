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

    private final static String ES_FIELD = "es_record";
    private final static String POSITION_FIELD = "position";

    // 설정값 및 상태를 저장할 변수들
    private EsClient client;
    private String topic;

    private long position = 0L;
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

        // 이 데이터의 출처가 어디인지 이름표를 붙임
        this.sourcePartition  = Collections.singletonMap(ES_FIELD, this.topic);
        // Kafka 내부 저장소에서 이 이름표("ES_FIELD")에 해당하는 마지막 offset을 얻어옴
        this.sourceOffset  = context.offsetStorageReader().offset(this.sourcePartition);

        if (this.sourceOffset != null) { // 읽고자 하는 데이터가 없을때
            log.info("offset found: {}", this.sourceOffset);
            Object lastReadOffset = this.sourceOffset.get(POSITION_FIELD);

            if (lastReadOffset != null) {
                this.position = (Long) lastReadOffset;
            }
        }else {
            this.position = 0L;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(1000); // 폴링 주기 조절을 위해 1초 대기
        List<SourceRecord> records = new ArrayList<>();
        try {
            ArrayNode response = client.search(searchAfter);
            if (response == null || response.size() == 0) {
                log.info("No new records found. Sleeping for a while.");
                return null;
            }else {
                this.searchAfter = response.get(response.size() - 1).get("sort").get(0).toString();
                for (JsonNode node : response) {
                    Map<String, Long> sourceOffset = Collections.singletonMap(ES_FIELD, ++this.position);
                    SourceRecord sourceRecord = new SourceRecord(
                            this.sourcePartition,
                            sourceOffset,
                            this.topic,
                            Schema.STRING_SCHEMA,
                            node.toString()
                    );
                    records.add(sourceRecord);
                }
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
