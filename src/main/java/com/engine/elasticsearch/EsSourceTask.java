package com.engine.elasticsearch;

import com.engine.elasticsearch.config.EsSourceConnectorConfig;
import com.engine.elasticsearch.elasticsearch.EsClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(EsSourceTask.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String ES_FIELD = "es_index";
    static final String SEARCH_AFTER_KEY = "search_after";
    static final String LEGACY_OFFSET_KEY = "offset";

    private EsClient client;
    private String topic;
    private String index;
    private String primaryField;
    private Map<String, Object> sourcePartition;
    private ArrayNode searchAfter;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> map) {
        log.info("Starting Elasticsearch SourceTask");
        EsSourceConnectorConfig config = new EsSourceConnectorConfig(map);

        this.topic = config.getString(EsSourceConnectorConfig.TOPIC);
        this.primaryField = config.getString(EsSourceConnectorConfig.PRIMARY_FIELD);
        this.client = new EsClient(config);
        this.index = config.getString(EsSourceConnectorConfig.ES_INDEX);
        this.sourcePartition = Collections.singletonMap(ES_FIELD, index);

        log.info(
                "SourceTask configured: topic={}, index={}, primaryField={}, sourcePartition={}",
                topic,
                index,
                primaryField,
                sourcePartition
        );

        Map<String, Object> lastOffset = context.offsetStorageReader().offset(this.sourcePartition);
        log.debug("Loaded raw source offset for partition {}: {}", this.sourcePartition, lastOffset);
        this.searchAfter = lastOffset == null ? null : restoreSearchAfter(lastOffset);
        log.info("Recovered search_after offset: {}", this.searchAfter);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(1000);
        try {
            log.debug("Polling Elasticsearch: index={}, topic={}, searchAfter={}", index, topic, searchAfter);
            ArrayNode response = client.search(this.searchAfter);
            if (response == null || response.isEmpty()) {
                log.debug("No Elasticsearch hits returned for index={}, searchAfter={}", index, searchAfter);
                return new ArrayList<>();
            }

            List<SourceRecord> records = new ArrayList<>();
            ArrayNode lastSortValue = null;
            int skippedRecords = 0;
            int missingKeyRecords = 0;

            for (JsonNode node : response) {
                JsonNode sortNode = node.get("sort");
                if (sortNode == null || !sortNode.isArray() || sortNode.isEmpty()) {
                    skippedRecords++;
                    log.warn(
                            "Skipping Elasticsearch hit because sort is missing or invalid: index={}, id={}, sort={}",
                            index,
                            getElasticsearchId(node),
                            sortNode
                    );
                    continue;
                }

                ArrayNode currentSearchAfter = (ArrayNode) sortNode;
                Map<String, Object> recordOffset = buildRecordOffset(currentSearchAfter);
                String recordKey = extractRecordKey(node, this.primaryField);
                if (recordKey == null) {
                    missingKeyRecords++;
                    log.warn(
                            "Kafka record key is null: index={}, id={}, primaryField={}, sourcePresent={}",
                            index,
                            getElasticsearchId(node),
                            primaryField,
                            node.has("_source")
                    );
                }

                SourceRecord sourceRecord = new SourceRecord(
                        this.sourcePartition,
                        recordOffset,
                        this.topic,
                        null,
                        Schema.STRING_SCHEMA,
                        recordKey,
                        Schema.STRING_SCHEMA,
                        node.toString()
                );
                records.add(sourceRecord);
                lastSortValue = currentSearchAfter;
                log.debug(
                        "Created SourceRecord: topic={}, key={}, offset={}, valueBytes={}",
                        topic,
                        recordKey,
                        recordOffset,
                        node.toString().length()
                );
            }

            if (lastSortValue != null) {
                this.searchAfter = lastSortValue;
                log.debug("Updated searchAfter: {}", this.searchAfter);
            }

            log.info(
                    "Poll completed: index={}, topic={}, fetchedHits={}, producedRecords={}, skippedRecords={}, missingKeyRecords={}, nextSearchAfter={}",
                    index,
                    topic,
                    response.size(),
                    records.size(),
                    skippedRecords,
                    missingKeyRecords,
                    this.searchAfter
            );
            return records;
        } catch (IOException e) {
            log.warn("Retriable Elasticsearch I/O error: index={}, searchAfter={}", index, searchAfter, e);
            throw new RetriableException("I/O error during HTTP request.", e);
        } catch (Exception e) {
            log.error("Unexpected Elasticsearch polling error: index={}, topic={}, searchAfter={}", index, topic, searchAfter, e);
            throw new ConnectException("Unexpected error.", e);
        }
    }

    @Override
    public void stop() {
        try {
            if (client != null) {
                log.info("Closing Elasticsearch client for index={}", index);
                client.close();
            }
        } catch (Exception e) {
            log.error("Error closing Elasticsearch client", e);
        }
    }

    static Map<String, Object> buildRecordOffset(ArrayNode sortNode) {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SEARCH_AFTER_KEY, sortNode.toString());
        return offset;
    }

    static ArrayNode restoreSearchAfter(Map<String, Object> lastOffset) {
        Object savedSearchAfter = lastOffset.get(SEARCH_AFTER_KEY);
        if (savedSearchAfter != null) {
            log.debug("Restoring search_after offset from key {}: {}", SEARCH_AFTER_KEY, savedSearchAfter);
            return parseSearchAfter(savedSearchAfter);
        }

        Object legacyOffset = lastOffset.get(LEGACY_OFFSET_KEY);
        if (legacyOffset != null) {
            log.warn("Restoring legacy offset key '{}'. Consider resetting offsets after upgrading to search_after offsets.", LEGACY_OFFSET_KEY);
            ArrayNode restored = MAPPER.createArrayNode();
            if (legacyOffset instanceof Number) {
                restored.add(((Number) legacyOffset).longValue());
            } else {
                restored.add(String.valueOf(legacyOffset));
            }
            return restored;
        }

        return null;
    }

    static String extractRecordKey(JsonNode node, String primaryField) {
        String field = primaryField;
        if (field == null || field.trim().isEmpty()) {
            field = EsSourceConnectorConfig.PRIMARY_FIELD_DEFAULT;
        }

        JsonNode keyNode;
        if (EsSourceConnectorConfig.PRIMARY_FIELD_DEFAULT.equals(field)) {
            keyNode = node.get("_id");
        } else {
            JsonNode sourceNode = node.get("_source");
            keyNode = sourceNode == null ? null : sourceNode.get(field);
        }

        if (keyNode == null || keyNode.isNull()) {
            return null;
        }
        return keyNode.asString();
    }

    private static String getElasticsearchId(JsonNode node) {
        JsonNode idNode = node == null ? null : node.get("_id");
        return idNode == null || idNode.isNull() ? null : idNode.asString();
    }

    private static ArrayNode parseSearchAfter(Object savedSearchAfter) {
        try {
            if (savedSearchAfter instanceof ArrayNode) {
                return (ArrayNode) savedSearchAfter;
            }
            JsonNode parsed = MAPPER.readTree(String.valueOf(savedSearchAfter));
            if (parsed != null && parsed.isArray()) {
                return (ArrayNode) parsed;
            }
        } catch (Exception e) {
            log.warn("Could not restore search_after offset: {}", savedSearchAfter, e);
        }
        return null;
    }
}
