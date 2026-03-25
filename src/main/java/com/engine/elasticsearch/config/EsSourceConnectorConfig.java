package com.engine.elasticsearch.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * 커넥터 설정 스키마 정의 클래스.
 * Kafka Connect가 이 ConfigDef를 사용해 필수값/타입/기본값을 검증한다.
 */
public class EsSourceConnectorConfig extends AbstractConfig {

    public static final String ES_SERVERS = "elasticsearch.hosts";
    public static final String ES_SERVERS_DOC = "Elasticsearch host address";

    public static final String ES_INDEX = "elasticsearch.index";
    public static final String ES_INDEX_DOC = "Elasticsearch index to read data from";

    public static final String PRIMARY_FIELD = "elasticsearch.primary.key";
    public static final String PRIMARY_FIELD_DOC = "Unique primary key field in Elasticsearch documents";

    public static final String SORT = "elasticsearch.sort.field";
    public static final String SORT_DOC = "Sort field";

    public static final String SIZE = "elasticsearch.query.size";
    public static final String SIZE_DOC = "Number of records to fetch per query (default: 200)";

    public static final String ES_QUERY = "elasticsearch.query";
    public static final String ES_QUERY_DOC = "Elasticsearch query to fetch data";

    public static final String ES_USER = "elasticsearch.user";
    public static final String ES_USER_DOC = "Elasticsearch username";

    public static final String ES_PASSWORD = "elasticsearch.password";
    public static final String ES_PASSWORD_DOC = "Elasticsearch password";

    public static final String TOPIC = "connector.topic";
    public static final String TOPIC_DOC = "Kafka topic to publish data to";

    public static ConfigDef CONFIG = new ConfigDef()
            .define(ES_SERVERS, Type.STRING, Importance.HIGH, ES_SERVERS_DOC)
            .define(ES_INDEX, Type.STRING, Importance.HIGH, ES_INDEX_DOC)
            .define(ES_QUERY, Type.STRING, Importance.HIGH, ES_QUERY_DOC)
            .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
            .define(ES_USER, Type.STRING, null, Importance.LOW, ES_USER_DOC)
            .define(ES_PASSWORD, Type.STRING, null,Importance.LOW, ES_PASSWORD_DOC)
            // 향후 upsert/중복제거 전략에 사용할 수 있도록 primary key 설정 키를 열어둔다.
            .define(PRIMARY_FIELD, Type.STRING, null, Importance.MEDIUM, PRIMARY_FIELD_DOC)
            .define(SORT, Type.STRING, null, Importance.HIGH, SORT_DOC)
            .define(SIZE, Type.INT, 200, Importance.LOW, SIZE_DOC);

    public EsSourceConnectorConfig(Map<String, String> props) {
        // AbstractConfig가 ConfigDef 기준으로 파싱/검증을 수행한다.
        super(CONFIG, props);
    }
}
