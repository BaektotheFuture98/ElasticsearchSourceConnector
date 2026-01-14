package com.engine.elasticsearch.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

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
            .define(PRIMARY_FIELD, Type.STRING, null, Importance.MEDIUM, PRIMARY_FIELD_DOC)
            .define(SORT, Type.STRING, null, Importance.HIGH, SORT_DOC)
            .define(SIZE, Type.INT, 200, Importance.LOW, SIZE_DOC)
            .define(ES_QUERY, Type.STRING, Importance.HIGH, ES_QUERY_DOC)
            .define(ES_USER, Type.STRING, null, Importance.LOW, ES_USER_DOC)
            .define(ES_PASSWORD, Type.STRING, null,Importance.LOW, ES_PASSWORD_DOC)
            .define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC);

    public EsSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props); // AbstractConfig는 설정엔진
    }
}

