package com.engine.elasticsearch;

import com.engine.elasticsearch.config.EsSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(EsSourceConnector.class);
    private Map<String, String> config;

    @Override
    public void start(Map<String, String> props) {
        this.config = props;
        log.info("Elasticsearch Source Connector started with properties: {}", props);
        try {
            new EsSourceConnectorConfig(props);
        }catch (ConfigException e) {
            throw new ConnectException(e.getMessage());
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        /**
        *  Task가 2개 이상인 경우 태스크마다 다른 설정값을 줄 때 사용한다.
        *  여기서는 단순히 동일한 설정을 maxTasks 개수만큼 복제해서 반환한다.
        */
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(config);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        try {
            log.info("Elasticsearch Source Connector stopped.");
        }catch (Exception e) {
            log.error("Error closing Elasticsearch client: ", e);
        }
    }

    @Override
    public ConfigDef config() {
        return EsSourceConnectorConfig.CONFIG;
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
