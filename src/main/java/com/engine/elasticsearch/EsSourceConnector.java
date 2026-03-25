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

/**
 * Kafka Connect Worker가 가장 먼저 로딩하는 SourceConnector 구현체.
 * - 역할 1) 사용자 설정 검증
 * - 역할 2) Task 클래스/설정 분배
 * - 역할 3) 커넥터 라이프사이클(start/stop) 관리
 */
public class EsSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(EsSourceConnector.class);
    private Map<String, String> config;

    @Override
    public void start(Map<String, String> props) {
        // Connector 단에서 설정을 1차 검증해 Task 시작 전에 실패를 빠르게 노출한다.
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
        /*
         * Task가 2개 이상인 경우 태스크마다 다른 설정값을 줄 수 있다.
         * 현재 구현은 파티셔닝 없이 "동일 설정"을 maxTasks 개수만큼 복제한다.
         * 즉, 스케일 아웃 전략은 아직 단순 복제 형태다.
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
