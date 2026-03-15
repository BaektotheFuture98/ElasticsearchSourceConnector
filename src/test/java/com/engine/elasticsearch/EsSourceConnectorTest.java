package com.engine.elasticsearch;

import com.engine.elasticsearch.config.EsSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class EsSourceConnectorTest {
    private EsSourceConnector connector;
    private Map<String, String> connectorConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        connector = new EsSourceConnector();

        connectorConfig = new HashMap<>();
        connectorConfig.put("elasticsearch.hosts", "localhost:9200");
        connectorConfig.put("elasticsearch.index", "test-index");
        connectorConfig.put("elasticsearch.query", "{\"query\": {\"match_all\": {}}}");
        connectorConfig.put("elasticsearch.sort.field", "timestamp");
        connectorConfig.put("elasticsearch.query.size", "200");
        connectorConfig.put("connector.topic", "test-topic");
    }

    /**
     * Test 1: Connector 시작
     */
    @Test
    public void testConnectorStart() {
        connector.start(connectorConfig);
        assertNotNull(connector);
    }

    /**
     * Test 2: 설정 검증 - 유효한 설정
     */
    @Test
    public void testConfigValidation() {
        connector.start(connectorConfig);
        // 예외가 발생하지 않으면 성공
    }

    /**
     * Test 3: 설정 검증 - 필수 항목 누락
     */
    @Test
    public void testInvalidConfigMissingRequired() {
        Map<String, String> invalidConfig = new HashMap<>();
        invalidConfig.put("elasticsearch.hosts", "localhost");
        // 필수 항목 누락

        try {
            connector.start(invalidConfig);
            fail("ConfigException should be thrown");
        } catch (ConfigException e) {
            // 예상된 동작
        }
    }

    /**
     * Test 4: Task 클래스 반환
     */
    @Test
    public void testTaskClass() {
        connector.start(connectorConfig);
        Class<?> taskClass = connector.taskClass();

        assertNotNull(taskClass);
        assertEquals(EsSourceTask.class, taskClass);
    }

    /**
     * Test 5: 단일 Task 구성
     */
    @Test
    public void testSingleTaskConfig() {
        connector.start(connectorConfig);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertNotNull(taskConfigs);
        assertEquals(1, taskConfigs.size());
        assertEquals(connectorConfig, taskConfigs.get(0));
    }

    /**
     * Test 6: 여러 Task 구성
     */
    @Test
    public void testMultipleTaskConfigs() {
        connector.start(connectorConfig);
        int maxTasks = 3;
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);

        assertNotNull(taskConfigs);
        assertEquals(maxTasks, taskConfigs.size());

        // 모든 Task가 동일한 설정을 가져야 함
        for (Map<String, String> config : taskConfigs) {
            assertEquals(connectorConfig, config);
        }
    }

    /**
     * Test 7: 많은 수의 Task 구성
     */
    @Test
    public void testManyTaskConfigs() {
        connector.start(connectorConfig);
        int maxTasks = 10;
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);

        assertEquals(maxTasks, taskConfigs.size());
    }

    /**
     * Test 8: ConfigDef 반환
     */
    @Test
    public void testConfigDef() {
        connector.start(connectorConfig);
        assertNotNull(connector.config());
        assertEquals(EsSourceConnectorConfig.CONFIG, connector.config());
    }

    /**
     * Test 9: 버전 정보
     */
    @Test
    public void testVersion() {
        connector.start(connectorConfig);
        String version = connector.version();

        assertNotNull(version);
        assertEquals("1.0.0", version);
    }

    /**
     * Test 10: Connector 중지
     */
    @Test
    public void testConnectorStop() {
        connector.start(connectorConfig);

        // stop이 예외를 던지지 않아야 함
        connector.stop();
    }

    /**
     * Test 11: 최대 Task 수가 0인 경우
     */
    @Test
    public void testZeroTaskConfigs() {
        connector.start(connectorConfig);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(0);

        assertNotNull(taskConfigs);
        assertEquals(0, taskConfigs.size());
    }

    /**
     * Test 12: Task 설정이 원본 설정을 변경하지 않음
     */
    @Test
    public void testTaskConfigImmutability() {
        connector.start(connectorConfig);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);

        // Task 설정 수정
        if (taskConfigs.size() > 0) {
            taskConfigs.get(0).put("new.key", "new.value");
        }

        // 원본 설정은 변경되지 않아야 함
        assertFalse(connectorConfig.containsKey("new.key"));
    }

    /**
     * Test 13: 설정 정보 로깅 확인
     */
    @Test
    public void testStartLogging() {
        // start 메서드가 설정을 로깅하는지 확인
        connector.start(connectorConfig);
        // 예외가 발생하지 않으면 로깅이 정상 작동
    }

    /**
     * Test 14: 여러 hosts를 포함한 설정
     */
    @Test
    public void testMultipleHostsConfig() {
        connectorConfig.put("elasticsearch.hosts",
            "es-node1.example.com:9200,es-node2.example.com:9200,es-node3.example.com:9200");

        connector.start(connectorConfig);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        String hosts = taskConfigs.get(0).get("elasticsearch.hosts");
        assertTrue(hosts.contains("es-node1.example.com"));
        assertTrue(hosts.contains("es-node2.example.com"));
        assertTrue(hosts.contains("es-node3.example.com"));
    }

    /**
     * Test 15: 사용자 인증 설정 포함
     */
    @Test
    public void testAuthenticationConfig() {
        connectorConfig.put("elasticsearch.user", "testuser");
        connectorConfig.put("elasticsearch.password", "testpass");

        connector.start(connectorConfig);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertEquals("testuser", taskConfigs.get(0).get("elasticsearch.user"));
        assertEquals("testpass", taskConfigs.get(0).get("elasticsearch.password"));
    }
}

