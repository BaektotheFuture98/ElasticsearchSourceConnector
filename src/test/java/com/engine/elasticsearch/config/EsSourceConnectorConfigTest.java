package com.engine.elasticsearch.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class EsSourceConnectorConfigTest {
    private Map<String, String> validConfig;

    @Before
    public void setUp() {
        validConfig = new HashMap<>();
        validConfig.put("elasticsearch.hosts", "localhost:9200,localhost:9201");
        validConfig.put("elasticsearch.index", "test-index");
        validConfig.put("elasticsearch.query", "{\"query\": {\"match_all\": {}}}");
        validConfig.put("elasticsearch.sort.field", "timestamp");
        validConfig.put("elasticsearch.query.size", "200");
        validConfig.put("connector.topic", "test-topic");
    }

    /**
     * Test 1: 필수 설정만으로 초기화 성공
     */
    @Test
    public void testValidConfigInitialization() {
        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        assertEquals("localhost:9200,localhost:9201", config.getString("elasticsearch.hosts"));
        assertEquals("test-index", config.getString("elasticsearch.index"));
        assertEquals("test-topic", config.getString("connector.topic"));
    }

    /**
     * Test 2: elasticsearch.hosts 필수 설정 확인
     */
    @Test
    public void testMissingEsHosts() {
        Map<String, String> config = new HashMap<>(validConfig);
        config.remove("elasticsearch.hosts");

        try {
            new EsSourceConnectorConfig(config);
            fail("ConfigException should be thrown");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("elasticsearch.hosts"));
        }
    }

    /**
     * Test 3: elasticsearch.index 필수 설정 확인
     */
    @Test
    public void testMissingEsIndex() {
        Map<String, String> config = new HashMap<>(validConfig);
        config.remove("elasticsearch.index");

        try {
            new EsSourceConnectorConfig(config);
            fail("ConfigException should be thrown");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("elasticsearch.index"));
        }
    }

    /**
     * Test 4: elasticsearch.query 필수 설정 확인
     */
    @Test
    public void testMissingEsQuery() {
        Map<String, String> config = new HashMap<>(validConfig);
        config.remove("elasticsearch.query");

        try {
            new EsSourceConnectorConfig(config);
            fail("ConfigException should be thrown");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("elasticsearch.query"));
        }
    }

    /**
     * Test 5: connector.topic 필수 설정 확인
     */
    @Test
    public void testMissingConnectorTopic() {
        Map<String, String> config = new HashMap<>(validConfig);
        config.remove("connector.topic");

        try {
            new EsSourceConnectorConfig(config);
            fail("ConfigException should be thrown");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("connector.topic"));
        }
    }

    /**
     * Test 6: 선택적 설정 - 사용자 및 비밀번호 없이도 초기화
     */
    @Test
    public void testOptionalUserCredentials() {
        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        String user = config.getString("elasticsearch.user");
        String password = config.getString("elasticsearch.password");

        assertNull(user);
        assertNull(password);
    }

    /**
     * Test 7: 선택적 설정 - 사용자 정보 포함
     */
    @Test
    public void testWithUserCredentials() {
        validConfig.put("elasticsearch.user", "testuser");
        validConfig.put("elasticsearch.password", "testpass");

        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        assertEquals("testuser", config.getString("elasticsearch.user"));
        assertEquals("testpass", config.getString("elasticsearch.password"));
    }

    /**
     * Test 8: 정수형 설정값 읽기 - query.size
     */
    @Test
    public void testIntegerConfigValue() {
        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        int size = config.getInt("elasticsearch.query.size");
        assertEquals(200, size);
    }

    /**
     * Test 9: 기본값 - size가 없는 경우
     */
    @Test
    public void testDefaultSizeValue() {
        Map<String, String> config = new HashMap<>(validConfig);
        config.remove("elasticsearch.query.size");

        EsSourceConnectorConfig esConfig = new EsSourceConnectorConfig(config);
        int size = esConfig.getInt("elasticsearch.query.size");

        // 기본값 200
        assertEquals(200, size);
    }

    /**
     * Test 10: sort.field 선택적 설정
     */
    @Test
    public void testOptionalSortField() {
        Map<String, String> config = new HashMap<>(validConfig);
        config.remove("elasticsearch.sort.field");

        EsSourceConnectorConfig esConfig = new EsSourceConnectorConfig(config);
        String sortField = esConfig.getString("elasticsearch.sort.field");

        assertNull(sortField);
    }

    /**
     * Test 11: primary.key 선택적 설정
     */
    @Test
    public void testOptionalPrimaryKey() {
        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        String primaryKey = config.getString("elasticsearch.primary.key");
        assertNull(primaryKey);
    }

    /**
     * Test 12: 모든 선택적 설정 포함
     */
    @Test
    public void testFullConfigWithAllSettings() {
        validConfig.put("elasticsearch.user", "admin");
        validConfig.put("elasticsearch.password", "secret");
        validConfig.put("elasticsearch.primary.key", "_id");

        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        assertEquals("admin", config.getString("elasticsearch.user"));
        assertEquals("secret", config.getString("elasticsearch.password"));
        assertEquals("_id", config.getString("elasticsearch.primary.key"));
    }

    /**
     * Test 13: ConfigDef 정의 확인
     */
    @Test
    public void testConfigDefStructure() {
        assertNotNull(EsSourceConnectorConfig.CONFIG);

        // CONFIG 객체가 설정 정의를 포함하는지 확인
        // 이것은 AbstractConfig의 내부 구조이므로 직접 테스트는 제한됨
    }

    /**
     * Test 14: 복잡한 쿼리 문자열 읽기
     */
    @Test
    public void testComplexQueryString() {
        String complexQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [{\"match\": {\"status\": \"active\"}}]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        validConfig.put("elasticsearch.query", complexQuery);
        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        String query = config.getString("elasticsearch.query");
        assertNotNull(query);
        assertTrue(query.contains("bool"));
    }

    /**
     * Test 15: 여러 hosts 구성
     */
    @Test
    public void testMultipleElasticsearchHosts() {
        String multipleHosts = "es-node1.example.com:9200,es-node2.example.com:9200,es-node3.example.com:9200";
        validConfig.put("elasticsearch.hosts", multipleHosts);

        EsSourceConnectorConfig config = new EsSourceConnectorConfig(validConfig);

        String hosts = config.getString("elasticsearch.hosts");
        assertEquals(multipleHosts, hosts);
        assertTrue(hosts.contains("es-node1.example.com"));
        assertTrue(hosts.contains("es-node2.example.com"));
        assertTrue(hosts.contains("es-node3.example.com"));
    }
}

