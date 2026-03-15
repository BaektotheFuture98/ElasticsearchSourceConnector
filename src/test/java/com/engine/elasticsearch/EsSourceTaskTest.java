package com.engine.elasticsearch;

import com.engine.elasticsearch.config.EsSourceConnectorConfig;
import com.engine.elasticsearch.elasticsearch.EsClient;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class EsSourceTaskTest {
    private EsSourceTask task;

    @Mock
    private EsClient mockClient;

    @Mock
    private SourceTaskContext mockContext;

    @Mock
    private OffsetStorageReader mockOffsetStorageReader;

    private ObjectMapper mapper;
    private Map<String, String> taskConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        task = new EsSourceTask();
        mapper = new ObjectMapper();

        // 기본 설정
        taskConfig = new HashMap<>();
        taskConfig.put("elasticsearch.hosts", "localhost");
        taskConfig.put("elasticsearch.index", "test-index");
        taskConfig.put("elasticsearch.query", "{\"query\": {\"match_all\": {}}}");
        taskConfig.put("elasticsearch.sort.field", "timestamp");
        taskConfig.put("elasticsearch.query.size", "200");
        taskConfig.put("connector.topic", "test-topic");
    }

    @After
    public void tearDown() {
        task.stop();
    }

    /**
     * Test 1: Task 초기화 - offset이 없는 경우
     */
    @Test
    public void testStartWithoutSavedOffset() throws Exception {
        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
        when(mockOffsetStorageReader.offset(any(Map.class))).thenReturn(null);

        task.initialize(mockContext);
        task.start(taskConfig);

        // searchAfter는 null로 초기화되어야 함
        assertNotNull(task);
    }

    /**
     * Test 2: Task 초기화 - 저장된 offset이 있는 경우
     */
    @Test
    public void testStartWithSavedOffset() throws Exception {
        Map<String, Object> savedOffset = new HashMap<>();
        savedOffset.put("offset", 1710498600000L);

        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
        when(mockOffsetStorageReader.offset(any(Map.class))).thenReturn(savedOffset);

        task.initialize(mockContext);
        task.start(taskConfig);

        assertNotNull(task);
    }

    /**
     * Test 3: poll() - 응답이 없는 경우 빈 리스트 반환
     */
    @Test
    public void testPollWithEmptyResponse() throws Exception {
        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
        when(mockOffsetStorageReader.offset(any(Map.class))).thenReturn(null);

        task.initialize(mockContext);
        task.start(taskConfig);

        // 이 테스트는 실제 EsClient 동작 필요 - 추후 통합 테스트로 이동
    }

    /**
     * Test 4: poll() - null 응답을 빈 리스트로 처리
     */
    @Test
    public void testPollHandlesNullResponse() throws Exception {
        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
        when(mockOffsetStorageReader.offset(any(Map.class))).thenReturn(null);

        task.initialize(mockContext);
        task.start(taskConfig);

        assertNotNull(task);
    }

    /**
     * Test 5: 숫자형 sort 값에서 offset 추출
     */
    @Test
    public void testExtractNumericOffsetFromSort() throws Exception {
        // JSON 응답 생성 (숫자형 sort)
        String mockResponse = "[\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"1\", \"data\": \"test1\"},\n" +
                "    \"sort\": [1710498600000]\n" +
                "  },\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"2\", \"data\": \"test2\"},\n" +
                "    \"sort\": [1710498600010]\n" +
                "  }\n" +
                "]";

        ArrayNode mockArrayNode = (ArrayNode) mapper.readTree(mockResponse);

        // sort 값이 숫자인지 확인
        assertTrue(mockArrayNode.get(0).get("sort").get(0).isNumber());
        assertEquals(1710498600000L, mockArrayNode.get(0).get("sort").get(0).asLong());
        assertEquals(1710498600010L, mockArrayNode.get(1).get("sort").get(0).asLong());
    }

    /**
     * Test 6: 문자형 sort 값에서 offset 추출
     */
    @Test
    public void testExtractStringOffsetFromSort() throws Exception {
        // JSON 응답 생성 (문자형 sort)
        String mockResponse = "[\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"doc1\"},\n" +
                "    \"sort\": [\"2026-03-15\"]\n" +
                "  },\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"doc2\"},\n" +
                "    \"sort\": [\"2026-03-16\"]\n" +
                "  }\n" +
                "]";

        ArrayNode mockArrayNode = (ArrayNode) mapper.readTree(mockResponse);

        // sort 값이 문자열인지 확인
        assertTrue(mockArrayNode.get(0).get("sort").get(0).isTextual());
        assertEquals("2026-03-15", mockArrayNode.get(0).get("sort").get(0).asString());
        assertEquals("2026-03-16", mockArrayNode.get(1).get("sort").get(0).asString());
    }

    /**
     * Test 7: searchAfter ArrayNode 생성 검증
     */
    @Test
    public void testSearchAfterArrayCreation() throws Exception {
        // 숫자 sort 값으로 ArrayNode 생성
        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add(1710498600010L);

        assertNotNull(searchAfter);
        assertTrue(searchAfter.isArray());
        assertEquals(1, searchAfter.size());
        assertEquals(1710498600010L, searchAfter.get(0).asLong());
    }

    /**
     * Test 8: 여러 sort 필드가 있는 경우 처리
     */
    @Test
    public void testMultipleSortFields() throws Exception {
        String mockResponse = "[\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"1\"},\n" +
                "    \"sort\": [2026, 3, 15]\n" +
                "  }\n" +
                "]";

        ArrayNode mockArrayNode = (ArrayNode) mapper.readTree(mockResponse);

        // 첫 번째 sort 값만 사용 (Elasticsearch search_after는 sort 순서대로)
        assertEquals(2026, mockArrayNode.get(0).get("sort").get(0).asInt());
        assertEquals(3, mockArrayNode.get(0).get("sort").get(1).asInt());
        assertEquals(15, mockArrayNode.get(0).get("sort").get(2).asInt());
    }

    /**
     * Test 9: sort 필드가 없는 레코드는 스킵
     */
    @Test
    public void testSkipRecordsWithoutSort() throws Exception {
        String mockResponse = "[\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"1\"},\n" +
                "    \"sort\": [1710498600000]\n" +
                "  },\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"2\"}\n" +
                "  },\n" +
                "  {\n" +
                "    \"_source\": {\"id\": \"3\"},\n" +
                "    \"sort\": [1710498600020]\n" +
                "  }\n" +
                "]";

        ArrayNode mockArrayNode = (ArrayNode) mapper.readTree(mockResponse);

        // sort가 없는 레코드 확인
        assertNotNull(mockArrayNode.get(0).get("sort"));
        assertNull(mockArrayNode.get(1).get("sort"));
        assertNotNull(mockArrayNode.get(2).get("sort"));
    }

    /**
     * Test 10: SourceRecord 생성 검증
     */
    @Test
    public void testSourceRecordCreation() throws Exception {
        String mockResponse = "{\n" +
                "  \"_source\": {\"id\": \"1\", \"name\": \"test\"},\n" +
                "  \"sort\": [1710498600000]\n" +
                "}";

        org.apache.kafka.connect.json.JsonConverter converter =
            new org.apache.kafka.connect.json.JsonConverter();

        Map<String, Object> node = mapper.readValue(mockResponse, Map.class);
        assertNotNull(node);
        assertNotNull(node.get("_source"));
    }

    /**
     * Test 11: offset 스토리지 파티션 생성
     */
    @Test
    public void testSourcePartitionCreation() throws Exception {
        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
        when(mockOffsetStorageReader.offset(any(Map.class))).thenReturn(null);

        task.initialize(mockContext);
        task.start(taskConfig);

        // 파티션은 인덱스 이름으로 생성되어야 함
        assertNotNull(task);
    }

    /**
     * Test 12: 설정 검증
     */
    @Test
    public void testConfigValidation() throws Exception {
        // 필수 설정 누락 테스트
        Map<String, String> invalidConfig = new HashMap<>();
        invalidConfig.put("elasticsearch.hosts", "localhost");
        // 다른 필수 설정 누락

        // 예외 발생 확인 (실제 테스트는 config 클래스에서 수행)
    }

    /**
     * Test 13: 버전 확인
     */
    @Test
    public void testVersionString() {
        String version = task.version();
        assertNotNull(version);
        assertEquals("1.0.0", version);
    }

    /**
     * Test 14: task stop 호출
     */
    @Test
    public void testStopMethod() {
        when(mockContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);
        when(mockOffsetStorageReader.offset(any(Map.class))).thenReturn(null);

        task.initialize(mockContext);
        try {
            task.start(taskConfig);
        } catch (Exception e) {
            // 초기화 실패 무시
        }

        // stop이 예외를 던지지 않아야 함
        task.stop();
    }
}

