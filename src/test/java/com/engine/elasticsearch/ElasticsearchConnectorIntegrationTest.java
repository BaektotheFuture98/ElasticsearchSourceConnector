package com.engine.elasticsearch;

import com.engine.elasticsearch.config.EsSourceConnectorConfig;
import com.engine.elasticsearch.elasticsearch.QueryUtils;
import org.junit.Before;
import org.junit.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * 통합 테스트: 여러 컴포넌트의 협력을 검증
 */
public class ElasticsearchConnectorIntegrationTest {
    private ObjectMapper mapper;
    private EsSourceConnectorConfig config;
    private Map<String, String> taskConfig;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();

        taskConfig = new HashMap<>();
        taskConfig.put("elasticsearch.hosts", "localhost:9200");
        taskConfig.put("elasticsearch.index", "test-index");
        taskConfig.put("elasticsearch.query", "{\"query\": {\"match_all\": {}}}");
        taskConfig.put("elasticsearch.sort.field", "timestamp");
        taskConfig.put("elasticsearch.query.size", "200");
        taskConfig.put("connector.topic", "test-topic");

        config = new EsSourceConnectorConfig(taskConfig);
    }

    /**
     * Test 1: 설정 로드 후 쿼리 생성
     */
    @Test
    public void testConfigLoadAndQueryCreation() throws Exception {
        String baseQuery = config.getString("elasticsearch.query");
        String sortField = config.getString("elasticsearch.sort.field");

        // 쿼리 생성
        JsonNode query = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, null);

        assertNotNull(query);
        assertNotNull(query.get("sort"));
    }

    /**
     * Test 2: 첫 페이지 조회 (search_after 없음)
     */
    @Test
    public void testFirstPageQuery() throws Exception {
        String baseQuery = config.getString("elasticsearch.query");
        String sortField = config.getString("elasticsearch.sort.field");

        // search_after 없이 쿼리 생성
        JsonNode firstPageQuery = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, null);

        assertNull(firstPageQuery.get("search_after"));
        assertNotNull(firstPageQuery.get("sort"));

        // 쿼리가 유효한 JSON인지 확인
        String queryString = firstPageQuery.toString();
        assertNotNull(queryString);
        assertTrue(queryString.length() > 0);
    }

    /**
     * Test 3: 페이징 시뮬레이션 - 첫 페이지 → 두 번째 페이지
     */
    @Test
    public void testPaginationFlow() throws Exception {
        String baseQuery = config.getString("elasticsearch.query");
        String sortField = config.getString("elasticsearch.sort.field");

        // 1단계: 첫 페이지 조회
        JsonNode firstPageQuery = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, null);
        assertNull(firstPageQuery.get("search_after"));

        // 2단계: 응답에서 마지막 sort 값 추출 (시뮬레이션)
        long lastSortValue = 1710498600100L;

        // 3단계: searchAfter 배열 생성
        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add(lastSortValue);

        // 4단계: 두 번째 페이지 쿼리 생성
        JsonNode secondPageQuery = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, searchAfter);

        assertNotNull(secondPageQuery.get("search_after"));
        assertEquals(lastSortValue, secondPageQuery.get("search_after").get(0).asLong());
    }

    /**
     * Test 4: 여러 번의 페이징 반복
     */
    @Test
    public void testMultiplePagePagination() throws Exception {
        String baseQuery = config.getString("elasticsearch.query");
        String sortField = config.getString("elasticsearch.sort.field");

        long[] pageOffsets = {
            1710498600100L,
            1710498600200L,
            1710498600300L,
            1710498600400L,
            1710498600500L
        };

        // 각 페이지마다 쿼리 생성
        for (long offset : pageOffsets) {
            ArrayNode searchAfter = mapper.createArrayNode();
            searchAfter.add(offset);

            JsonNode query = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, searchAfter);

            assertNotNull(query.get("search_after"));
            assertEquals(offset, query.get("search_after").get(0).asLong());
        }
    }

    /**
     * Test 5: 문자열 기반 sort 필드로 페이징
     */
    @Test
    public void testPaginationWithStringSortField() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}}";
        String sortField = "_id";

        // 첫 페이지
        JsonNode firstQuery = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, null);
        assertNotNull(firstQuery.get("sort"));

        // 다음 페이지 (문자열 기반)
        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add("doc-999");

        JsonNode secondQuery = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, searchAfter);
        assertNotNull(secondQuery.get("search_after"));
        assertEquals("doc-999", secondQuery.get("search_after").get(0).asString());
    }

    /**
     * Test 6: 복합 쿼리의 sort 보존
     */
    @Test
    public void testComplexQuerySortPreservation() throws Exception {
        String complexQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [{\"match\": {\"status\": \"active\"}}]\n" +
                "    }\n" +
                "  },\n" +
                "  \"sort\": [{\"created_at\": {\"order\": \"desc\"}}]\n" +
                "}";

        // sort가 이미 있는 경우
        JsonNode query = QueryUtils.buildSearchAfterQuery(complexQuery, null, null);

        // 기존 sort 구조 보존
        assertNotNull(query.get("sort"));
        assertNotNull(query.get("query").get("bool"));
    }

    /**
     * Test 7: 설정값 읽기 - 모든 필수 항목
     */
    @Test
    public void testAllRequiredConfigValues() {
        String hosts = config.getString("elasticsearch.hosts");
        String index = config.getString("elasticsearch.index");
        String query = config.getString("elasticsearch.query");
        String topic = config.getString("connector.topic");
        int size = config.getInt("elasticsearch.query.size");

        assertNotNull(hosts);
        assertNotNull(index);
        assertNotNull(query);
        assertNotNull(topic);
        assertEquals(200, size);
    }

    /**
     * Test 8: 쿼리 JSON 유효성 검증
     */
    @Test
    public void testQueryJsonValidation() throws Exception {
        String baseQuery = config.getString("elasticsearch.query");
        String sortField = config.getString("elasticsearch.sort.field");

        // 쿼리 생성
        JsonNode query = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, null);

        // JSON으로 직렬화 가능한지 확인
        String jsonString = mapper.writeValueAsString(query);

        // 다시 파싱 가능한지 확인
        JsonNode reparsed = mapper.readTree(jsonString);
        assertNotNull(reparsed);
    }

    /**
     * Test 9: offset 저장 및 복구 시뮬레이션
     */
    @Test
    public void testOffsetStorageSimulation() throws Exception {
        // 페이지 1 - offset 저장
        long page1Offset = 1710498600100L;

        // 페이지 2 - 이전 offset으로부터 search_after 생성
        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add(page1Offset);

        String baseQuery = config.getString("elasticsearch.query");
        String sortField = config.getString("elasticsearch.sort.field");

        JsonNode page2Query = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, searchAfter);

        // 페이지 2의 응답에서 새로운 offset 추출
        long page2Offset = 1710498600200L;

        // 이를 저장하고 다시 페이지 3으로
        ArrayNode nextSearchAfter = mapper.createArrayNode();
        nextSearchAfter.add(page2Offset);

        JsonNode page3Query = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, nextSearchAfter);
        assertEquals(page2Offset, page3Query.get("search_after").get(0).asLong());
    }

    /**
     * Test 10: 큰 데이터셋 시뮬레이션
     */
    @Test
    public void testLargeDatasetPagination() throws Exception {
        String baseQuery = config.getString("elasticsearch.query");
        String sortField = config.getString("elasticsearch.sort.field");
        int pageSize = config.getInt("elasticsearch.query.size");

        // 1000만 건의 데이터를 가정 (200개 씩 50,000 페이지)
        int totalRecords = 10_000_000;
        int totalPages = (totalRecords + pageSize - 1) / pageSize;

        assertEquals(50000, totalPages);

        // 최후의 오프셋만 저장되므로 메모리 효율적
        long lastOffset = (long) (totalRecords - 1);
        ArrayNode finalSearchAfter = mapper.createArrayNode();
        finalSearchAfter.add(lastOffset);

        JsonNode lastPageQuery = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, finalSearchAfter);
        assertNotNull(lastPageQuery.get("search_after"));
    }

    /**
     * Test 11: 토픽 이름 검증
     */
    @Test
    public void testKafkaTopicConfiguration() {
        String topic = config.getString("connector.topic");

        assertNotNull(topic);
        assertEquals("test-topic", topic);
        // 토픽 이름은 알파벳, 숫자, -, _, . 만 포함 가능
        assertTrue(topic.matches("[a-zA-Z0-9._-]+"));
    }

    /**
     * Test 12: 인덱스 이름 검증
     */
    @Test
    public void testElasticsearchIndexConfiguration() {
        String index = config.getString("elasticsearch.index");

        assertNotNull(index);
        assertEquals("test-index", index);
        // ES 인덱스 명시적 검증
        assertFalse(index.isEmpty());
        assertTrue(index.length() <= 255);
    }
}

