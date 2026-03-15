package com.engine.elasticsearch.elasticsearch;

import org.junit.Before;
import org.junit.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;

import static org.junit.Assert.*;

public class QueryUtilsTest {
    private ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper();
    }

    /**
     * Test 1: 기본 쿼리에 sort 필드 추가
     */
    @Test
    public void testAddSortFieldToQuery() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}}";
        String sortField = "timestamp";

        JsonNode result = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, null);

        assertNotNull(result.get("sort"));
        assertTrue(result.get("sort").isArray());
        assertEquals(1, result.get("sort").size());
    }

    /**
     * Test 2: search_after 없이 쿼리 생성
     */
    @Test
    public void testBuildQueryWithoutSearchAfter() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}, \"sort\": [{\"timestamp\": {\"order\": \"asc\"}}]}";

        JsonNode result = QueryUtils.buildSearchAfterQuery(baseQuery, null, null);

        assertNotNull(result.get("sort"));
        assertNull(result.get("search_after"));
    }

    /**
     * Test 3: search_after를 포함한 쿼리 생성 (숫자)
     */
    @Test
    public void testBuildQueryWithNumericSearchAfter() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}, \"sort\": [{\"timestamp\": {\"order\": \"asc\"}}]}";

        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add(1710498600000L);

        JsonNode result = QueryUtils.buildSearchAfterQuery(baseQuery, null, searchAfter);

        assertNotNull(result.get("search_after"));
        assertTrue(result.get("search_after").isArray());
        assertEquals(1710498600000L, result.get("search_after").get(0).asLong());
    }

    /**
     * Test 4: search_after를 포함한 쿼리 생성 (문자열)
     */
    @Test
    public void testBuildQueryWithStringSearchAfter() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}, \"sort\": [{\"_id\": {\"order\": \"asc\"}}]}";

        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add("doc-123");

        JsonNode result = QueryUtils.buildSearchAfterQuery(baseQuery, null, searchAfter);

        assertNotNull(result.get("search_after"));
        assertEquals("doc-123", result.get("search_after").get(0).asString());
    }

    /**
     * Test 5: 빈 search_after는 추가하지 않음
     */
    @Test
    public void testEmptySearchAfterNotAdded() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}}";
        ArrayNode emptySearchAfter = mapper.createArrayNode();

        JsonNode result = QueryUtils.buildSearchAfterQuery(baseQuery, null, emptySearchAfter);

        assertNull(result.get("search_after"));
    }

    /**
     * Test 6: 복잡한 쿼리에 search_after 추가
     */
    @Test
    public void testComplexQueryWithSearchAfter() throws Exception {
        String complexQuery = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [{\"match\": {\"status\": \"active\"}}],\n" +
                "      \"filter\": [{\"range\": {\"age\": {\"gte\": 18}}}]\n" +
                "    }\n" +
                "  },\n" +
                "  \"sort\": [{\"created_at\": {\"order\": \"desc\"}}]\n" +
                "}";

        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add(1710498600000L);

        JsonNode result = QueryUtils.buildSearchAfterQuery(complexQuery, null, searchAfter);

        // 쿼리 구조 유지 확인
        assertNotNull(result.get("query").get("bool"));
        assertNotNull(result.get("search_after"));
        assertEquals(1710498600000L, result.get("search_after").get(0).asLong());
    }

    /**
     * Test 7: sort 필드가 없고 sort 파라미터가 있을 때 추가
     */
    @Test
    public void testAddSortFieldWhenMissing() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}}";
        String sortField = "updated_at";

        JsonNode result = QueryUtils.buildSearchAfterQuery(baseQuery, sortField, null);

        assertNotNull(result.get("sort"));
        assertTrue(result.get("sort").isArray());
        assertNotNull(result.get("sort").get(0).get(sortField));
    }

    /**
     * Test 8: search_after 배열의 여러 요소 (복합 sort)
     */
    @Test
    public void testMultipleSearchAfterValues() throws Exception {
        String baseQuery = "{\"query\": {\"match_all\": {}}, \"sort\": [{\"year\": {\"order\": \"asc\"}}, {\"month\": {\"order\": \"asc\"}}]}";

        ArrayNode searchAfter = mapper.createArrayNode();
        searchAfter.add(2026);
        searchAfter.add(3);

        JsonNode result = QueryUtils.buildSearchAfterQuery(baseQuery, null, searchAfter);

        assertEquals(2, result.get("search_after").size());
        assertEquals(2026, result.get("search_after").get(0).asInt());
        assertEquals(3, result.get("search_after").get(1).asInt());
    }
}

