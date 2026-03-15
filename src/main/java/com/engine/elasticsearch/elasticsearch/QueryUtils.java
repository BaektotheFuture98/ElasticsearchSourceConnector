package com.engine.elasticsearch.elasticsearch;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

public class QueryUtils {
    /**
     * Elasticsearch search_after 쿼리 생성
     *
     * @param baseQuery 기본 쿼리 JSON 문자열
     * @param sort 정렬 필드명
     * @param searchAfter search_after 배열 (ArrayNode)
     * @return 완성된 쿼리 JsonNode
     * @throws Exception JSON 파싱 예외
     */
    public static JsonNode buildSearchAfterQuery(String baseQuery, String sort, ArrayNode searchAfter) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode query = mapper.readTree(baseQuery);

        // 1. sort 필드 추가 (없을 경우)
        if (query.get("sort") == null && sort != null) {
            ObjectNode node = (ObjectNode) query;

            ArrayNode sortArray = mapper.createArrayNode();
            ObjectNode sortDetails = mapper.createObjectNode();
            ObjectNode orderNode = mapper.createObjectNode();

            orderNode.put("order", "asc");
            sortDetails.set(sort, orderNode);
            sortArray.add(sortDetails);

            node.set("sort", sortArray);
        }

        // 2. search_after 배열 추가 (있을 경우)
        // search_after는 sort와 동일한 구조의 배열이어야 함
        if (searchAfter != null && searchAfter.size() > 0) {
            ObjectNode node = (ObjectNode) query;
            node.set("search_after", searchAfter);
        }

        return query;
    }
}