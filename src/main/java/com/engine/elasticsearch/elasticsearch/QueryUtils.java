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
        // [Q1] 사용자 baseQuery 파싱
        // 사용자가 넣은 baseQuery(JSON 문자열)를 트리로 파싱한다.
        JsonNode query = mapper.readTree(baseQuery);

        // [Q2] sort 미지정 시 기본 sort를 주입 (search_after 필수 전제)
        // 1) baseQuery에 sort가 없고, 설정 sort 필드가 있으면 기본 asc 정렬을 보강한다.
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

        // [Q3] 이전 배치 마지막 sort를 search_after로 주입
        // 2) 이전 poll의 마지막 sort 값을 search_after로 주입한다.
        //    search_after 배열 구조는 sort 필드 순서/타입과 동일해야 한다.
        if (searchAfter != null && searchAfter.size() > 0) {
            ObjectNode node = (ObjectNode) query;
            node.set("search_after", searchAfter);
        }

        return query;
    }
}
