package com.engine.elasticsearch.elasticsearch;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

public class QueryUtils {
    public static JsonNode buildSearchAfterQuery(String baseQuery, String sort, String searchAfter) {
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

        // 2. search_after 추가 (빈 문자열이 아닐 때만!) ✅
        if (searchAfter != null && !searchAfter.isEmpty()) {
            ((ObjectNode) query).putArray("search_after").add(searchAfter);
        }

        return query;
    }
}