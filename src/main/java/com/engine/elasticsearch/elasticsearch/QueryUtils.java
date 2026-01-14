package com.engine.elasticsearch.elasticsearch;


import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

public class QueryUtils {
    public static JsonNode buildSearchAfterQuery(String baseQuery, String sort, String searchAfter) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode query = mapper.readTree(baseQuery);
        if (query.get("sort") == null && sort != null) {
            ObjectNode node = (ObjectNode) query;

            ArrayNode sortArray = mapper.createArrayNode();
            ObjectNode sortDetails = mapper.createObjectNode();
            ObjectNode orderNode = mapper.createObjectNode();

            orderNode.put("order", "asc");
            sortDetails.set(sort, orderNode);
            sortArray.add(sortDetails);

            // set() 메서드로 노드 추가
            node.set("sort", sortArray);
        }

        if (sort != null) {
            ((ObjectNode) query).putArray("search_after").add(searchAfter);
        }
        return query;
    }
}
