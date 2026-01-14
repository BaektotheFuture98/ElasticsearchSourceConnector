package com.engine.elasticsearch.elasticsearch;


import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

public class QueryUtils {
    public static JsonNode buildSearchAfterQuery(String baseQuery, String primary_field, String searchAfter) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode query = mapper.readTree(baseQuery);
        if (query.get("sort") == null) {
            ObjectNode node = (ObjectNode) query;

            ArrayNode sortArray = mapper.createArrayNode();
            ObjectNode sortDetails = mapper.createObjectNode();
            ObjectNode orderNode = mapper.createObjectNode();

            orderNode.put("order", "asc");
            sortDetails.set(primary_field, orderNode);
            sortArray.add(sortDetails);

            // set() 메서드로 노드 추가
            node.set("sort", sortArray);
        }

        if (!searchAfter.isEmpty()) {
            ((ObjectNode) query).putArray("search_after").add(searchAfter);
        }
        return query;
    }
}
