package com.engine.elasticsearch.elasticsearch;


import com.engine.elasticsearch.config.EsSourceConnectorConfig;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;


import java.util.Arrays;
import java.util.Base64;
import java.util.List;


public class EsClient implements AutoCloseable {
    private final String es_hosts;
    private final String es_index;
    private final String es_query;
    private final String es_user;
    private final String es_password;
    private final int search_size;
    private final String sort;

    private RestClient client;

    public EsClient(EsSourceConnectorConfig config) {
        this.es_hosts = config.getString(EsSourceConnectorConfig.ES_SERVERS);
        this.es_index = config.getString(EsSourceConnectorConfig.ES_INDEX);
        this.es_query = config.getString(EsSourceConnectorConfig.ES_QUERY);
        this.es_user = config.getString(EsSourceConnectorConfig.ES_USER);
        this.es_password = config.getString(EsSourceConnectorConfig.ES_PASSWORD);
        this.search_size = config.getInt(EsSourceConnectorConfig.SIZE);
        this.sort = config.getString(EsSourceConnectorConfig.SORT);
        this.client = makeClient();
    }
    private RestClient makeClient() {
        List<String> list = Arrays.asList(es_hosts.split(","));
        HttpHost[] hostList = list.stream().map(host ->
                new HttpHost(host, 9200, "http")
        ).toArray(HttpHost[]::new);

        RestClientBuilder builder = RestClient.builder(hostList);

        // 사용자 정보와 비밀번호가 둘 다 있을 때만 인증 헤더 추가
        if (es_user != null && !es_user.isEmpty() && es_password != null && !es_password.isEmpty()) {
            String CREDENTIALS_STRING = es_user + ":" + es_password;
            String encodedBytes = Base64.getEncoder().encodeToString(CREDENTIALS_STRING.getBytes());
            Header[] headers = {
                    new BasicHeader("Authorization", "Basic " + encodedBytes)
            };
            builder.setDefaultHeaders(headers);
        }

        return builder.build();
    }
//    private RestClient makeClient() {
//
//        List<String> list = Arrays.asList(es_hosts.split(","));
//        HttpHost[] hostList = list.stream().map(host ->
//                new HttpHost(host, 9200, "http")
//        ).toArray(HttpHost[]::new);
//
//        String CREDENTIALS_STRING = es_user + ":" + es_password;
//        String encodedBytes = Base64.getEncoder().encodeToString(CREDENTIALS_STRING.getBytes());
//        Header[] headers = {
//                new BasicHeader("Authorization", "Basic " + encodedBytes)
//        };
//        return RestClient.builder(hostList)
//                .setDefaultHeaders(headers)
//                .build();
//    }

    public ArrayNode search(String search_after) throws Exception {
        Request request = new Request("GET", "/" + es_index + "/_search?size=" + search_size);
        JsonNode queryNode = QueryUtils.buildSearchAfterQuery(es_query, sort, search_after);
        request.setEntity(new NStringEntity(queryNode.toString(), ContentType.APPLICATION_JSON));
        Response response = client.performRequest(request);
        if (response == null) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        JsonNode hitshits = mapper.readTree(EntityUtils.toString(response.getEntity()))
                .get("hits").get("hits");
        return (ArrayNode) hitshits;
    }


    @Override
    public void close() throws Exception {
        client.close();
    }
}
