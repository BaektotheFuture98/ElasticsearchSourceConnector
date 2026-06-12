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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class EsClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EsClient.class);

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
        HttpHost[] hostList = list.stream()
                .map(String::trim)
                .filter(host -> !host.isEmpty())
                .map(EsClient::parseHost)
                .toArray(HttpHost[]::new);

        log.info(
                "Creating Elasticsearch RestClient: hosts={}, index={}, querySize={}, sort={}, authEnabled={}",
                Arrays.toString(hostList),
                es_index,
                search_size,
                sort,
                es_user != null && !es_user.isEmpty() && es_password != null && !es_password.isEmpty()
        );

        RestClientBuilder builder = RestClient.builder(hostList);
        if (es_user != null && !es_user.isEmpty() && es_password != null && !es_password.isEmpty()) {
            String credentials = es_user + ":" + es_password;
            String encodedBytes = Base64.getEncoder().encodeToString(credentials.getBytes());
            Header[] headers = {
                    new BasicHeader("Authorization", "Basic " + encodedBytes)
            };
            builder.setDefaultHeaders(headers);
        }

        return builder.build();
    }

    static HttpHost parseHost(String host) {
        if (host.startsWith("http://") || host.startsWith("https://")) {
            HttpHost parsed = HttpHost.create(host);
            log.debug("Parsed Elasticsearch host '{}' as {}", host, parsed);
            return parsed;
        }

        String hostname = host;
        int port = 9200;
        int colonIndex = host.lastIndexOf(':');
        if (colonIndex > -1 && colonIndex < host.length() - 1) {
            hostname = host.substring(0, colonIndex);
            port = Integer.parseInt(host.substring(colonIndex + 1));
        }

        HttpHost parsed = new HttpHost(hostname, port, "http");
        log.debug("Parsed Elasticsearch host '{}' as {}", host, parsed);
        return parsed;
    }

    public ArrayNode search(ArrayNode searchAfter) throws Exception {
        Request request = new Request("GET", "/" + es_index + "/_search?size=" + search_size);
        JsonNode queryNode = QueryUtils.buildSearchAfterQuery(es_query, sort, searchAfter);
        request.setEntity(new NStringEntity(queryNode.toString(), ContentType.APPLICATION_JSON));
        log.debug(
                "Executing Elasticsearch search: endpoint={}, index={}, size={}, searchAfter={}, query={}",
                request.getEndpoint(),
                es_index,
                search_size,
                searchAfter,
                queryNode
        );

        Response response = client.performRequest(request);
        if (response == null) {
            log.warn("Elasticsearch returned null response: index={}, searchAfter={}", es_index, searchAfter);
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode responseJson = mapper.readTree(responseBody);
        JsonNode hitsNode = responseJson.get("hits");
        if (hitsNode == null || hitsNode.get("hits") == null || !hitsNode.get("hits").isArray()) {
            log.warn(
                    "Unexpected Elasticsearch response shape: status={}, index={}, body={}",
                    response.getStatusLine(),
                    es_index,
                    responseBody
            );
            return mapper.createArrayNode();
        }

        ArrayNode hits = (ArrayNode) hitsNode.get("hits");
        log.debug(
                "Elasticsearch search completed: status={}, index={}, returnedHits={}, searchAfter={}",
                response.getStatusLine(),
                es_index,
                hits.size(),
                searchAfter
        );
        return hits;
    }

    @Override
    public void close() throws Exception {
        log.info("Closing Elasticsearch RestClient: index={}", es_index);
        client.close();
    }
}
