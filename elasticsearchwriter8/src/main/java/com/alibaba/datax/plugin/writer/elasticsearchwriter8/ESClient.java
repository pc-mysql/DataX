package com.alibaba.datax.plugin.writer.elasticsearchwriter8;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ESClient {
    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    public ElasticsearchClient esClient;


    public ElasticsearchClient getClient() {
        return esClient;
    }

    public void createClient(String endpoint,
                             String user,
                             String passwd,
                             boolean multiThread,
                             int readTimeout,
                             boolean compression,
                             boolean discovery) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        RestClientBuilder restClientBuilder = RestClient
                .builder(
                        HttpHost.create(endpoint));
        restClientBuilder.setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(30000)
                        .setSocketTimeout(60000)
                        .setContentCompressionEnabled(compression));
        //设置身份认证
        if (!("".equals(user) || "".equals(passwd))) {
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(user, passwd));  //es账号密码
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.disableAuthCaching();
                httpClientBuilder.setMaxConnTotal(200);
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        }

        RestClient restClient = restClientBuilder.build();
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        esClient = new ElasticsearchClient(transport);
    }

    public boolean indicesExists(String indexName) throws Exception {
        boolean isIndicesExists = false;
        BooleanResponse exists = esClient.indices().exists(builder -> builder.index(indexName));
        if (exists.value()) {
            isIndicesExists = true;
        } else {
            isIndicesExists = false;
            log.warn(exists.toString());
        }
        return isIndicesExists;
    }

    public boolean deleteIndex(String indexName) throws Exception {
        log.info("delete index " + indexName);

        if (!indicesExists(indexName)) {
            log.info("index cannot found, skip delete " + indexName);
            return true;
        }
        DeleteIndexResponse deleteIndexResponse = esClient.indices().delete(builder -> builder.index(indexName));
        boolean acknowledged = deleteIndexResponse.acknowledged();
        if (acknowledged) {
            return true;
        } else {
            log.warn("index cannot delete " + deleteIndexResponse.toString());
            return false;
        }
    }

    public boolean createIndex(String indexName, String typeName,
                               Object mappings, String settings, boolean dynamic) throws Exception {
        if (!indicesExists(indexName)) {
            log.info("create index " + indexName);
            CreateIndexResponse createIndexResponse = esClient
                    .indices()
                    .create(builder ->
                            builder.index(indexName)
                                    .settings(s -> s.withJson(new StringReader(settings)))
                                    .masterTimeout(tb -> tb.time("5m")));//指定时间
            if (!createIndexResponse.acknowledged()) {
                log.error(createIndexResponse.toString());
            } else {
                log.info(String.format("create [%s] index success", indexName));
            }
        }

        int idx = 0;
        while (idx < 5) {
            if (indicesExists(indexName)) {
                break;
            }
            Thread.sleep(2000);
            idx++;
        }
        if (idx >= 5) {
            return false;
        }

        if (dynamic) {
            log.info("ignore mappings");        //也就是自动mapping，不使用
            return true;
        }
        log.info("create mappings for " + indexName + "  " + mappings);

        PutMappingResponse putMappingResponse = esClient
                .indices()
                .putMapping(
                        builder ->
                                builder.index(indexName)
                                        .withJson(new StringReader((String) mappings))
                                        .masterTimeout(tb -> tb.time("5m")));
        if (!putMappingResponse.acknowledged()) {
            log.error(putMappingResponse.toString());
            return false;
        } else {
            log.info(String.format("create [%s] index success", indexName));
        }
        return true;
    }

    public boolean alias(String indexname, String aliasname, boolean needClean) throws IOException {
        GetAliasResponse getAliasResponse = esClient.indices()
                .getAlias(a -> a.name(aliasname));
        Map<String, IndexAliases> indexAliasesMap = getAliasResponse.result();
        List<String> indicesList = new ArrayList<>();
        //如果有别名
        if (indexAliasesMap != null && !indexAliasesMap.isEmpty()) {
            for (Map.Entry<String, IndexAliases> entry : indexAliasesMap.entrySet()) {
                String tindex = entry.getKey();
                if (indexname.equals(tindex)) {
                    continue;
                }
                if (needClean) {
                    indicesList.add(tindex);
                }
            }
        }
        UpdateAliasesResponse updateAliasesResponse = esClient
                .indices()
                .updateAliases(update -> update
                        .actions(action -> action
                                .remove(remove -> remove
                                        .alias(aliasname)
                                        .indices(indicesList)))
                        .actions(action -> action
                                .add(add -> add
                                        .alias(aliasname)
                                        .indices(indexname))));
        if (!updateAliasesResponse.acknowledged()) {
            log.error(updateAliasesResponse.toString());
            return false;
        }
        return true;
    }

    public BulkResponse bulkInsert(BulkRequest bulkRequest) throws Exception {
        return esClient.bulk(bulkRequest);
    }

    /**
     * 关闭JClient客户端
     */
    public void closeclient() {
        if (esClient != null) {
            esClient.shutdown();
        }
    }
}
