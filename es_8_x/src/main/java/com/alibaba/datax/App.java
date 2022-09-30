package com.alibaba.datax;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {

        String IP = "127.0.0.1";
        int PORT = 9200;
        String userName = "elastic";

        String passWord = "cKa*swE3Fz3C=_lO8Z-S";
        String url = "127.0.0.1:9200";
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, passWord));  //es账号密码
        RestClientBuilder http = RestClient.builder(
                new HttpHost(IP, PORT, "http")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setMaxConnTotal(200);
            }
        });
        RestClientBuilder restClientBuilder =
                http.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(60000).setContentCompressionEnabled(true);
                    }

                });
        RestClient build = http.build();



        ElasticsearchTransport transport = new RestClientTransport(
                build,
                new JacksonJsonpMapper()
        );

        ElasticsearchClient esClient = new ElasticsearchClient(transport);
        try {
            CreateIndexResponse test_ela = esClient.indices().create(c -> c.index("test_ela"));
            System.out.println(test_ela.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ElasticsearchIndicesClient indices = esClient.indices();
        new CreateIndexRequest.Builder().index("index_test").build();
//        RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost(IP, PORT, "http")).setHttpClientConfigCallback(new RestClientBuilder
//                        .HttpClientConfigCallback() {
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        httpClientBuilder.disableAuthCaching();
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                }));        //使用RestClinet首先创建一个连接，然后使用RestClient进行
//        DeleteRequest twitter = new DeleteRequest("twitter");
//        DeleteResponse delete = client.delete(twitter);
        System.out.println(esClient.toString());
    }
}
