package test;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.alibaba.datax.plugin.writer.elasticsearchwriter8.ESClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ESClientTest {

    ESClient esClient;

    @Before
    public void createClient(){
        esClient = new ESClient();
        esClient.createClient("http://172.29.224.1:9200","elastic","cKa*swE3Fz3C=_lO8Z-S",true, 5000,true,true);
    }

    @Test
    public void existIndex() throws Exception {
        System.out.println(esClient.indicesExists("testwow"));
    }

    @Test
    public void deleteIndex() throws Exception {
        System.out.println(esClient.deleteIndex("testwow"));
    }

    @Test
    public void createIndex() throws Exception {
        String mappings = "{\"properties\":{\"col_date\":{\"type\":\"date\"},\"col_integer\":{\"type\":\"integer\"},\"col_keyword\":{\"type\":\"keyword\"},\"col_text\":{\"type\":\"text\"},\"col_double\":{\"type\":\"double\"},\"col_long\":{\"type\":\"long\"}}}";
        String settings = "{\"index\":{\"number_of_shards\":1,\"number_of_replicas\":0}}";
        System.out.println(esClient.createIndex("testwow","_doc",mappings,settings,false));
    }

    @Test
    public void bulkInsert() throws IOException {
        Map<String, Object> data1 = new HashMap<String, Object>();
        data1.put("col_date","1989-06-04T01:00:00.000+09:00");
        data1.put("col_integer",121);
        data1.put("col_keyword","wow");
        data1.put("col_text","test");
        data1.put("col_double",1.1);
        data1.put("col_long",19890604);

        Map<String, Object>  data2 = new HashMap<String, Object>();
        data2.put("col_date","1989-06-04T01:00:00.000+09:00");
        data2.put("col_integer","www11");
        data2.put("col_keyword","wow");
        data2.put("col_text","test");
        data2.put("col_double",1.1);
        data2.put("col_long",19890604);
        List<BulkOperation> bulkOperationList = new ArrayList<>();
        BulkOperation build = new BulkOperation.Builder().index(new IndexOperation.Builder<>().document(data1).build()).build();
        bulkOperationList.add(BulkOperation.of(b->b.index(c->c.document(data1))));
        bulkOperationList.add(BulkOperation.of(b->b.index(c->c.document(data2))));
        BulkResponse bulkResponse = esClient.getClient().bulk(x -> x
                .index("testwow")
                .operations(bulkOperationList));
        List<BulkResponseItem> items = bulkResponse.items();
        for (BulkResponseItem item :
                items) {
            System.out.println(item.toString());
        }
    }

    @After
    public void closeClient(){
        if (esClient!=null){
            esClient.closeclient();
        }
    }
}
