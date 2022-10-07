package com.alibaba.datax.plugin.writer.elasticsearchwriter8;

public class ESResponseStatus {
    public static boolean successStatus(int status){
        return status/100 == 2;
    }
}
