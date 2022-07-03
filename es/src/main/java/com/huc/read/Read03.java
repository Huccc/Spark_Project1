package com.huc.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

public class Read03 {
    public static void main(String[] args) {
        // 1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        // 2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // 3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();





        // 关闭连接
        jestClient.shutdownClient();
    }
}
