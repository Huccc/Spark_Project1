package com.huc.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Read02 {
    public static void main(String[] args) throws IOException {
        // 1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        // 2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // 3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        // 4.查询es中的数据

        // TODO-----------------------------{ }--------------------------------
        // SearchSourceBuilder相当于最外层的花括号
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // TODO-----------------------------bool--------------------------------

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        // TODO-----------------------------term--------------------------------

        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "true");

        // TODO-----------------------------query--------------------------------

        searchSourceBuilder.query(boolQueryBuilder);

        // TODO-----------------------------filter--------------------------------

        boolQueryBuilder.filter(termQueryBuilder);

        // TODO-----------------------------match--------------------------------

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "羽毛球");

        // TODO-----------------------------must--------------------------------

        boolQueryBuilder.must(matchQueryBuilder);

        // TODO-----------------------------aggs.terms--------------------------------

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupByClass").field("class_id");

        // TODO-----------------------------aggs.max--------------------------------

        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("groupByAge").field("age");

        // 嵌套
        // TODO-----------------------------嵌套--------------------------------
        searchSourceBuilder.aggregation(termsAggregationBuilder.subAggregation(maxAggregationBuilder));

        // TODO-----------------------------from--------------------------------
        searchSourceBuilder.from(0);

        // TODO-----------------------------size--------------------------------
        searchSourceBuilder.size(2);


        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();

        SearchResult result = jestClient.execute(search);

        //a.获取命中条数
        System.out.println("total:" + result.getTotal());

        //b.获取hits中的数据内容
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);

        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println("_index:" + hit.index);
            System.out.println("_type:" + hit.type);
            System.out.println("_id" + hit.id);
            System.out.println("_score" + hit.score);

            // 获取数据明细
            Map source = hit.source;

            // 遍历Map集合
            for (Object o : source.keySet()) {
                System.out.println(o + ":" + source.get(o));
            }
        }
        // c.获取聚合组中的数据
        MetricAggregation aggregations = result.getAggregations();
        // 获取同班级下的聚合组数据
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:" + bucket.getKey());
            System.out.println("doc_count:" + bucket.getCount());

            // 获取年龄聚合组数据
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:" + groupByAge.getMax());

        }

        // 关闭连接
        jestClient.shutdownClient();
    }
}























