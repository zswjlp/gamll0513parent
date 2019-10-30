package com.atguigu.gmall0513.publisher.service.impl;

import com.atguigu.gmall0513.publisher.mapper.DauMappeer;
import com.atguigu.gmall0513.publisher.mapper.OrderMapper;
import com.atguigu.gmall0513.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMappeer dauMapper;

    @Autowired
    OrderMapper orderMapper;
    @Autowired
    JestClient jestClient;
    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauTotalHours(String date) {
        //变换格式 [{"LH":"11","CT":489},{"LH":"12","CT":123},{"LH":"13","CT":4343}]
        //===》 {"11":383,"12":123,"17":88,"19":200 }
        List<Map> dauListMap = dauMapper.selectDauTotalHours(date);
        Map<String ,Long> dauMap =new HashMap();
        for (Map map : dauListMap) {
            String  lh =(String) map.get("LH");
            Long  ct =(Long) map.get("CT");
            dauMap.put(lh,ct);
        }
        return dauMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHours(String date) {
        //变换格式 [{"C_HOUR":"11","AMOUNT":489.0},{"C_HOUR":"12","AMOUNT":223.0}]
        //===》 {"11":489.0,"12":223.0 }
        Map<String, Double> hourMap=new HashMap<>();
        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        for (Map map : mapList) {
            hourMap.put((String)map.get("C_HOUR"),(Double) map.get("AMOUNT"));
        }

        return hourMap;
    }

    @Override
    public Map getSaleDetailMap(String date, String keyword, int startPage, int pageSize) {
        String query="{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        { \n" +
                "          \"match\": {\n" +
                "            \"sku_name\": {\n" +
                "              \"query\": \"小米手机\",\n" +
                "              \"operator\": \"and\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"dt\": \"2019-10-26\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_gender\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_gender\",\n" +
                "        \"size\": 2\n" +
                "      }\n" +
                "    },\n" +
                "    \"groupby_age\":{\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_age\",\n" +
                "        \"size\": 120\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  ,\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 20\n" +
                "  \n" +
                "}";

        SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
        //查询过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        searchSourceBuilder.query(boolQueryBuilder);
//聚合
        TermsBuilder aggGender = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        TermsBuilder aggAge = AggregationBuilders.terms("groupby_age").field("user_age").size(120);

        searchSourceBuilder.aggregation(aggGender);
        searchSourceBuilder.aggregation(aggAge);

//分页
        searchSourceBuilder.from((startPage-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        Map resultMap=new HashMap();
        try {
            SearchResult result = jestClient.execute(search);
            //明细List
            List<Map> saleDetailList= new ArrayList<>();
            List<SearchResult.Hit<Map,Void>> hits = result.getHits(Map.class);
            for(SearchResult.Hit<Map,Void> hit :hits){
                saleDetailList.add(hit.source);
            }
            // 性别聚合结构
            Map genderMap = new HashMap();
            List<TermsAggregation.Entry> genderBuckets =  result.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry bucket : genderBuckets){
                genderMap.put(bucket.getKey(),bucket.getCount());
            }
            //年龄
            Map ageMap = new HashMap();
            List<TermsAggregation.Entry> ageBuckets = result.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            for (TermsAggregation.Entry bucket : ageBuckets) {
                ageMap.put( bucket.getKey(), bucket.getCount());
            }

            // 查询结果总数
            Long total = result.getTotal();

            resultMap.put("saleDetailList",saleDetailList);
            resultMap.put("genderMap",genderMap);
            resultMap.put("ageMap",ageMap);
            resultMap.put("total",total);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return resultMap;
    }
}
