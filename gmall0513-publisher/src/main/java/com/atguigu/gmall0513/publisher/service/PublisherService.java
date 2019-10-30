package com.atguigu.gmall0513.publisher.service;

import java.util.Map;

public interface PublisherService {
    public Long getDauTotal(String date);

    public Map<String,Long> getDauTotalHours(String date);

    public Double getOrderAmount(String date);

    public Map<String,Double> getOrderAmountHours(String date);

    public Map getSaleDetailMap(String date,String keyword,int startPage,int pageSize);
}
