package com.atguigu.gmall0513.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    public Double selectOrderAmount(String date);
    public List<Map> selectOrderAmountHour(String date);

}
