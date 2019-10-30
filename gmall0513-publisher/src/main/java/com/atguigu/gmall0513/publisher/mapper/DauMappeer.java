package com.atguigu.gmall0513.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMappeer {
    //查询某日用户活跃总数
    public Long selectDauTotal(String date);
    //查询某日用户活跃数的分时值
    public List<Map> selectDauTotalHours(String data);
}
