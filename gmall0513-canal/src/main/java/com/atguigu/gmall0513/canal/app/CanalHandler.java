package com.atguigu.gmall0513.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0513.canal.util.MyKafkaSender;
import com.atguigu.gmall0513.common.constants.GmallConstant;

import java.util.List;

public class CanalHandler {
    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }
    public  void  handle(){
        if(tableName.equals("order_info")&&eventType== CanalEntry.EventType.INSERT){
            //遍历行集
            sendRowDataList2Topic(GmallConstant.KAFKA_ORDER);
        }else if(tableName.equals("user_info")&&eventType== CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE){
            sendRowDataList2Topic(GmallConstant.KAFKA_USER);
        }else if(tableName.equals("order_detail")&&eventType== CanalEntry.EventType.INSERT){
            sendRowDataList2Topic(GmallConstant.KAFKA_ORDER_DETAIL);
        }
    }

    public void  sendRowDataList2Topic(String topic ){
        for (CanalEntry.RowData rowData : rowDataList) {
            //列集
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                String name = column.getName();
                String value = column.getValue();
                System.out.println(name +"::"+value);
                jsonObject.put(name,value);
            }
            String rowJson = jsonObject.toJSONString();
            MyKafkaSender.send(topic,rowJson);

        }

    }

}
