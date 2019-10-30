package com.atguigu.gmall0513.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0513.publisher.bean.Option;
import com.atguigu.gmall0513.publisher.bean.Stat;
import com.atguigu.gmall0513.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dateString){
        Long dauTotal = publisherService.getDauTotal(dateString);

        List<Map>  totalList=new ArrayList<>();
        HashMap dauMap = new HashMap();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);


        HashMap midMap = new HashMap();

        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value",323);

        totalList.add(midMap);
        //交易总额
        Double orderAmount = publisherService.getOrderAmount(dateString);
        HashMap orderAmountMap = new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmount);

        totalList.add(orderAmountMap);

        return  JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String dateString){
        if("dau".equals(id)) {
            Map<String, Long> dauTotalHoursTD = publisherService.getDauTotalHours(dateString);
            String yesterday = getYesterday(dateString);
            Map<String, Long> dauTotalHoursYD = publisherService.getDauTotalHours(yesterday);

            Map hourMap = new HashMap();
            hourMap.put("today", dauTotalHoursTD);
            hourMap.put("yesterday", dauTotalHoursYD);

            return JSON.toJSONString(hourMap);
        }else if("order_amount".equals(id)){
            Map<String, Double> orderAmountHoursTD = publisherService.getOrderAmountHours(dateString);
            String yesterday = getYesterday(dateString);
            Map<String, Double> orderAmountHoursYD = publisherService.getOrderAmountHours(yesterday);

            Map hourMap = new HashMap();
            hourMap.put("today", orderAmountHoursTD);
            hourMap.put("yesterday", orderAmountHoursYD);

            return JSON.toJSONString(hourMap);
        }
        return  null;
    }

    private String   getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date todayD = simpleDateFormat.parse(today);
            Date yesterdayD = DateUtils.addDays(todayD, -1);
            String yesterday = simpleDateFormat.format(yesterdayD);
            return  yesterday;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;

    }


    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") int startPage,@RequestParam("size")int size,@RequestParam("keyword")String keyword){
        Map saleDetailMap=publisherService.getSaleDetailMap(date,keyword,startPage,size);
        Long total = (Long) saleDetailMap.get("total");
        List<Map> saleDetailList = (List)saleDetailMap.get("detail");
        Map ageMap = (Map)saleDetailMap.get("ageMap");
        Map genderMap = (Map)saleDetailMap.get("genderMap");

//  genderMap 整理成为  OptionGroup
        Long femaleCount =(Long) genderMap.get("F");
        Long maleCount =(Long) genderMap.get("M");
        double femaleRate = Math.round(femaleCount * 1000D / total) / 10D;
        double maleRate = Math.round(maleCount * 1000D / total) / 10D;
        List<Option> genderOptions=new ArrayList<>();
        genderOptions.add( new Option("男", maleRate));
        genderOptions.add( new Option("女", femaleRate));
        Stat genderstat = new Stat("性别占比", genderOptions);
        //  ageMap 整理成为  OptionGroup

        Long age_20Count=0L;
        Long age20_30Count=0L;
        Long age30_Count=0L;


        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String agekey =(String) entry.getKey();
            int age = Integer.parseInt(agekey);
            Long ageCount =(Long) entry.getValue();
            if(age <20){
                age_20Count+=ageCount;
            }else   if(age>=20&&age<30){
                age20_30Count+=ageCount;
            }else{
                age30_Count+=ageCount;
            }
        }

        Double age_20rate=0D;
        Double age20_30rate=0D;
        Double age30_rate=0D;

        age_20rate = Math.round(age_20Count * 1000D / total) / 10D;
        age20_30rate = Math.round(age20_30Count * 1000D / total) / 10D;
        age30_rate = Math.round(age30_Count * 1000D / total) / 10D;
        List<Option> ageOptions=new ArrayList<>();
        ageOptions.add( new Option("20岁以下",age_20rate));
        ageOptions.add( new Option("20岁到30岁",age20_30rate));
        ageOptions.add( new Option("30岁以上",age30_rate));
        Stat agestat = new Stat("年龄占比", ageOptions);

        List<Stat> statList=new ArrayList<>();
        statList.add(genderstat);
        statList.add(agestat);


        Map resultMap = new HashMap();
        resultMap.put("total",total);
        resultMap.put("stat",statList);
        resultMap.put("detail",saleDetailMap.get("saleDetailList"));

        return JSON.toJSONString(resultMap);
    }

}
