package com.atguigu.gmall0513.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0513.common.constants.GmallConstant
import com.atguigu.gmall0513.realtime.bean.OrderInfo
import com.atguigu.gmall0513.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._


object OrderApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_ORDER,ssc)
        val orderDstream: DStream[OrderInfo] = inputDstream.map { record =>
            val jsonString: String = record.value()
            val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            val createtimeArr: Array[String] = orderInfo.create_time.split(" ")
            orderInfo.create_date = createtimeArr(0)
            orderInfo.create_hour = createtimeArr(1).split(":")(0)
            val tel3_8: (String, String) = orderInfo.consignee_tel.splitAt(3)
            val front3: String = tel3_8._1 //138****1234
            val back4: String = tel3_8._2.splitAt(4)._2
            orderInfo.consignee_tel = front3 + "****" + back4
            orderInfo
        }

        // 增加一个字段 ，  标识该比订单是否是该用户首次下单
        orderDstream.foreachRDD{rdd=>
            rdd.saveToPhoenix("gmall0513_order_info",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),new Configuration,Some("hadoop102,hadoop103,hadoop104"))
        }
        ssc.start()
        ssc.awaitTermination()
    }
}
