package com.atguigu.gmall0513.realtime.app

import java.text.SimpleDateFormat
import java.util.Date
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0513.common.constants.GmallConstant
import com.atguigu.gmall0513.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall0513.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.util.control.Breaks._


object CouponAlertApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("coupon_alert_app")

        val ssc= new StreamingContext(sparkConf,Seconds(5));

        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_EVENT,ssc)

        //1 开窗
        //2 调整结构  => record => case class
        val eventDstream: DStream[EventInfo] = inputDstream.map { record =>
            val jsonString: String = record.value()
            val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
            val formator = new SimpleDateFormat("yyyy-MM-dd HH")
            val dateHour: String = formator.format(new Date(eventInfo.ts))
            val dateHourArr: Array[String] = dateHour.split(" ")
            eventInfo.logDate = dateHourArr(0)
            eventInfo.logHour = dateHourArr(1)
            eventInfo
        }
        val eventWindowsDstream: DStream[EventInfo] = eventDstream.window(Seconds(300),Seconds(10))
        //3.分组
        val eventGroupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventWindowsDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()
        //4 筛选
        //    a 三次几以上领取优惠券   //在组内根据mid的事件集合进行 过滤筛选
        //    b 用不同账号
        //    c  在过程中没有浏览商品

        // 1 判断出来是否达到预警的要求 2 如果达到要求组织预警的信息
        val alertDstream: DStream[(Boolean, CouponAlertInfo)] = eventGroupbyMidDstream.map { case (mid, eventInfoItr) =>
            var ifAlert = true;
            // 登录过的uid
            val uidSet = new util.HashSet[String]()
            //  领取的商品Id
            val itemIdSet = new util.HashSet[String]()
            //  做过哪些行为
            val eventList = new util.ArrayList[String]()
            breakable(
                for (eventInfo: EventInfo <- eventInfoItr) {
                    if (eventInfo.evid == "coupon") {
                        uidSet.add(eventInfo.uid)
                        itemIdSet.add(eventInfo.itemid)
                    }
                    eventList.add(eventInfo.evid)
                    if (eventInfo.evid == "clickItem") {
                        ifAlert = false
                        break
                    }
                }
            )
            if (uidSet.size() < 3) { //超过3个及以上账号登录 符合预警要求
                ifAlert = false;
            }

            (ifAlert, CouponAlertInfo(mid, uidSet, itemIdSet, eventList, System.currentTimeMillis()))

        }
//        alertDstream.foreachRDD{rdd=>
//            println(rdd.collect().mkString("\n"))
//        }

        //过滤
        val filterAlertDstream: DStream[(Boolean, CouponAlertInfo)] = alertDstream.filter(_._1 )
        //  转换结构(ifAlert,alerInfo) =>（ mid_minu  ,  alerInfo）
        val alertInfoWithIdDstream: DStream[(String, CouponAlertInfo)] = filterAlertDstream.map { case (ifAlert, alertInfo) =>
            val uniKey = alertInfo.mid + "_" + alertInfo.ts / 1000 / 60
            (uniKey, alertInfo)
        }

        //5 存储->es   提前建好index  和 mapping
        alertInfoWithIdDstream.foreachRDD{rdd=>
            rdd.foreachPartition{alertInfoItr=>
                val alertList: List[(String, CouponAlertInfo)] = alertInfoItr.toList
                MyEsUtil.insertBulk(alertList ,GmallConstant.ES_INDEX_ALERT,GmallConstant.ES_DEFAULT_TYPE )
            }

        }

        ssc.start()
        ssc.awaitTermination()
    }
}
