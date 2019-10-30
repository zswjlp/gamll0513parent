package com.atguigu.gmall0513.realtime.app
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0513.common.constants.GmallConstant
import com.atguigu.gmall0513.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0513.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
object SaleDetailApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
        val ssc = new StreamingContext(sparkConf,Seconds(5))
        val orderInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_ORDER,ssc)
        val orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_ORDER_DETAIL,ssc)

        //转换结构 record => case class
        val orderDstream: DStream[OrderInfo] = orderInputDstream.map { record =>
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
        val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDstream.map { record =>
            val jsonString: String = record.value()
            val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
            orderDetail
        }
        val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderDstream.map{orderInfo=>(orderInfo.id,orderInfo)}
        val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map{orderDetail=>(orderDetail.order_id,orderDetail)}

        //双流join
        val fulljoinDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)
        val saleDetaiDstream: DStream[SaleDetail] = fulljoinDstream.mapPartitions { fulljoinResultItr =>
            //当前partition 中最终join成功的结果
            val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
            implicit val formats = org.json4s.DefaultFormats
            val jedis: Jedis = RedisUtil.getJedisClient
            for ((orderId, (orderInfoOpt, orderDetailOpt)) <- fulljoinResultItr) {
                if (orderInfoOpt != None) {
                    val orderInfo: OrderInfo = orderInfoOpt.get
                    if (orderDetailOpt != None) {
                        val orderDetail: OrderDetail = orderDetailOpt.get
                        val saleDetail = new SaleDetail(orderInfo, orderDetail)
                        saleDetailList += saleDetail
                    }
                    // 2  把自己的数据写入到缓存中
                    // redis     1 type  string    2 key   order_info:[orderId]  3 value  order_info json
                    val orderInfoKey = "order_info:" + orderInfo.id
                    //把scal对象转成json
                    //  val orderInfoJson: String = JSON.toJSONString()
                    val orderInfoJson: String = Serialization.write(orderInfo)
                    jedis.setex(orderInfoKey, 600, orderInfoJson)
                    // 3  查询缓存中的Detail 是否能跟自己匹配 如果匹配  生成一个saleDetail
                    val orderDetailKey = "orderDetail:" + orderInfo.id
                    val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
                    if (orderDetailSet != null && orderDetailSet.size() > 0) {
                        import scala.collection.JavaConversions._
                        for (orderDetailJson <- orderDetailSet) {
                            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                            val saleDetail = new SaleDetail(orderInfo, orderDetail)
                            saleDetailList += saleDetail
                        }

                    }
                } else {
                    val orderDetail: OrderDetail = orderDetailOpt.get
                    //1 写缓存
                    // redis  type  :  set         key:   order_detail:[orderId]     value :  orderDetailJson 多个
                    val orderDetailKey = "orderDetail:" + orderDetail.order_id
                    val orderDetailJson: String = Serialization.write(orderDetail)
                    jedis.sadd(orderDetailKey, orderDetailJson)
                    jedis.expire(orderDetailKey, 600);

                    //2 从缓存中读取 ，看是否有匹配
                    val orderInfoKey = "order_info:" + orderDetail.order_id
                    val orderInfoJson: String = jedis.get(orderInfoKey)
                    if (orderInfoJson != null) {
                        //缓存里如果存在  进行关联处理 生成saleDetail 放入结果列表
                        val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                        val saleDetail = new SaleDetail(orderInfo, orderDetail)
                        saleDetailList += saleDetail
                    }

                }

            }
            jedis.close()
            saleDetailList.toIterator
        }

        val fullSaleDetailDstream: DStream[SaleDetail] = saleDetaiDstream.mapPartitions { saleDetaiItr =>
            val jedis: Jedis = RedisUtil.getJedisClient
            val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
            for (saleDetail <- saleDetaiItr) {
                val userInfoKey: String = "user_info:" + saleDetail.user_id
                val userInfoJson: String = jedis.get(userInfoKey)
                val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
                saleDetail.mergeUserInfo(userInfo)
                saleDetailList += saleDetail
            }
            jedis.close()
            // 用user_id 去查询 redis中的userInfo  补齐用户的字段
            saleDetailList.toIterator

        }
        //写入到es中
        fullSaleDetailDstream.foreachRDD{rdd=>
            rdd.foreachPartition{saleDetailItr=>
                val saleDetailWithIdItr: Iterator[(String, SaleDetail)] = saleDetailItr.map(saleDetail=>(saleDetail.order_detail_id,saleDetail))
                MyEsUtil.insertBulk(saleDetailWithIdItr.toList,GmallConstant.ES_INDEX_SALE,GmallConstant.ES_DEFAULT_TYPE)

            }

        }


//        fullSaleDetailDstream.foreachRDD{rdd=>
//            println(rdd.collect().mkString("\n"))
//
//
//        }


//        saleDetaiDstream.foreachRDD(rdd=>
//            println(rdd.collect().mkString("\n"))
//        )
//-----------------------同步user_info 到redis 库
        val userInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_USER,ssc)
        val userInfoDstream: DStream[UserInfo] = userInputDstream.map { record =>
            val userInfoJson: String = record.value()
            val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
            userInfo
        }
        userInfoDstream.foreachRDD{rdd=>
            rdd.foreachPartition{userInfoItr=>
                val jedis: Jedis = RedisUtil.getJedisClient
                implicit val formats = org.json4s.DefaultFormats
                for(userInfo<-userInfoItr){
                    val userInfoJson: String = Serialization.write(userInfo)
                    val userInfoKey: String = "user_info:"+userInfo.id
                    jedis.set(userInfoKey,userInfoJson)
                }
                jedis.close()
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
