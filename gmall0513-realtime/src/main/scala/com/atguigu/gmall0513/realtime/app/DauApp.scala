package com.atguigu.gmall0513.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0513.common.constants.GmallConstant
import com.atguigu.gmall0513.realtime.bean.StartUpLog
import com.atguigu.gmall0513.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_STARTUP, ssc)
        //        inputDstream.foreachRDD{rdd=>
        //            println(rdd.map(_.value()).collect().mkString("\n"))
        //        }
        val startUplogDstream: DStream[StartUpLog] = inputDstream.map { record =>
            val jsonString: String = record.value()
            val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
            val formator = new SimpleDateFormat("yyyy-MM-dd HH")
            val dateHour: String = formator.format(new Date(startUpLog.ts))
            val dateHourArr: Array[String] = dateHour.split(" ")
            startUpLog.logDate = dateHourArr(0)
            startUpLog.logHour = dateHourArr(1)
            startUpLog
        }
        //设置数据缓存防止保存慢数据写入快崩溃
        startUplogDstream.cache()
        //3 根据清单进行过滤


        val filteredDstream: DStream[StartUpLog] = startUplogDstream.transform { rdd =>
            //driver 每批次执行一次
            println("过滤前：" + rdd.count())
            val jedis = RedisUtil.getJedisClient // driver
            val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val dauKey = "dau:" + dateString
            val dauMidSet: util.Set[String] = jedis.smembers(dauKey)
            val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)

            val filteredRdd: RDD[StartUpLog] = rdd.filter { startuplog => //executor
                val dauMidSet: util.Set[String] = dauMidBC.value
                val flag: Boolean = dauMidSet.contains(startuplog.mid)
                !flag
            }
            //Connected to the target VM, address: '127.0.0.1:51422', transport: 'socket'
//            jedis.close()
            println("过滤后：" + filteredRdd.count())
            filteredRdd

        }
        //批次内去重 ， 同一批次内，相同mid 只保留第一条 ==> 对相同的mid进行分组，组内进行比较 保留第一条
        val startupDstreamGroupbyMid: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
        val startupRealFilteredDstream: DStream[StartUpLog] = startupDstreamGroupbyMid.flatMap { case (mid, startupItr) =>
            val top1List: List[StartUpLog] = startupItr.toList.sortWith { (startup1, startup2) =>
                startup1.ts < startup2.ts
            }.take(1)
            top1List
        }




        //广播
        //        val jedis: Jedis = RedisUtil.getJedisClient
        //        val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        //        val daukey: String = "dau:"+dateString
        //        val dauMidSet: util.Set[String] = jedis.smembers(daukey)
        //        val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
        //
        //        val filteredRdd: DStream[StartUpLog] = startUplogDstream.filter { startuplog =>
        //            val dauMiDset: util.Set[String] = dauMidBC.value
        //            val flag: Boolean = dauMidSet.contains(startuplog.mid)
        //            !flag
        //        }
        //        filteredRdd

        //把用户访问清单保存到redis中
        startupRealFilteredDstream.foreachRDD { rdd =>
            rdd.foreachPartition { startupItr =>
                //executor 执行一次
                // val jedis = new Jedis("hadoop102",6379)
                val jedis = RedisUtil.getJedisClient
                for (startup <- startupItr) {
                    //executor 反复执行
                    val dateKey: String = "dau:" + startup.logDate
                    jedis.sadd(dateKey,startup.mid)
                }
                jedis.close()
            }
        }
        startupRealFilteredDstream.foreachRDD{rdd=>
            rdd.saveToPhoenix("GMALL0513_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),new Configuration(),Some("hadoop102,hadoop103,hadoop104:2181"))
        }
        //       startUplogDstream.foreachRDD{rdd=>
        //           rdd.foreach(startuplog=>{
        //               val jedis = new Jedis("hadoop102",6379)
        //                val dateKey: String = "dau:"+startuplog.logDate
        //               jedis.sadd(dateKey,startuplog.mid)
        //                jedis.close()
        //
        //
        //           })
        //
        //       }


        ssc.start()
        ssc.awaitTermination()

    }
}
