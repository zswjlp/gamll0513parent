package com.atguigu.gmall0513.realtime.util

import java.util
import java.util.Objects


import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConversions._


  object MyEsUtil {
    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory:JestClientFactory = null

    /**
      * 获取客户端
      *
      * @return jestclient
      */
    def getClient: JestClient = {
      if (factory == null) build()
      factory.getObject
    }

    /**
      * 关闭客户端
      */
    def close(client: JestClient): Unit = {
      if ( client!=null) try
        client.shutdownClient()
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    /**
      * 建立连接
      */
    private def build(): Unit = {
      factory = new JestClientFactory
      factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
        .maxTotalConnection(20) //连接总数
        .connTimeout(10000).readTimeout(10000).build)

    }

    def insert(source:Any): Unit ={
       val jest: JestClient = getClient
       val stud4 = Stud("li4","male")

       val index: Index = new Index.Builder(source).index("gmall0513_stud").`type`("stud").id("2").build() //代表一次插入动作
       jest.execute(index)
    }

    //Batch   bulk
    def insertBulk( sourceList:List[(String,Any)],indexName:String,typeName:String): Unit ={
      if(sourceList!=null&&sourceList.size>0){
        val jest: JestClient = getClient
        val bulkBuilder = new Bulk.Builder()
        bulkBuilder.defaultIndex(indexName).defaultType(typeName)

        for ( (id,source)<- sourceList ) {
          val index: Index = new Index.Builder(source).id(id).build() //代表一次插入动作
          bulkBuilder.addAction(index)
        }

        val bulk: Bulk =bulkBuilder.build()
        val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
        println(s" 保存= ${items.size()} 条数据!")
      }
    }







    def main(args: Array[String]): Unit = {
//        insert()
    }


    case class Stud( name:String, gender:String)


}
