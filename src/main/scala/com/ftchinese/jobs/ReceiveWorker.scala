package com.ftchinese.jobs

import java.util.Properties

import akka.actor.Actor
import com.alibaba.fastjson.JSON
import com.ftchinese.jobs.common.{TokenMessage, Logging, JobsConfig}
import com.ftchinese.jobs.database.{BeanConfig, AnalyticDataSource, AnalyticDB}
import org.springframework.context.annotation.AnnotationConfigApplicationContext


/**
 * The thread of token receiving worker.
 * Created by wanbo on 16/3/14.
 */
class ReceiveWorker(topics: Array[String], kafkaConf: Properties, conf: JobsConfig) extends Actor with Logging {

    private val _consumer = new MessageConsumer(kafkaConf)

    private var _dbConf = Map[String, String]()

    private var _bufferedDataList = List[TokenMessage]()

    def consumeMessage(): Unit = {

        while (true) {

            _consumer.consume((partition, offset, message) => {

                try {

                    log.info(partition + "###" + offset + "###" + message)

                    val obj = JSON.parseObject(message)



                    var deviceToken = ""
                    var timeZone = ""
                    var status = ""
                    var preference = ""
                    var deviceType = ""
                    var appNumber = "0"
                    var timestamp = ""

                    if(obj.containsKey("deviceToken"))
                        deviceToken = obj.getString("deviceToken").trim
                    if(obj.containsKey("timeZone"))
                        timeZone = obj.getString("timeZone").trim
                    if(obj.containsKey("status"))
                        status = obj.getString("status").trim
                    if(obj.containsKey("preference"))
                        preference = obj.getString("preference").trim
                    if(obj.containsKey("deviceType"))
                        deviceType = obj.getString("deviceType").trim
                    if(obj.containsKey("appNumber"))
                        appNumber = obj.getString("appNumber").trim
                    if(obj.containsKey("timestamp"))
                        timestamp = obj.getString("timestamp").trim


                    _bufferedDataList = _bufferedDataList :+ TokenMessage(deviceToken, timeZone, status, preference, deviceType, appNumber, timestamp)

                    if(_bufferedDataList.size > 0) {
                        _bufferedDataList.synchronized {
                            save(_bufferedDataList)
                            _bufferedDataList = List[TokenMessage]()
                        }
                    }

                    // Slow down the consume speed.
                    Thread.sleep(conf.kafka_consumer_consumeInterval)

                } catch {
                    case e: Exception => // Ignore
                }

            })

            if(_bufferedDataList.size > 0) {
                _bufferedDataList.synchronized {
                    save(_bufferedDataList)
                    _bufferedDataList = List[TokenMessage]()
                }
            }

        }
    }

    override def receive: Receive = {
        case "Start" =>

            _dbConf = conf.fetchDriverConf("analytic", "mysql")

            if(_dbConf.size > 0) {

                // 85792L
                if (conf.kafka_consumer_defaultOffset > 0)
                    _consumer.subscribe(topics, conf.kafka_consumer_defaultOffset)
                else
                    _consumer.subscribe(topics)

                consumeMessage()
            } else
                log.error("Didn't find database configuration.")



        case "ShutDown" =>
            _consumer.close()
            context.stop(self)
    }

    def save(dataList:List[TokenMessage]) {

        try {

            val ctx = new AnnotationConfigApplicationContext(classOf[BeanConfig])

            val ds: AnalyticDataSource = ctx.getBean(classOf[AnalyticDataSource])
            ds.setUrl("jdbc:mysql://" + _dbConf.get("host").get + ":3306/analytic?characterEncoding=utf-8")
            ds.setUsername(_dbConf.get("uname").get)
            ds.setPassword(_dbConf.get("upswd").get)

            val _analytic = ctx.getBean(classOf[AnalyticDB])
            _analytic.setDataSource(ds)

            val batchSize = 30
            val listSize = dataList.size
            var posa = 0
            var posb = 0

            do{
                log.info("---------------------------------")
                // Start batch control
                posb = posa + batchSize

                if(posb >= listSize)
                    posb = listSize

                log.info("posa:" + posa + " posb:" + posb + " batchSize:" + batchSize)

                val tempList = dataList.slice(posa, posb)

                log.info("Slice size:" + tempList.size)

                posa = posa + batchSize

                // End batch control

                val sqlStr = new StringBuilder("replace into ios_device_token values ")

                var dot = ""
                tempList.foreach(x => {
                    if(x != null){
                        sqlStr.append(dot)
                        dot = ","
                        sqlStr.append("('")
                        sqlStr.append(x.deviceToken)
                        sqlStr.append("','")
                        sqlStr.append(x.timeZone)
                        sqlStr.append("','")
                        sqlStr.append(x.status)
                        sqlStr.append("','")
                        sqlStr.append(x.preference)
                        sqlStr.append("','")
                        sqlStr.append(x.deviceType)
                        sqlStr.append("',")
                        sqlStr.append(x.appNumber)
                        sqlStr.append(",")
                        sqlStr.append(x.timestamp)
                        sqlStr.append(")")
                    }
                })

                log.info("---------------------------------")
                log.info("---sql----:" + sqlStr.toString())
                log.info("---------------------------------")

                _analytic.executeQuery(sqlStr.toString())
            } while (posa < listSize)

        } catch {
            case e:Exception =>
                log.error("Save data error message:", e)
                _consumer.close()
        }
    }
}
