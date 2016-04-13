package com.ftchinese.jobs.ui

import java.util.Properties

import com.ftchinese.jobs.common.{JobsConfig, Logging, NotificationMessage, TaskMessage}
import com.ftchinese.jobs.database.{AnalyticDB, AnalyticDataSource, BeanConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import scala.util.Random

/**
 * Worker
 * Created by wanbo on 16/3/23.
 */
class PushTaskWorker(conf: JobsConfig, taskMessage: TaskMessage) extends Thread with Logging {

    private var _analytic: AnalyticDB = _

    override def run(): Unit ={

        try {

            // Database instance initialization.
            val dbConf = conf.fetchDriverConf("analytic", "mysql")

            val ctx = new AnnotationConfigApplicationContext(classOf[BeanConfig])

            val ds: AnalyticDataSource = ctx.getBean(classOf[AnalyticDataSource])
            ds.setUrl("jdbc:mysql://" + dbConf.get("host").get + ":3306/analytic?characterEncoding=utf-8")
            ds.setUsername(dbConf.get("uname").get)
            ds.setPassword(dbConf.get("upswd").get)

            _analytic = ctx.getBean(classOf[AnalyticDB])
            _analytic.setDataSource(ds)

            // Register a task
            PushTaskQueue.push(new PushTask)

            var totalSize = 0

            var batchIndex = 0
            val batchSize  = 100

            val kafkaProps = Map("kafka_topic" -> "push_notification", "kafka_host" -> conf.kafka_bootstrap_servers)
            var nl = produceNotification(batchIndex, batchSize)

            while (nl.nonEmpty && totalSize < 6000) {

                pushToKafka(kafkaProps, nl.map(_.toJson))
                //nl.map(_.toJson).foreach(log.info)

                totalSize += nl.size

                batchIndex = batchIndex + batchSize
                nl = produceNotification(batchIndex, batchSize)
            }

            // Unregister task
            PushTaskQueue.pull()
        } catch {
            case e: Exception =>
                log.error("Error:", e)
        }
    }

    private def produceNotification(batchIndex: Int, batchSize: Int): List[NotificationMessage] = {

        var notificationList: List[NotificationMessage] = List()

        try {

            var dataList = List[Map[String, String]]()

            if (taskMessage.production){
                log.info("-------------[Production mode]-------------")
                dataList = _analytic.getTokens(batchIndex, batchSize)
            } else {
                log.info("-------------[Test mode]-------------")
                dataList = _analytic.getTestTokens(batchIndex, batchSize)
            }

            if (dataList.nonEmpty) {
                notificationList = dataList.map(x => {
                    val device_token = x.getOrElse("device_token", "")
                    val device_type = x.getOrElse("device_type", "")
                    val app_number = x.getOrElse("app_number", "")
                    val timezone = x.getOrElse("timezone", "")

                    val id = generateId

                    NotificationMessage(device_token, device_type, app_number, timezone, taskMessage.message, taskMessage.action, taskMessage.label, "", 1, "default", id, taskMessage.production, taskMessage.createTime)
                })
            }

        } catch {
            case e:Exception =>
                log.error("Read data error message:", e)
        }

        notificationList
    }

    /**
     * Send message data to kafka.
     *
     * for kafka 0.9.0.1
     *
     * @param _conf      Configuration.
     * @param dataList   Batch data.
     */
    def pushToKafka(_conf: Map[String, String], dataList:List[String]): Unit ={
        try {

            val topic = _conf.get("kafka_topic").get

            if (topic == "")
                throw new Exception("The topic of producer is empty.")

            val props = new Properties()
            props.put("bootstrap.servers",  _conf.get("kafka_host").get)
            props.put("acks", "1")
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

            val producer = new KafkaProducer[String, String](props)

            val batchSize = 30
            val listSize = dataList.size
            var posa = 0
            var posb = 0

            do {
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


                tempList.foreach(message => {
                    producer.send(new ProducerRecord[String, String](topic, System.currentTimeMillis().toString, message))
                })

                log.info(tempList.length + " records saved to kafka successful.")
            } while (posa < listSize)

            producer.close()
        } catch {
            case e:Exception =>
                log.error("Save data error message:", e)
        }
    }

    def generateId: Int ={

        val timeStr = System.currentTimeMillis().toString
        val len = timeStr.length

        timeStr.substring(len - 5, len).toInt + Random.nextInt(1000)
    }
}
