package com.ftchinese.jobs

import java.util.Properties

import com.ftchinese.jobs.common.{JobsConfig, Logging, NotificationMessage, TaskMessage}
import com.ftchinese.jobs.database.{AnalyticDB, AnalyticDataSource, BeanConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.springframework.context.annotation.AnnotationConfigApplicationContext

/**
 * Worker
 * Created by wanbo on 16/3/23.
 */
class PushTaskWorker(conf: JobsConfig, taskMessage: TaskMessage) extends Thread with Logging {

    override def run(): Unit ={
        PushTaskQueue.push(new PushTask)

        val nl = produceNotification(conf.fetchDriverConf("analytic", "mysql"), taskMessage)

        nl.foreach(x => println(x.toJson))

        val kafkaProps = Map("kafka_topic" -> "push_notification", "kafka_host" -> conf.kafka_bootstrap_servers)

        if(nl.size > 0){
            pushToKafka(kafkaProps, nl.map(_.toJson))
        }

        PushTaskQueue.pull()
    }

    private def produceNotification(conf: Map[String, String], taskMessage: TaskMessage): List[NotificationMessage] = {

        var notificationList: List[NotificationMessage] = List()

        try {

            val ctx = new AnnotationConfigApplicationContext(classOf[BeanConfig])

            val ds: AnalyticDataSource = ctx.getBean(classOf[AnalyticDataSource])
            ds.setUrl("jdbc:mysql://" + conf.get("host").get + ":3306/analytic?characterEncoding=utf-8")
            ds.setUsername(conf.get("uname").get)
            ds.setPassword(conf.get("upswd").get)

            val _analytic = ctx.getBean(classOf[AnalyticDB])
            _analytic.setDataSource(ds)

            val dataList = _analytic.getTokens(0)

            notificationList = dataList.map(x => {
                val device_token = x.getOrElse("device_token", "")
                val device_type = x.getOrElse("device_type", "")
                val app_number = x.getOrElse("app_number", "")
                val timezone = x.getOrElse("timezone", "")
                NotificationMessage(device_token, device_type, app_number, timezone, taskMessage.message, taskMessage.action, taskMessage.id)
            })

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
}
