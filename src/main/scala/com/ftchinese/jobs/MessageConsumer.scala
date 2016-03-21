package com.ftchinese.jobs

import java.util.Properties

import com.ftchinese.jobs.common.Logging
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.slf4j.MDC

import scala.collection.JavaConverters._

/**
 * Message consumer.
 * Created by wanbo on 3/14/16.
 */
class MessageConsumer(kafkaConfig: Properties) extends Logging {

    MDC.put("destination", "system")

    private var defaultConf = Map[String, String]()
    defaultConf = defaultConf.+("bootstrap.servers" -> "")
    defaultConf = defaultConf.+("group.id" -> "")
    defaultConf = defaultConf.+("enable.auto.commit" -> "true")
    defaultConf = defaultConf.+("auto.commit.interval.ms" -> "5000")
    defaultConf = defaultConf.+("session.timeout.ms" -> "30000")
    defaultConf = defaultConf.+("key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    defaultConf = defaultConf.+("value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")

    private val _kafkaConf = kafkaConfig

    defaultConf.foreach{case (k,v) =>
        if(!_kafkaConf.containsKey(k))
            _kafkaConf.put(k, v)
    }

    private val _consumer = new KafkaConsumer[String, String](_kafkaConf)


    def subscribe(topics: Array[String], defaultOffset: Long = -1L): Unit ={
        _consumer.subscribe(topics.toList.asJava, new SaveAndSeekOffsets(_consumer, defaultOffset) )
    }

    def consume(f: (List[ConsumerRecord[String, String]]) => Unit): Unit ={
        val records: ConsumerRecords[String, String] = _consumer.poll(10)

        val recordsList = records.iterator().asScala.toList

        if(recordsList.size > 0){
            f(recordsList)
        }
    }

    def close(): Unit = {
        _consumer.close()
    }
}
