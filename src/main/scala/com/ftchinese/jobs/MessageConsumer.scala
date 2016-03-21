package com.ftchinese.jobs

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._

/**
 * Message consumer.
 * Created by wanbo on 3/14/16.
 */
class MessageConsumer(kafkaConfig: Properties) {

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

    def consume(f: (Int, Long, String) => Unit): Unit ={
        val records: ConsumerRecords[String, String] = _consumer.poll(100)

        val recordsIterator = records.iterator()

        while (recordsIterator.hasNext) {
            val record = recordsIterator.next()
            f(record.partition(), record.offset(), record.value())
        }
    }

    def close(): Unit = {
        _consumer.close()
    }
}
