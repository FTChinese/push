package com.ftchinese.jobs

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
 * Message consumer.
 * Created by wanbo on 3/14/16.
 */
class MessageConsumer(kafkaConfig: Properties) {

    kafkaConfig.put("bootstrap.servers", "BJtest6:9091,BJtest6:9092,BJtest6:9093")
    kafkaConfig.put("group.id", "test")
    kafkaConfig.put("enable.auto.commit", "true")
    kafkaConfig.put("auto.commit.interval.ms", "5000")
    kafkaConfig.put("session.timeout.ms", "30000")
    kafkaConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val _consumer = new KafkaConsumer[String, String](kafkaConfig)

    var _currentPartitions: util.Collection[TopicPartition] = null

    _consumer.subscribe(Array("apptoken").toList.asJava, new ConsumerRebalanceListener {
        override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
            _currentPartitions = collection
        }

        override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
            //_consumer.position()
        }
    })


    def consume(f: (String) => Unit): Unit ={
        val records: ConsumerRecords[String, String] = _consumer.poll(100)

        val recordsIterator = records.iterator()

        while (recordsIterator.hasNext) {
            val record = recordsIterator.next()
            f(record.topic())
        }
    }

    def close(): Unit = {
        _consumer.close()
    }
}
