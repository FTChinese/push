package com.ftchinese.jobs

import java.util

import com.ftchinese.jobs.common.Logging
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

/**
 * Kafka offsets
 * Created by wanbo on 3/14/16.
 */
class SaveAndSeekOffsets(consumer: KafkaConsumer[String, String], defaultOffset: Long = -1L) extends ConsumerRebalanceListener with Logging {


    /**
     * Read start offset before start to consume.
     * @param collection  Topic with partitions.
     */
    override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
        // Read offset and set the position for consumer.

        val topicPartition = collection.iterator()

        if(defaultOffset > 0) {

            log.info("The consumer was assigned a new offset [%d] ...!".format(defaultOffset))

            while (topicPartition.hasNext) {
                consumer.seek(topicPartition.next(), defaultOffset)
            }
        } else {
            log.info("Read offset from zookeeper!")
        }
    }

    /**
     * Store current offset for next time to consume.
     * @param collection  Topic with partitions.
     */
    override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
        // Save offsets to local storage.
        val topicPartition = collection.iterator()

        while(topicPartition.hasNext) {
            val partition = topicPartition.next()

            val position = consumer.position(partition)

            log.info("The consumer is shutting down, current offset is :" + partition.partition() + "#" + position)
            println(position)
        }
    }
}
