package com.ftchinese.jobs.common

import java.util

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

        while (topicPartition.hasNext) {

            val tp = topicPartition.next()

            val storedOffset = ZookeeperManager.getTopicPartitionOffset(tp)

            log.info("Default offset:" + defaultOffset)
            log.info("Stored offset:" + storedOffset)

            if(defaultOffset > 0) {
                log.info("The consumer was assigned the default offset [%s] [%d] [%d] ...!".format(tp.topic(), tp.partition(), defaultOffset))
                consumer.seek(tp, defaultOffset)
            } else if (storedOffset > 0) {
                log.info("The consumer was assigned the stored offset [%s] [%d] [%d] ...!".format(tp.topic(), tp.partition(), storedOffset + 1))
                consumer.seek(tp, storedOffset + 1)
            } else {
                log.info("There is no offset settings, so consume automatically.!")
            }
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

            ZookeeperManager.setTopicPartitionOffset(partition, position)

            log.info("The consumer is shutting down, current offset is :" + partition.partition() + "#" + position)
            //println(position)
        }
    }
}
