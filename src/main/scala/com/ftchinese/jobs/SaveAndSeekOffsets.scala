package com.ftchinese.jobs

import java.util

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

/**
 * Kafka offsets
 * Created by wanbo on 3/14/16.
 */
class SaveAndSeekOffsets(consumer: KafkaConsumer[String, String]) extends ConsumerRebalanceListener{
    override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
        // Save offsets to local storage.
    }

    override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
        // Read offset and set the position for consumer.
        val iterator = collection.iterator()
        while(iterator.hasNext){
//            consumer.seekToBeginning(iterator.next())
            consumer.seek(iterator.next(), 112220687L)
        }
    }
}
