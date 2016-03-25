package com.ftchinese.jobs.push

import java.nio.ByteBuffer
import java.util.Properties

import akka.actor.{Actor, Props}
import akka.routing.SmallestMailboxPool
import com.alibaba.fastjson.JSON
import com.ftchinese.jobs.common._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.MDC


/**
 * The thread of token receiving worker.
 * Created by wanbo on 16/3/14.
 */
class PushReceiver(topics: Array[String], kafkaConf: Properties, conf: JobsConfig) extends Actor with Logging {

    private val _consumer = new MessageConsumer(kafkaConf)

    private var _latestConsume = Map[TopicPartition, Long]()

    private var _status = "start"

    private var _bufferedDataList = List[NotificationMessage]()

    val sendingWorkers = context.actorOf(Props(new SendingWorker()).withRouter(SmallestMailboxPool(10)), name = "sending_worker")

    MDC.put("destination", "system")

    def consumeMessage(): Unit = {


        _consumer.consume((recordsList: List[ConsumerRecord[String, String]]) => {

            try {

                recordsList.foreach(record => {

                    val topic = record.topic()
                    val partition = record.partition()
                    val offset = record.offset()
                    val message = record.value()

                    log.info(topic + "###" + partition + "###" + offset + "###" + message)

                    _latestConsume = _latestConsume.updated(new TopicPartition(topic, partition), offset)

                    val obj = JSON.parseObject(message)

                    var deviceToken = ""
                    var deviceType = ""
                    var appNumber = "0"
                    var timeZone = ""
                    var text = ""
                    var action = ""
                    var label = ""
                    var lead = ""
                    var badge = "1"
                    var sound = "default"

                    var id: Int = 0

                    var production = false
                    var createTime: Long = 0


                    if(obj.containsKey("deviceToken"))
                        deviceToken = obj.getString("deviceToken").trim
                    if(obj.containsKey("deviceType"))
                        deviceType = obj.getString("deviceType").trim
                    if(obj.containsKey("appNumber"))
                        appNumber = obj.getString("appNumber").trim
                    if(obj.containsKey("timeZone"))
                        timeZone = obj.getString("timeZone").trim
                    if(obj.containsKey("text"))
                        text = obj.getString("text").trim
                    if(obj.containsKey("action"))
                        action = obj.getString("action").trim
                    if(obj.containsKey("label"))
                        label = obj.getString("label").trim
                    if(obj.containsKey("lead"))
                        lead = obj.getString("lead").trim
                    if(obj.containsKey("badge"))
                        badge = obj.getString("badge").trim
                    if(obj.containsKey("sound"))
                        sound = obj.getString("sound").trim

                    if(obj.containsKey("id"))
                        id = obj.getInteger("id")

                    if(obj.containsKey("production"))
                        production = obj.getBoolean("production")

                    if(obj.containsKey("createTime"))
                        createTime = obj.getLong("createTime")

                    if(obj.containsKey("createTime"))
                        createTime = obj.getLong("createTime")

                    _bufferedDataList = _bufferedDataList :+ NotificationMessage(deviceToken, deviceType, appNumber, timeZone, text, action, label, lead, badge, sound, id, production, createTime)
                })

                if(_bufferedDataList.size > 0) {
                    _bufferedDataList.synchronized {
                        send(_bufferedDataList)
                        _bufferedDataList = List[NotificationMessage]()
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
                send(_bufferedDataList)
                _bufferedDataList = List[NotificationMessage]()
            }
        }

        if (_status == "start") {
            self ! "consume"
        } else {
            _status = "stopped"
        }
    }

    override def receive: Receive = {
        case "Start" =>

            ZookeeperManager.init(conf)

            _consumer.subscribe(topics)

            self ! "consume"


        case "consume" =>
            consumeMessage()

        case "ShutDown" =>
            _status = "stop"
            _consumer.close()

            _latestConsume.foreach{
                case (tp: TopicPartition, offset: Long) =>
                    ZookeeperManager.setTopicPartitionOffset(tp, offset)
            }

            log.info("Received a shutdown message ------------ !")

            context.stop(self)
    }


    /**
     * @todo Need to validate every message expire
     * @param sendList Notification list
     */
    private def send(sendList: List[NotificationMessage]): Unit ={

        sendList.foreach(note => {

            sendingWorkers ! note

        })
    }

    def intTo2ByteArray(value: Int): Array[Byte] ={
        val s1 = (value & 0xFF00) >> 8
        val s2 = value & 0xFF
        Array(s1.toByte, s2.toByte)
    }

    def intTo4ByteArray(value: Int): Array[Byte] ={
        ByteBuffer.allocate(4).putInt(value).array()
    }
}
