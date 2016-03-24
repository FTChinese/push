package com.ftchinese.jobs

import java.io.{InputStreamReader, BufferedReader, FileInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.security.KeyStore
import java.util.Properties
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import akka.actor.Actor
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
                    var id = ""
                    var lead = ""
                    var badge = "1"
                    var sound = "default"


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
                    if(obj.containsKey("id"))
                        id = obj.getString("id").trim
                    if(obj.containsKey("lead"))
                        lead = obj.getString("lead").trim
                    if(obj.containsKey("badge"))
                        badge = obj.getString("badge").trim
                    if(obj.containsKey("sound"))
                        sound = obj.getString("sound").trim


                    _bufferedDataList = _bufferedDataList :+ NotificationMessage(deviceToken, deviceType, appNumber, timeZone, text, action, id, lead, badge, sound)
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

            try {

                val token = note.deviceToken
                val tokenBytes = token.sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray

                log.info("------------------ Start to send ------------------")

                val payload = new Payload()

                payload.setAlert(note.text)
                payload.setBadge(note.badge)
                payload.setSound(note.sound)

                payload.addCustomDictionary("action", note.action)
                payload.addCustomDictionary("id", note.id)
                payload.addCustomDictionary("lead", note.lead)

                val message = payload.getPayload

                log.info("Payload:" + message)

                val bao = new ByteArrayOutputStream()

                val command: Byte = 1

                bao.write(command)

                bao.write(intTo4ByteArray(1000002))

                bao.write(intTo4ByteArray(3))

                bao.write(intTo2ByteArray(tokenBytes.size))

                bao.write(tokenBytes)

                bao.write(intTo2ByteArray(message.getBytes("UTF-8").size))

                bao.write(message.getBytes("UTF-8"))

                bao.flush()


                val context = SSLContext.getInstance("TLS")
                val keyStore = KeyStore.getInstance("PKCS12")


                keyStore.load(new FileInputStream("../conf/" + "mobile.p12"), Array[Char]())


                val kmf = KeyManagerFactory.getInstance("sunx509")

                kmf.init(keyStore, Array[Char]())



                context.init(kmf.getKeyManagers, null, null)

                val factory = context.getSocketFactory

                val socket = factory.createSocket(AppleServer.pro_host, AppleServer.pro_port)
                socket.setSoTimeout(3000)

                val out = socket.getOutputStream

                out.write(bao.toByteArray)
                out.flush()


                val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
                val ooo = new ByteArrayOutputStream()

                var k = in.read()
                while (k != -1) {
                    ooo.write(k)
                    k = in.read()
                }

                val oooArr = ooo.toByteArray

                oooArr.foreach(println)

                log.info("id:" + ((oooArr(2) << 24) + (oooArr(3) << 16) + (oooArr(4) << 8) + oooArr(5)))

                out.close()
                in.close()

                socket.close()
            } catch {
                case e: Exception =>
                    log.info("Error:", e)
            }
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
