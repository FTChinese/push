package com.ftchinese.jobs.update

import java.io.ByteArrayOutputStream

import akka.actor.Actor
import com.ftchinese.jobs.common._
import com.ftchinese.jobs.database.{AnalyticDB, AnalyticDataSource}
import org.slf4j.MDC


/**
 * The thread of token receiving worker.
 * Created by wanbo on 4/5/16.
 */
class UpdateWorker(conf: JobsConfig) extends Actor with Logging {

    private var _status = "start"

    private var _dbConf = Map[String, String]()

    private var _bufferedDataList = List[UpdateTokenMessage]()

    private val _feedback_tuple_size = 38

    MDC.put("destination", "update")

    def startUpdate(): Unit = {

        try {

            while (_status == "start") {

                val receiveData = connectFeedbackService()

                if(receiveData.length > 0){
                    loadData(receiveData)

                    log.info(_bufferedDataList.size.toString + " tokens was found!")

                    if (_bufferedDataList.nonEmpty) {
                        _bufferedDataList.synchronized {
                            delete(_bufferedDataList)
                            _bufferedDataList = List[UpdateTokenMessage]()
                        }
                    }

                } else {
                    // Slow down the consume speed.
                    Thread.sleep(conf.update_token_intervalMillis)
                }

            }

        } catch {
            case e: Exception => // Ignore
        }

        if (_bufferedDataList.nonEmpty) {
            _bufferedDataList.synchronized {
                delete(_bufferedDataList)
                _bufferedDataList = List[UpdateTokenMessage]()
            }
        }

        if (_status != "start") {
            _status = "stopped"
        }
    }

    private def loadData(data: Array[Byte]): Unit ={
        try {

            if(data.length > 0){
                val tuple = data.sliding(_feedback_tuple_size, _feedback_tuple_size).toList

                log.info(data.length.toString)
                log.info("Found %d tuples.".format(tuple.length))

                tuple.foreach(t => {
                    val firstByte = 0x000000FF & t(0).toInt
                    val secondByte = 0x000000FF & t(1).toInt
                    val thirdByte = 0x000000FF & t(2).toInt
                    val fourthByte = 0x000000FF & t(3).toInt

                    val timestamp = (firstByte << 24 | secondByte << 16 | thirdByte << 8 | fourthByte) & 0xFFFFFFFFL

                    val deviceTokenLength = t(4) << 8 | t(5)

                    log.info("device token length:" + deviceTokenLength)

                    val deviceToken = t.slice(6, _feedback_tuple_size).map(x => {
                        "%02x".format(0x000000FF & x.toInt)
                    }).mkString

                    log.info(deviceToken + "---->" + timestamp)

                    if(deviceToken != "")
                        _bufferedDataList = _bufferedDataList :+ UpdateTokenMessage(deviceToken, timestamp)
                })
            }

        } catch {
            case e: Exception =>
                log.error("Error:" + e)
        }
    }

    private def connectFeedbackService(): Array[Byte] ={

        var data: Array[Byte] = Array[Byte]()

        try {

            // TODO: Different device use different worker to update.
            val updateServer = new UpdateServerManager("phone2", "")

            val socket = updateServer.getPushServer(true)

            val in = socket.getInputStream
            val ooo = new ByteArrayOutputStream()

            var rdBytes = in.read()
            while (rdBytes != -1) {
                ooo.write(rdBytes)
                rdBytes = in.read()
            }

            data = ooo.toByteArray

            log.info("The data size read from feedback service is:" + data.length)

            //data.foreach(x => log.info(x.toInt.toString))

            in.close()

            socket.close()
        } catch {
            case e: Exception =>
                log.error("Error:", e)
        }

        data
    }

    override def receive: Receive = {
        case "Start" =>

            //ZookeeperManager.init(conf)

            _dbConf = conf.fetchDriverConf("analytic", "mysql")

            if(_dbConf.nonEmpty) {

                self ! "consume"
            } else
                log.error("Didn't find database configuration.")

        case "consume" =>
            startUpdate()

        case "ShutDown" =>
            _status = "stop"

            log.info("Received a shutdown message ------------ !")

            context.stop(self)
    }

    def delete(dataList:List[UpdateTokenMessage]) {

        try {

            val ds: AnalyticDataSource = new AnalyticDataSource
            ds.setUrl("jdbc:mysql://" + _dbConf.get("host").get + ":3306/analytic?characterEncoding=utf-8")
            ds.setUsername(_dbConf.get("uname").get)
            ds.setPassword(_dbConf.get("upswd").get)

            val _analytic = new AnalyticDB
            _analytic.setDataSource(ds)

            val batchSize = 30
            val listSize = dataList.size
            var posa = 0
            var posb = 0

            do{
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

                val sqlStr = new StringBuilder("delete from ios_device_token where device_token in (")

                var dot = ""
                tempList.foreach(x => {
                    if(x != null){
                        sqlStr.append(dot)
                        dot = ","
                        sqlStr.append("'")
                        sqlStr.append(x.deviceToken)
                        sqlStr.append("'")
                    }
                })
                sqlStr.append(")")

                log.info("---------------------------------")
                log.info("---sql----:" + sqlStr.toString())
                log.info("---------------------------------")

                _analytic.executeQuery(sqlStr.toString())
            } while (posa < listSize)

        } catch {
            case e:Exception =>
                log.error("Delete data error message:", e)
        }
    }
}
