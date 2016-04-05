package com.ftchinese.jobs.update

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader}

import akka.actor.Actor
import com.ftchinese.jobs.common._
import com.ftchinese.jobs.database.{AnalyticDB, AnalyticDataSource, BeanConfig}
import org.slf4j.MDC
import org.springframework.context.annotation.AnnotationConfigApplicationContext


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
                val tuple = data.sliding(_feedback_tuple_size, _feedback_tuple_size)

                log.info("Found %d tuples.".format(tuple.length))

                tuple.foreach(t => {
                    val firstByte = 0x000000FF & t(0).toInt
                    val secondByte = 0x000000FF & t(1).toInt
                    val thirdByte = 0x000000FF & t(2).toInt
                    val fourthByte = 0x000000FF & t(3).toInt

                    val timestamp = (firstByte << 24 | secondByte << 16 | thirdByte << 8 | fourthByte) & 0xFFFFFFFFL

                    val deviceTokenLength = t(4) << 8 | t(5)

                    val deviceToken = t.slice(6, deviceTokenLength).map(x => {
                        "%02x".format(0x000000FF & x.toInt)
                    }).mkString

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

            val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
            val ooo = new ByteArrayOutputStream()

            var k = in.read()
            while (k != -1) {
                ooo.write(k)
                k = in.read()
            }

            data = ooo.toByteArray

            log.info("The data size read from feedback service is:" + data.length)

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

            ZookeeperManager.init(conf)

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

            val ctx = new AnnotationConfigApplicationContext(classOf[BeanConfig])

            val ds: AnalyticDataSource = ctx.getBean(classOf[AnalyticDataSource])
            ds.setUrl("jdbc:mysql://" + _dbConf.get("host").get + ":3306/analytic?characterEncoding=utf-8")
            ds.setUsername(_dbConf.get("uname").get)
            ds.setPassword(_dbConf.get("upswd").get)

            val _analytic = ctx.getBean(classOf[AnalyticDB])
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

                val sqlStr = new StringBuilder("delete from ios_device_token where device_toke in ")

                var dot = ""
                tempList.foreach(x => {
                    if(x != null){
                        sqlStr.append(dot)
                        dot = ","
                        sqlStr.append("('")
                        sqlStr.append(x.deviceToken)
                        sqlStr.append("')")
                    }
                })

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
