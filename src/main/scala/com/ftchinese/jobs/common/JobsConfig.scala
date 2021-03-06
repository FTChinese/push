package com.ftchinese.jobs.common

import java.io.File
import java.util.Properties

import scala.xml.XML

/**
 * Configuration of jobs.
 * Created by wanbo on 16/3/15.
 */
class JobsConfig {

    var serverHost: String = ""

    var serverUIPort: Int = 8080

    var zk_hosts = ""

    var kafka_bootstrap_servers: String = ""

    var kafka_consumer_groupId: String = ""

    var kafka_consumer_defaultOffset: Long = 0

    var kafka_consumer_consumeInterval: Int = 0

    var driverSettings = Map[String, Map[String, String]]()

    var update_token_intervalMillis: Int = 7200000

    def parseConf(confProps: Properties): Unit = {

        serverHost = confProps.getProperty("server.ui.host", "localhost")
        val tmpPort = confProps.getProperty("server.ui.port", "8080")

        if(tmpPort.forall(_.isDigit)){
            try {
                val intPort = tmpPort.toInt
                if(intPort > 0)
                    serverUIPort = intPort
            } catch {
                case e: Exception => // Ignore
            }
        }

        zk_hosts = confProps.getProperty("zookeeper.hosts", "")

        kafka_bootstrap_servers = confProps.getProperty("kafka.bootstrap.servers", "")

        kafka_consumer_groupId = confProps.getProperty("consumer.consume.groupId", "")

        val ofs = confProps.getProperty("consumer.consume.defaultOffset", "0")

        if(ofs.forall(_.isDigit)) {
            val ofsInt = ofs.toLong
            if(ofsInt > 0)
                kafka_consumer_defaultOffset = ofsInt
        }

        kafka_consumer_consumeInterval = confProps.getProperty("consumer.consume.interval", "0").toInt

        val dbConf = confProps.getProperty("database.conf", "database.xml")

        try {
            val confFile = new File("../conf/" + dbConf)

            if(confFile.exists()){
                val xml = XML.loadFile(confFile)

                for (node <- xml \ "property") {
                    val dbType = (node \ "@type").text
                    dbType match {
                        case "mysql" =>

                            val settings = Map(
                                "type" -> dbType,
                                "host" -> (node \\ "host").text,
                                "port" -> (node \\ "port").text,
                                "uname" -> (node \\ "uname").text,
                                "upswd" -> (node \\ "upswd").text,
                                "dbname" -> (node \\ "dbname").text,
                                "writable" -> (node \ "@writable").text
                            )

                            val settingKey = settings.hashCode().toString

                            driverSettings = driverSettings.+(settingKey -> settings)

                        case "hbase" =>

                            val settings = Map("type" -> dbType, "zk" -> (node \\ "zookeeper").text)

                            val settingKey = settings.hashCode().toString

                            driverSettings = driverSettings.+(settingKey -> settings)

                        case _ => // Ignore setup error.
                    }
                }
            }
        } catch {
            case e: Exception => // Ignore the file opening exceptions.
        }

        update_token_intervalMillis = confProps.getProperty("update.token.interval.millis", "7200000").toInt
    }

    def fetchDriverConf(dbName: String = "", dbType: String = "", writable: String = "true"): Map[String, String] = {
        driverSettings.values.filter(x => x.getOrElse("dbname", "") == dbName && x.getOrElse("type", "") == dbType && x.getOrElse("writable", "true") == writable).last
    }
}