package com.ftchinese.jobs.common

import java.io.File
import java.util.Properties

import scala.xml.XML

/**
 * Configuration of jobs.
 * Created by wanbo on 16/3/15.
 */
class JobsConfig {

    var driverSettings = Map[String, Map[String, String]]()

    def parseConf(confProps: Properties): Unit = {
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
    }
}