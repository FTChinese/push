package com.ftchinese.jobs

import java.io.FileInputStream
import java.util.Properties

import akka.actor.{Props, ActorSystem}
import com.ftchinese.jobs.common.{Logging, JobsConfig}

/**
 * Consume app token message from kafka.
 * Created by wanbo on 3/14/16.
 */
object ReceiveToken extends Logging {

    protected val conf = new JobsConfig

    def main(args: Array[String]) {

        try {

            val confProps = new Properties()
            val configFile = System.getProperty("easy.conf", "config.properties")
            confProps.load(new FileInputStream(configFile))

            conf.parseConf(confProps)

        } catch {
            case e: Exception =>
                e.printStackTrace()
        }

        val kafkaConf = new Properties()

        kafkaConf.put("bootstrap.servers", "SIG01:9091,SIG01:9092,SIG01:9093")
        //kafkaConf.put("group.id", "test")
        kafkaConf.put("group.id", "ftc-push")

        val topics = Array("apptoken")

        val system = ActorSystem("System")

        val worker = system.actorOf(Props(new ReceiveWorker(topics, kafkaConf, conf)))

        worker ! "Start"

        Runtime.getRuntime.addShutdownHook(new Thread(){
            override def run(): Unit = {

                worker ! "ShutDown"
                try {
                    Thread.sleep(10000)
                } catch {
                    case e: Exception => // Ignore
                }
            }
        })
    }
}
