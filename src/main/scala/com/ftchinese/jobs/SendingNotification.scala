package com.ftchinese.jobs

import java.io.FileInputStream
import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.ftchinese.jobs.common.JobsConfig
import com.ftchinese.jobs.push.PushReceiver

/**
 * Consume app token message from kafka.
 * Created by wanbo on 3/14/16.
 */
object SendingNotification {

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

        kafkaConf.put("bootstrap.servers", conf.kafka_bootstrap_servers)
        kafkaConf.put("group.id", conf.kafka_consumer_groupId)

        val topics = Array("push_notification")

        val system = ActorSystem("System")

        val worker = system.actorOf(Props(new PushReceiver(topics, kafkaConf, conf)))

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
