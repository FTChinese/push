package com.ftchinese.jobs

import java.io.FileInputStream
import java.util.Properties

import akka.actor.{ActorSystem, Props}
import com.ftchinese.jobs.common.JobsConfig
import com.ftchinese.jobs.update.UpdateWorker

/**
 * Updating app token from Apple feedback service.
 * Created by wanbo on 4/5/16.
 */
object UpdateToken {

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

        val system = ActorSystem("System")

        val worker = system.actorOf(Props(new UpdateWorker(conf)))

        worker ! "Start"

        Runtime.getRuntime.addShutdownHook(new Thread(){
            override def run(): Unit = {

                worker ! "ShutDown"
                try {
                    Thread.sleep(5000)
                } catch {
                    case e: Exception => // Ignore
                }
            }
        })
    }
}
