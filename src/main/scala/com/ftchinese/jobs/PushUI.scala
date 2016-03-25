package com.ftchinese.jobs

import java.io.FileInputStream
import java.util.Properties

import com.ftchinese.jobs.common.{HttpServer, JobsConfig, Logging}
import com.ftchinese.jobs.ui.handlers.HomePageHandler
import com.ftchinese.jobs.ui.pages.{HomePage, PageTab, WebPage}
import org.eclipse.jetty.server.handler.ResourceHandler
import org.eclipse.jetty.servlet.ServletContextHandler
import org.slf4j.MDC

import scala.collection.mutable.ArrayBuffer

/**
 * The UI of push notification management system.
 * Created by wanbo on 3/22/16.
 */
class PushUI(conf: JobsConfig) extends Logging {
    private val _server = new HttpServer(conf)

    private var _pages = ArrayBuffer[WebPage]()

    private def initialize(): Unit = {

        // Global static
        val resourceHandler = new ResourceHandler
        resourceHandler.setDirectoriesListed(false)
        resourceHandler.setResourceBase("../webapp/static")

        val staticContext = new ServletContextHandler()
        staticContext.setContextPath("/static")
        staticContext.setHandler(resourceHandler)

        _server.attachHandler(staticContext)

        val homePage = new HomePage

        homePage.attachTab(new PageTab("Home", "/home"))

        homePage.attachHandler(new HomePageHandler(conf, "/home", homePage))

        _pages += homePage



        _pages.foreach(p => {
            p._handlers.foreach(h => {
                _server.attachHandler(h)
            })
        })

    }

    initialize()

    def start(): Unit ={
        _server.start()
    }

    def stop(): Unit ={
        _server.stop()
    }
}


object PushUI extends Logging {

    MDC.put("destination", "ui")

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
        kafkaConf.put("group.id", conf.kafka_consumer_groupId)

        val pushUI = new PushUI(conf)

        pushUI.start()

        Runtime.getRuntime.addShutdownHook(new Thread(){
            override def run(): Unit = {

                pushUI.stop()

                log.info("Push UI service has shutdown.")
            }
        })
    }
}
