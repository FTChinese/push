package com.ftchinese.jobs.common

import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.server.handler.{ContextHandler, ContextHandlerCollection}
import org.eclipse.jetty.server.{Server, ServerConnector}

import scala.collection.mutable.ArrayBuffer

/**
* An http server.
* Created by wanbo on 15/8/21.
*/
class HttpServer(conf: JobsConfig) extends Logging {

    private var _server: Server = null
    private val _port: Int = conf.serverUIPort

    private var _handlers: ArrayBuffer[ContextHandler] = ArrayBuffer[ContextHandler]()

    def start(): Unit ={
        if(_server != null)
            throw new Exception("Server is already started.")
        else {
            doStart()
        }
    }

    /**
     * Actually start the HTTP server.
     */
    private def doStart(): Unit ={

        // The server
        _server = new Server()

        val connector = new ServerConnector(_server)
        connector.setHost(conf.serverHost)
        connector.setPort(_port)

        _server.addConnector(connector)

        // Set handlers
        if(_handlers.size > 0) {
            val collection = new ContextHandlerCollection

            val gzipHandlers = _handlers.map(h => {
                val gzipHandler = new GzipHandler
                gzipHandler.setHandler(h)
                gzipHandler
            })

            collection.setHandlers(gzipHandlers.toArray)

            _server.setHandler(collection)
        }

        // Start the server
        _server.start()
        _server.join()
    }

    def attachHandler(handler: ContextHandler): Unit ={
        _handlers += handler
    }

    def stop(): Unit ={
        if(_server == null)
            throw new Exception("Server is already stopped.")
        else {
            _server.stop()
            _server = null
        }
    }
}
