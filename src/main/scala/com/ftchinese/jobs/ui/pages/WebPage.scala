package com.ftchinese.jobs.ui.pages

import org.eclipse.jetty.server.handler.ContextHandler

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

/**
 * Web page
 * Created by wanbo on 15/8/27.
 */
abstract class WebPage {

    var _tabs = ArrayBuffer[PageTab]()
    var _handlers = ArrayBuffer[ContextHandler]()

    var title = ""
    var content = Seq[Node]()

    // HTML Headers
    var htmlHeaders = Seq[Node]()

    // HTML Footers
    var htmlFooters = Seq[Node]()

    def attachTab(tab: PageTab): Unit ={
        _tabs += tab
    }

    def attachHandler(handler: ContextHandler): Unit ={
        _handlers += handler
    }
}

case class PageTab(name: String, uri: String) {
    protected val _name = name
    protected val _uri  = uri
}