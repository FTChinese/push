package com.ftchinese.jobs.handlers

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.ftchinese.jobs.common.{JobsConfig, Logging, NotificationMessage}
import com.ftchinese.jobs.database.{AnalyticDB, AnalyticDataSource, BeanConfig}
import com.ftchinese.jobs.pages.{PageTemplate, WebPage}
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler}
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import scala.xml.Node

/**
 * The handler for home page.
 * Created by wanbo on 15/8/27.
 */
class HomePageHandler(conf: JobsConfig, contextPath: String, page: WebPage) extends ContextHandler with Logging {

    val handler = new AbstractHandler {
        override def handle(s: String, request: Request, httpServletRequest: HttpServletRequest, httpServletResponse: HttpServletResponse): Unit = {

            val requestMethod = request.getMethod

            httpServletResponse.setContentType("text/html; charset=utf-8")
            httpServletResponse.setStatus(HttpServletResponse.SC_OK)

            log.info("Response contents ------------ server")

            val out = httpServletResponse.getWriter

            if(requestMethod == "POST") {
                page.title = "Home"


                val parameters = request.getParameterMap

                val message = parameters.getOrDefault("message", Array("")).apply(0)
                val action = parameters.getOrDefault("action", Array("")).apply(0)
                val id = parameters.getOrDefault("id", Array("")).apply(0)

                log.info("Receive a message: %s, action: %s, id: %s".format(message, action, id))

                if(message.isEmpty || action.isEmpty || id.isEmpty){
                    page.content = failed()
                    out.println(PageTemplate.commonNavigationPage(page))
                } else {

                    // Do some work
                    // Need to create task queue, new task will push into the queue.
                    // Keep each task is unique in the queue.
                    // Every task in the queue will running in a new thread.
                    val nl = produceNotification(conf.fetchDriverConf("analytic", "mysql"), (message, action, id))

                    println(nl)
                    nl.foreach(x => println(x.toJson))

                    page.content = success()
                    out.println(PageTemplate.commonNavigationPage(page))
                }
            } else {
                page.title = "Home"
                page.content = makeTable()

                out.println(PageTemplate.commonNavigationPage(page))
            }


            log.info("Response contents ------------ server finish")

            request.setHandled(true)
        }
    }

    this.setContextPath(contextPath)
    this.setHandler(handler)

    private def makeTable(): Seq[Node] = {


        <h2>New push task</h2>
            <p>Create a new push task to push notification to target users.</p>
            <form class="form" method="POST" action="/home/?act=post">
                <div class="input-group">
                    <label for="message">Alert Message:</label>
                    <textarea id="message" name="message" type="text" placeholder="The message alert on user's device." class="form-control" cols="50" rows="5"></textarea>
                </div>
                <br />
                <div class="input-group">
                    <label for="action">Action Type:</label>
                    <select id="action" name="action" class="form-control">
                        <option value="story">Story</option>
                        <option value="tag">Tag</option>
                        <option value="channel">Channel</option>
                        <option value="video">Video</option>
                        <option value="photo">Photo</option>
                        <option value="gym">GYM</option>
                        <option value="special">Special</option>
                        <option value="page">Page</option>
                    </select>
                </div>
                <br />
                <div class="input-group">
                    <label for="id">Action to do:</label>
                    <input id="id" name="id" type="text" placeholder="Video ID or Story ID or Page URL" class="form-control"></input>
                </div>
                <br />
                <div class="input-group">
                    <button type="submit" class="btn btn-lg btn-primary">Submit</button>
                </div>
            </form>
    }

    private def success(): Seq[Node] ={
        <div class="alert alert-success" role="alert">
            <strong>Well done!</strong> You successfully create this push notification task.
        </div>
    }

    private def failed(): Seq[Node] ={
        <div class="alert alert-danger" role="alert">
            <strong>Well done!</strong> You successfully create this push notification task.
        </div>
    }

    private def produceNotification(conf: Map[String, String], message: (String, String, String)): List[NotificationMessage] = {

        var notificationList: List[NotificationMessage] = List()

        try {

            val ctx = new AnnotationConfigApplicationContext(classOf[BeanConfig])

            val ds: AnalyticDataSource = ctx.getBean(classOf[AnalyticDataSource])
            ds.setUrl("jdbc:mysql://" + conf.get("host").get + ":3306/analytic?characterEncoding=utf-8")
            ds.setUsername(conf.get("uname").get)
            ds.setPassword(conf.get("upswd").get)

            val _analytic = ctx.getBean(classOf[AnalyticDB])
            _analytic.setDataSource(ds)



            val dataList = _analytic.getTokens(0)

            notificationList = dataList.map(x => {
                val device_token = x.getOrElse("device_token", "")
                val device_type = x.getOrElse("device_type", "")
                val app_number = x.getOrElse("app_number", "")
                val timezone = x.getOrElse("timezone", "")
                NotificationMessage(device_token, device_type, app_number, timezone, message._1, message._2, message._3)
            })

        } catch {
            case e:Exception =>
                log.error("Read data error message:", e)
        }

        notificationList
    }
}