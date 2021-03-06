package com.ftchinese.jobs.ui.handlers

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.ftchinese.jobs.common.{JobsConfig, Logging, TaskMessage}
import com.ftchinese.jobs.ui.{PushTaskQueue, PushTaskWorker}
import com.ftchinese.jobs.ui.pages.{PageTemplate, WebPage}
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler}

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
            page.title = "Home"

            val out = httpServletResponse.getWriter

            if(requestMethod == "POST") {

                val parameters = request.getParameterMap

                val message = parameters.getOrDefault("message", Array("")).apply(0)
                val sound = parameters.getOrDefault("sound", Array("")).apply(0)
                val action = parameters.getOrDefault("action", Array("")).apply(0)
                val label = parameters.getOrDefault("label", Array("")).apply(0)
                val test = parameters.getOrDefault("test", Array("")).apply(0)

                log.info("Receive a message: %s, sound: %s, action: %s, label: %s, test: %s".format(message, sound, action, label, test))

                if(message.isEmpty || action.isEmpty || label.isEmpty){
                    page.content = failed("Every field must be nonempty!")
                    out.println(PageTemplate.commonNavigationPage(page))
                } else {

                    // Do some work
                    // Need to create task queue, new task will push into the queue.
                    // Keep each task is unique in the queue.
                    // Every task in the queue will running in a new thread.

                    if(PushTaskQueue.getSize > 0){
                        page.content = failed("There is another task running!")
                        out.println(PageTemplate.commonNavigationPage(page))
                    } else {

                        if(test == "")
                            new PushTaskWorker(conf, TaskMessage(message, sound, action, label)).start()
                        else
                            new PushTaskWorker(conf, TaskMessage(message, sound, action, label, production = false)).start()

                        page.content = success()
                        out.println(PageTemplate.commonNavigationPage(page))
                    }
                }
            } else {
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
                    <label for="sound">Sound Type:</label>
                    <select id="sound" name="sound" class="form-control">
                        <option value="">Mute</option>
                        <option value="default">Default</option>
                    </select>
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
                    <label for="label">Action target:</label>
                    <input id="label" name="label" type="text" placeholder="Video ID/Story ID/Tag/Page URL" class="form-control"></input>
                </div>
                <br />
                <div class="input-group" data-toggle="buttons">
                    <label class="btn btn-primary">
                        <input id="test" name="test" type="checkbox" autocomplete="off">Test</input>
                    </label>
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

    private def failed(msg: String): Seq[Node] ={
        <div class="alert alert-danger" role="alert">
            <strong>{msg}</strong>
        </div>
    }


}