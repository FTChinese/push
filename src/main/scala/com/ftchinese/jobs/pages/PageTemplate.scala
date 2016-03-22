package com.ftchinese.jobs.pages

import scala.xml.Node

/**
 * Utility functions for UI page contents.
 * Created by wanbo on 15/8/26.
 */
object PageTemplate {

    def commonHeaderNodes: Seq[Node] = {
            <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
            <link rel="stylesheet" href="/static/css/bootstrap.min.css" type="text/css"/>
            <link rel="stylesheet" href="/static/css/common.css" type="text/css"/>
    }

    def commonFooterNodes: Seq[Node] = {
            <script src="/static/js/jquery-1.11.3.min.js"></script>
            <script src="/static/js/bootstrap.min.js"></script>
    }

    def commonNavigationPage(page: WebPage): Seq[Node] ={

        val navItem = page._tabs.map(i => {
            <li><a href={i.uri}>{i.name}</a></li>
        })

        <html>
            <head>
                {commonHeaderNodes}
                {page.htmlHeaders}
                <title>{page.title}</title>
            </head>
            <body>
                <nav class="navbar navbar-default navbar-static-top">
                    <div class="container">
                        <div class="navbar-header">
                            <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                                <span class="sr-only">Toggle navigation</span>
                                <span class="icon-bar"></span>
                                <span class="icon-bar"></span>
                                <span class="icon-bar"></span>
                            </button>
                            <a class="navbar-brand" href="/">FTC Push Notification</a>
                        </div>
                        <div id="navbar" class="navbar-collapse collapse">
                            <ul class="nav navbar-nav">
                                {navItem}
                            </ul>
                            <ul class="nav navbar-nav navbar-right">
                                <li><a href="#">Help</a></li>
                            </ul>
                        </div>
                    </div>
                </nav>
                <div class="container">{page.content}</div>
                <footer class="footer">
                    <div class="container">
                        <p class="text-muted">Push Notification System.</p>
                    </div>
                </footer>
                {commonFooterNodes}
                {page.htmlFooters}
            </body>
        </html>
    }
}
