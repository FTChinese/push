package com.ftchinese.jobs.update

import com.ftchinese.jobs.common.AppleServer

/**
  * Feedback service server.
  * Created by wanbo on 16/4/5.
  */
object UpdateServer extends AppleServer{
    override val pro_host = "feedback.push.apple.com"
    override val pro_port = 2196

    override val dev_host = "feedback.sandbox.push.apple.com"
    override val dev_port = 2196
}
