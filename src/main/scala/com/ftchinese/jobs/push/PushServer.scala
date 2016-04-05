package com.ftchinese.jobs.push

import com.ftchinese.jobs.common.AppleServer

/**
 * Apple server.
 * Created by wanbo on 16/3/21.
 */
object PushServer extends AppleServer{
    override val pro_host = "gateway.push.apple.com"
    override val pro_port = 2195

    override val dev_host = "gateway.sandbox.push.apple.com"
    override val dev_port = 2195
}
