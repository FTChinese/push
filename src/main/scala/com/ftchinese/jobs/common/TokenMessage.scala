package com.ftchinese.jobs.common

/**
 * The message of Token.
 * Created by wanbo on 3/17/16.
 */
case class TokenMessage(deviceToken: String = "", timeZone: String = "", status: String = "", preference: String = "",
    deviceType: String = "", appNumber: String = "", timestamp: String = "")
