package com.ftchinese.jobs.update

/**
 * The message of Token update from Apple feedback service.
 * Created by wanbo on 4/5/16.
 */
case class UpdateTokenMessage(deviceToken: String = "", timestamp: Long = 0L)
