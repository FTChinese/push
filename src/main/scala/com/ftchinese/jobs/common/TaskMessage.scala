package com.ftchinese.jobs.common

/**
 * Task case class
 * Created by wanbo on 16/3/23.
 *
 * @param production: Default is true, if false the task just for testing.
 * @param createTime: The time millis when task was created.
 */
case class TaskMessage(message: String = "", sound: String = "", action: String = "", label: String = "", production: Boolean = true, createTime: Long = System.currentTimeMillis())
