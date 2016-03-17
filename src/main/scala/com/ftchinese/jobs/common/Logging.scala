package com.ftchinese.jobs.common

import org.slf4j.LoggerFactory

/**
 * Trait of log data.
 * Created by wanbo on 8/26/15.
 */
trait Logging {
    protected val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
}
