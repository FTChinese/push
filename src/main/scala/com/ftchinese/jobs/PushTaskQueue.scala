package com.ftchinese.jobs

import com.ftchinese.jobs.common.Logging

import scala.collection.mutable

/**
 * Task Queue
 * Created by wanbo on 16/3/23.
 */
object PushTaskQueue extends Serializable with Logging {

    val maxSize = 1
    private val taskQueue = mutable.Queue[PushTask]()

    /**
     * Push a task to queue
     */
    def push(task: PushTask): Boolean = {
        var ret = false

        try{

            taskQueue.synchronized {

                if(!taskQueue.contains(task)) {
                    if (taskQueue.size < maxSize) {
                        taskQueue += task
                        ret = true
                    } else {
                        throw new Exception("The queue was full.")
                    }
                } else {
                    log.info("The message was exists.")
                }

            }

        } catch {
            case e: Exception =>
                log.error("Error:", e)
        }
        ret
    }

    /**
     * Pull a task from queue
     */
    def pull(): PushTask = {
        var task: PushTask = null

        taskQueue.synchronized{
            if(taskQueue.size > 0)
                task = taskQueue.dequeue()
        }

        task
    }

    def getSize = taskQueue.size
}
