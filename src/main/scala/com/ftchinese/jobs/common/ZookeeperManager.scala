package com.ftchinese.jobs.common

import org.apache.kafka.common.TopicPartition

/**
 * Zookeeper Manager
 * Created by wanbo on 16/3/21.
 */
object ZookeeperManager extends Logging {

    private var _conf: JobsConfig = null
    private var _zk: ZookeeperClient = null

    val app_root = "ftc-push"
    val topics_node = app_root + "/topics"

    def init(conf: JobsConfig): Unit ={
        this._conf = conf

        this.connect()

        if(!_zk.exists(app_root)){
            _zk.createPath(app_root)
        }

        if(!_zk.exists(topics_node)){
            _zk.createPath(topics_node)
        }
    }

    def getTopicPartitionOffset(tp: TopicPartition): Long ={
        var offset = -1L

        val node = topics_node + "/" + tp.topic() + "/" + tp.partition()
        if(_zk.exists(node)){
            try {
                val oData = _zk.get(node)
                offset = new String(oData).toLong
            } catch {
                case e: Exception =>
                    log.error("Throws exception when read the offset of TopicPartition.", e)
            }
        }

        offset
    }

    def setTopicPartitionOffset(tp: TopicPartition, offset: Long): Unit ={
        val node = topics_node + "/" + tp.topic() + "/" + tp.partition()
        if(!_zk.exists(node)){
            _zk.createPath(node)
        }

        if(_zk.exists(node)) {
            try {
                _zk.set(node, offset.toString.getBytes)
            } catch {
                case e: Exception =>
                    log.error("Throws exception when save the offset of TopicPartition.", e)
            }
        }
    }

    private def connect(): Unit = {
        _zk = new ZookeeperClient(_conf.zk_hosts, 3000, "/", Some(this.callback))
    }

    private def callback(zk: ZookeeperClient): Unit ={}
}
