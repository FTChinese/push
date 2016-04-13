package com.ftchinese.jobs.push

import scala.util.parsing.json.JSONFormat._
import scala.util.parsing.json.JSONObject


/**
 * Payload
 * Created by wanbo on 16/3/21.
 */
class Payload() {

    private var aps: Map[String, Any] = Map("alert" -> "", "badge" -> 1, "sound" -> "default")
    private var customs: Map[String, String] = Map()

    def setAlert(alert: Any): Unit ={

        alert match {
            case x: String =>
                aps = aps.updated("alert", x)
            case _ =>
        }

    }

    def setBadge(badge: Int){
        aps = aps.updated("badge", badge)
    }

    def setSound(sound: String){
        aps = aps.updated("sound", sound)
    }

    /**
     * Add a custom dictionary
     * @param key     The string key of dictionary item.
     * @param value   The value of dictionary item, now just string type is supported.
     */
    def addCustomDictionary(key: String, value: Any): Unit ={

        value match {
            case x: String =>
                customs = customs.updated(key, x)
            case _ =>
        }
    }

    def getPayload: String = {
        var tmp: Map[String, Any] = customs

        tmp = tmp + ("aps" -> aps)

        mapToJson(tmp)
    }

    def mapToJson(data: Map[String, Any]): String ={

        val ret = data.map( item => {
            item._2 match {
                case s: String =>
                    item._1 -> s
                case i: Int =>
                    item._1 -> i
                case x: Map[_, _] =>

                    val tmp = mapToJson(x.asInstanceOf[Map[String, Any]])
                    item._1 -> tmp.substring(0, tmp.length)
            }
        })

        val formatter: ValueFormatter = {
            case s: String =>
                if(s.startsWith("{")){
                    s
                } else {
                    "\"" + s + "\""
                }
            case x =>
                x.toString
        }

        JSONObject.apply(ret).toString(formatter)
    }
}
