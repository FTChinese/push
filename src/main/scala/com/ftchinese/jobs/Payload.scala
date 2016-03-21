package com.ftchinese.jobs

import scala.util.parsing.json.JSONObject


/**
 * Payload
 * Created by wanbo on 16/3/21.
 */
class Payload() {

    private var aps: Map[String, String] = Map("alert" -> "", "badge" -> "1", "sound" -> "default")
    private var customs: Map[String, String] = Map()

    def setAlert(alert: Any): Unit ={

        alert match {
            case x: String =>
                aps = aps.updated("alert", x)
            case _ =>
        }

    }

    def setBadge(badge: String){
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

        val apsJson = JSONObject.apply(aps)

        customs = customs.updated("aps", apsJson.toString())

        val jsonArr = JSONObject.apply(customs)

        jsonArr.toString()
    }

}
