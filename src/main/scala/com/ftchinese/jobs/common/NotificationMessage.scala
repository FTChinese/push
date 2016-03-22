package com.ftchinese.jobs.common

/**
 * The message of Notification.
 * Created by wanbo on 3/22/16.
 */
case class NotificationMessage( deviceToken: String = "", deviceType: String = "", appNumber: String = "", timeZone: String = "",
      text: String = "", action: String = "", id: String = "", lead: String = "", badge: String = "1", sound: String = "default" ) extends Message{

      override def toJson: String ={
            """{"deviceToken": "%s", "deviceType": "%s", "appNumber": "%s", "timeZone": "%s", "text": "%s", "action": "%s", "id": "%s", "lead": "%s", "badge": "%s", "sound": "%s"}""".format(deviceToken, deviceType, appNumber, timeZone, text, action, id, lead, badge, sound)
      }
}
