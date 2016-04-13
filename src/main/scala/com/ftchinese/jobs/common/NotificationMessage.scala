package com.ftchinese.jobs.common

/**
 * The message of Notification.
 * Created by wanbo on 3/22/16.
 *
 * @param   id: The notification unique id, at least it is unique in the notifications created current date.
 */
case class NotificationMessage( deviceToken: String = "", deviceType: String = "", appNumber: String = "", timeZone: String = "",
      text: String = "", action: String = "", label: String = "", lead: String = "", badge: Int = 1, sound: String = "default",
                                 id: Int, production: Boolean, createTime: Long) extends Message{

      override def toJson: String ={
            """{"deviceToken": "%s", "deviceType": "%s", "appNumber": "%s", "timeZone": "%s", "text": "%s", "action": "%s", "label": "%s", "lead": "%s", "badge": %d, "sound": "%s", "id": %d, "production": %s, "createTime": %s}""".format(deviceToken, deviceType, appNumber, timeZone, text, action, label, lead, badge, sound, id, production, createTime)
      }
}
