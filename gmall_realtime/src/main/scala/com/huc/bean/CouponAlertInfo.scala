package com.huc.bean

case class CouponAlertInfo(mid: String,  // 设备id
                           uids: java.util.HashSet[String],  // 用户id
                           itemIds: java.util.HashSet[String],  // 商品id
                           events: java.util.List[String],  // 用户行为
                           ts: Long)