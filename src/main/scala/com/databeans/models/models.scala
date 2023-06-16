package com.databeans.models

case class Owner(id: String,  username:String )
case class Data(created_at: Long, id:String, owner:Owner, text:String )
case class Comments(data: Seq[Data])
case class Edge_media_preview_like(count: Long)
case class Edge_media_to_comment(count: Long)

case class OwnerGraphImages(id:String)
case class GraphImages(__typename:String, comments:Comments,comments_disabled:Boolean, edge_media_preview_like:Edge_media_preview_like,
                       edge_media_to_comment:Edge_media_to_comment,id:String, is_video:Boolean, owner:OwnerGraphImages, shortcode:String, taken_at_timestamp:Long, username:String)
case class Info (biography:String,followers_count:Long,following_count:Long, full_name:String , id:String , is_business_account:Boolean, is_joined_recently:Boolean, is_private:Boolean, posts_count:Long)
case class GraphProfileInfo(created_time:Long,info:Info,username:String)
case class InputData(GraphImages:Seq[GraphImages],GraphProfileInfo:GraphProfileInfo)