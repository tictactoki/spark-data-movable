package com.movable.utils

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import scala.collection.JavaConverters._

case class S3Utils() {

  val region = Regions.EU_WEST_3
  val env = new ProfileCredentialsProvider()
  val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(env)
    .withRegion(region)
    .build()

  def bucketList() = {
    s3Client.listBuckets().asScala.foreach { b =>
      println(b.getName)
    }
  }


}
