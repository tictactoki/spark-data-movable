package com.movable.utils

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.movable.models.AWSConfigBuilder
import com.typesafe.config.Config

import scala.collection.JavaConverters._

final case class S3Utils(config: Config) {


  val awsConfigBuilder = AWSConfigBuilder(config)
  val env = new ProfileCredentialsProvider()
  val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(env)
    .withRegion(awsConfigBuilder.region)
    .build()

  def bucketList() = {
    s3Client.listBuckets().asScala.foreach { b =>
      println(b.getName)
    }
  }


}
