package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import java.util.concurrent.Executors

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, _}
import scala.util.control.NonFatal

object Test {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("CMSMonthlyLoading").enableHiveSupport().getOrCreate()

    println("TestSuccessfull")
  }
}
