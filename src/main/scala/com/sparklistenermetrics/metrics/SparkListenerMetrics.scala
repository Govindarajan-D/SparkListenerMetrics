package com.sparklistenermetrics.metrics

import org.apache.spark.scheduler.{SparkListener,TaskInfo, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.executor.TaskMetrics
import scala.collection.mutable.Buffer
import java.util.logging.Logger
import java.io.PrintWriter
import org.slf4j.LoggerFactory

class SparkListenerMetrics extends SparkListener {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val TaskDetails = Buffer[(TaskInfo,TaskMetrics)]()
  private val logWriter = new PrintWriter("log.txt")

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
     var taskMetricsVar = taskEnd.taskMetrics
     var taskInfoVar = taskEnd.taskInfo
     TaskDetails.append((taskInfoVar,taskMetricsVar))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val TaskDetailsString: Seq[String] = TaskDetails.map{temp:(TaskInfo,TaskMetrics) => TaskTimeAgg(temp._1,temp._2)}
    logger.info("OnStageCompleted - RecordsWritten:" + stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten)
    logger.info("Stage Details:"+stageCompleted.stageInfo.details)
    logger.info(""+stageCompleted.stageInfo.toString)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.info("OnJobEnd" + jobEnd.jobResult.toString())
    logWriter.close()
  }

  def TaskTimeAgg(info: TaskInfo, metrics: TaskMetrics): String ={
    logWriter.write(info.toString)
    logWriter.write(metrics.toString)
    info.toString
  }
}
