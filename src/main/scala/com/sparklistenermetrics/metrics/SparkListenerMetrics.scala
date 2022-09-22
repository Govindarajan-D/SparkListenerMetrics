package com.sparklistenermetrics.metrics

import org.apache.spark.scheduler.{SparkListener,TaskInfo, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.executor.TaskMetrics
import scala.collection.mutable.Buffer
import java.util.logging.Logger
import org.slf4j.LoggerFactory

class SparkListenerMetrics extends SparkListener {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val TaskDetails = Buffer[(TaskInfo,TaskMetrics)]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
     var taskMetricsVar = taskEnd.taskMetrics
     var taskInfoVar = taskEnd.taskInfo
     TaskDetails.append((taskInfoVar,taskMetricsVar))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.info("OnStageCompleted - RecordsWritten:" + stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten)
    logger.info("Stage Details:"+stageCompleted.stageInfo.details)
    logger.info(""+stageCompleted.stageInfo.toString)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.info("OnJobEnd" + jobEnd.jobResult.toString())
  }
}
