package com.sparklistenermetrics.metrics

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd, TaskInfo}
import org.apache.spark.executor.TaskMetrics

import scala.collection.mutable.Buffer
import java.util.logging.Logger
import java.io.PrintWriter
import org.slf4j.LoggerFactory

import scala.collection.mutable

class SparkListenerMetrics extends SparkListener {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private val TaskDetails = mutable.Buffer[(TaskInfo,TaskMetrics)]()
  private val StageDetails = mutable.Buffer[(String,Long)]()
  private val logWriter = new PrintWriter("log.txt")
  private var appName: String = _

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appName = applicationStart.appName
    logger.info("OnApplicationStart:" + applicationStart.appName)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
     var taskMetricsVar = taskEnd.taskMetrics
     var taskInfoVar = taskEnd.taskInfo
     TaskDetails.append((taskInfoVar,taskMetricsVar))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val TaskReadRecordList: mutable.Buffer[Long] = {
      TaskDetails.map((temp: (TaskInfo, TaskMetrics)) => TaskTimeAgg(temp._1, temp._2))
    }
    val sumRecordsRead: Long = {
      TaskReadRecordList.foldLeft(0: Long)(_ + _)
    }

    StageDetails.append((stageCompleted.stageInfo.name, sumRecordsRead))

    logger.info(appName + ": Stage:" + stageCompleted.stageInfo.name + ": OnStageCompleted - RecordsRead:" + sumRecordsRead.toString)
    logger.info(appName + ": Stage:" + stageCompleted.stageInfo.name + ": OnStageCompleted - RecordsWritten:" + stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten)
    logger.info("Stage Details:"+stageCompleted.stageInfo.details)
    logger.info(""+stageCompleted.stageInfo.toString)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val StageRecordsReadList: mutable.Buffer[Long] = {
      StageDetails.map((temp: (String,Long)) => temp._2)
    }

    val sumRecordsWrittenStage: Long = {
      StageRecordsReadList.foldLeft(0: Long)(_+_)
    }
    logger.info(appName + ": Job: " + jobEnd.jobId + "OnJobEnd - RecordsRead:" + sumRecordsWrittenStage.toString)
    logger.info(appName + ": OnJobEnd" + jobEnd.jobResult.toString)
    logWriter.close()
  }

  def TaskTimeAgg(info: TaskInfo, metrics: TaskMetrics): Long = {
    logWriter.write(info.toString)
    logWriter.write(metrics.inputMetrics.recordsRead.toString)
    metrics.inputMetrics.recordsRead
  }

}
