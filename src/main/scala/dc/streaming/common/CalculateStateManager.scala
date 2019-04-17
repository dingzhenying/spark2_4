package dc.streaming.common

import java.util

import dc.streaming.common.CalculateStateManager.{ExceptionWriteMode, State, StateInfo, TimePeriodWriteMode}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.GroupState
import java.util.Map.Entry

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by shirukai on 2019-02-21 14:02
  * 状态管理器
  */
object CalculateStateManager {

  /**
    * 状态信息
    *
    * @param value              数值
    * @param lastValue          上一个值
    * @param exceptionStartTime 异常开始时间
    * @param exceptionEndTime   异常结束时间
    * @param processingTimeMs   处理时间
    */
  case class StateInfo(value: Double, lastValue: Double = 0, exceptionStartTime: Long = 0, exceptionEndTime: Long = 0, processingTimeMs: Long = 0)

  case class DeviceWithException(gatewayId: String, namespace: String, pointId: String, id: String, uri: String, v: Double, s: String, t: Long,
                                 exceptionStartTime: Long, exceptionEndTime: Long, exceptionWriteMode: Int, timePeriodWriteMode: Int)

  type State = util.TreeMap[Long, StateInfo]
  implicit val State: Encoder[util.TreeMap[Long, StateInfo]] = org.apache.spark.sql.Encoders.kryo[util.TreeMap[Long, StateInfo]]

  def filter(state: GroupState[State]): (Long, Long, State) = {
    val oldState: State = state.getOption.getOrElse(new util.TreeMap[Long, StateInfo])
    val filterKey = state.getCurrentWatermarkMs() / 1000
    oldState.headMap(filterKey).clear()
    (state.getCurrentProcessingTimeMs(), filterKey, oldState)
  }


  /**
    * 异常写库出模式（只有INSERT、DELETE、NO_WRITE）
    *
    * 作用：写入异常库时标记哪些些数据是插入，哪些数据是更新，哪些数据是删除
    *
    * 满足插入的条件：
    * 1. 更新当前值且该值异常
    * 2. 更新下一条值 && processingTimeMs!=currentProcessingTimeMs，之前正常&&现在异常
    *
    * 满足更新的条件：
    * 1. 更新上一条值 && processingTimeMs!=currentProcessingTimeMs && currentProcessingTimeMs!=0(废弃)
    *
    * 2. 更新下一条值 && processingTimeMs!=currentProcessingTimeMs && 之前异常 && 现在异常(废弃）
    *
    * 满足删除的条件：
    * 1. 更新下一条值，现在值异常&&processingTimeMs!=currentProcessingTimeMs && 原来异常现在正常
    */
  object ExceptionWriteMode {
    val INSERT: Int = 1
    val DELETE: Int = -1
    val NO_WRITE: Int = 0
  }


  /**
    * 异常时间段写出模式
    * 作用：写入异常时间段库时，标记哪些数据是插入、哪些数据是更新、哪些数据是删除、哪些是不需要写入
    *
    * 满足插入条件：
    * 1. 更新上一条数据 &&  之前异常结束时间=0 && 现在的exceptionEndTime>0
    * 2. 更新当前值 && 下一条数据正常
    * 3. 更新下一条数据 && 现在的exceptionEndTime>0 && 现在的exceptionEndTime!=之前的现在的exceptionEndTime
    * 4. 最后一条数据且异常，现在的exceptionEndTime设置为1
    *
    * 满足更新条件
    * 1. 更新上一条数据 && processingTimeMs!=currentProcessingTimeMs && 之前异常结束时间>0 && 之前异常结束时间!=当前异常结束时间 && 当前异常结束时间>0
    * 2. 更新下一条数据 && processingTimeMs!=currentProcessingTimeMs && 之前异常结束时间>0 && 当前异常结束时间>0 && 之前结束结束时间!=当前异常结束时间
    *
    * 满足删除条件
    * 1. 更新上一条数据 && processingTimeMs!=currentProcessingTimeMs && 之前异常结束时间>0 && 现在exceptionEndTime = 0
    * 2. 更新下一条数据 && processingTimeMs!=currentProcessingTimeMs && 之前异常结束时间>0 && 现在exceptionEndTime = 0
    */
  object TimePeriodWriteMode {
    val INSERT: Int = 1
    val UPDATE: Int = 2
    val DELETE: Int = -1
    val NO_WRITE: Int = 0
  }

}

/**
  * 带有异常时间统计的接口
  *
  * @tparam A 传入数据类型
  * @tparam B 返回数据类型
  */
trait CalculateTrait[A, B] extends Serializable {

  /**
    * 当批数据预处理
    *
    * @param devices Iterator[A]
    * @return ListBuffer[A]
    */
  def handleDevices(devices: Iterator[A]): ListBuffer[A]

  /**
    * 判别当前数据是否异常
    *
    * @param lastState 上一条数据的状态
    * @param device    当前数据
    * @return boolean
    */
  def currentExceptionDiscriminator(lastState: Option[Entry[Long, StateInfo]], device: A): Boolean

  /**
    * 判别下一条数据是否异常
    *
    * @param device    当前数据
    * @param nextState 下一条数据
    * @return boolean
    */
  def nextExceptionDiscriminator(device: A, nextState: Option[Entry[Long, StateInfo]]): Boolean

  /**
    * 状态数据
    *
    * @param device 当前数据
    * @return stateInfo
    */
  def deviceState(device: A): (Long, StateInfo)

  /**
    * 异常数据处理
    *
    * @param device              当前数据
    * @param timestamp           时间戳
    * @param state               状态
    * @param exceptionWriteMode  异常写出模式
    * @param timePeriodWriteMode 异常时间段写出模式
    * @return
    */
  def handleDeviceWithException(device: A, timestamp: Long, state: StateInfo, exceptionWriteMode: Int, timePeriodWriteMode: Int): B

  /**
    * 处理最后一条数据
    *
    * @param lastDevice lastDevice
    * @return B
    */
  def handleLastDevice(lastDevice: B): B

  /*
    * 更新上一条记录
    *
    * 条件
    * 1. 存在上一条记录
    * 2. 上一条记录异常
    * 3. 当前值是否异常
    * 4. 异常结束时间是否变化
    * 更新内容
    * value、exceptionStart不变,exceptionEnd=当前值是否异常?0:当前值时间
    *
    * 更新当前记录
    *
    * 条件
    * 1. 上一条记录是否异常
    * 2. 下一条记录是否异常
    * 3. 当前记录是否异常
    * 更新内容
    * 当前记录正常的情况下，value、exceptionStart=0，exceptionEnd=0
    * 当前记录异常的情况下
    * exceptionStart=上一条记录是否异常?上一条记录的exceptionStart:当前时间
    * exceptionEnd=下一条记录是否异常?0:下一条记录的时间
    *
    * 更新下一条记录
    *
    * 条件
    * 1. 存在下一条记录
    * 2. 下一条是否记录异常
    * 3. 当前记录是否异常
    * 4. 循环更新下条异常值
    *
    * 更新内容
    *
    * value不变，exceptionStart=当前值是否异常？当前值的exceptionStart:下一条值的时间，exceptionEnd=0
   */
  def calculate(id: String, devices: Iterator[A], historyState: GroupState[State]): Iterator[B] = {
    // Handle new data
    val deviceList = handleDevices(devices)
    val updateDevices = mutable.TreeMap[Long, B]()
    if (deviceList.isEmpty) return updateDevices.valuesIterator

    // Filter out expired data in historical stat from spark
    val (currentProcessingTimeMs, filterKey, state) = CalculateStateManager.filter(historyState)


    deviceList.foreach(device => {
      val currentState = deviceState(device)
      // Filter duplicate data and expired data
      if (!state.containsKey(currentState._1) && currentState._1 >= filterKey) {

        // Get the previous value
        val previous = Option(state.floorEntry(currentState._1))
        // Get the next value
        val next = Option(state.higherEntry(currentState._1))

        // Determine if the data is exception
        val isPreviousException = previous.isDefined && previous.get.getValue.exceptionStartTime > 0
        val isCurrentException = currentExceptionDiscriminator(previous, device)
        val isNextException = nextExceptionDiscriminator(device, next)

        // Update the previous value
        if (isPreviousException) {
          val previousExceptionEnd = if (isCurrentException) 0 else currentState._1
          // Can be updated when the previous's exceptionEndTime changes
          if (previous.get.getValue.exceptionEndTime != previousExceptionEnd) {

            val previousStateInfo = previous.get.getValue.copy(exceptionEndTime = previousExceptionEnd)

            // The condition of the write mode：previousStateInfo.processingTimeMs != currentProcessingTimeMs && currentProcessingTimeMs != 0
            val exceptionWriteMode = ExceptionWriteMode.NO_WRITE
            var timePeriodWriteMode = TimePeriodWriteMode.NO_WRITE

            // 是否处于同一个处理批次
            if (previousStateInfo.processingTimeMs != currentProcessingTimeMs && currentProcessingTimeMs != 0) {

              if (previous.get.getValue.exceptionEndTime > 0) {
                if (previousExceptionEnd > 0) {
                  if (previous.get.getValue.exceptionEndTime != previousExceptionEnd) timePeriodWriteMode = TimePeriodWriteMode.UPDATE
                } else {
                  timePeriodWriteMode = TimePeriodWriteMode.DELETE
                }
              } else if (previousExceptionEnd > 0) {
                timePeriodWriteMode = TimePeriodWriteMode.INSERT
              }

            }

            state.put(previous.get.getKey, previousStateInfo)
            updateDevices(previous.get.getKey) = handleDeviceWithException(device, previous.get.getKey, previousStateInfo, exceptionWriteMode, timePeriodWriteMode)
          }
        }

        // Update the current value
        var currentStateInfo = currentState._2
        var cExceptionWriteMode = ExceptionWriteMode.NO_WRITE
        var cTimePeriodWriteMode = TimePeriodWriteMode.NO_WRITE
        val cLastValue = if (previous.isDefined) previous.get.getValue.value else 0
        if (isCurrentException) {
          currentStateInfo = currentState._2.copy(
            lastValue = cLastValue,
            exceptionStartTime = if (isPreviousException) previous.get.getValue.exceptionStartTime else currentState._1,
            exceptionEndTime = if (!isNextException && next.isDefined) {
              cTimePeriodWriteMode = TimePeriodWriteMode.INSERT
              next.get.getKey
            } else 0)
          cExceptionWriteMode = ExceptionWriteMode.INSERT
        } else {
          currentStateInfo = currentState._2.copy(lastValue = cLastValue)
          cExceptionWriteMode = ExceptionWriteMode.NO_WRITE
        }

        state.put(currentState._1, currentStateInfo)
        updateDevices(currentState._1) = handleDeviceWithException(device, currentState._1, currentStateInfo, cExceptionWriteMode, cTimePeriodWriteMode)

        // Update the next value
        if (next.isDefined) {
          val nextStateInfo = if (isNextException)
            next.get.getValue.copy(
              lastValue = currentStateInfo.value,
              exceptionStartTime = if (isCurrentException) currentStateInfo.exceptionStartTime else
                next.get.getKey)
          else next.get.getValue.copy(lastValue = currentStateInfo.value, exceptionStartTime = 0, exceptionEndTime = 0)

          if (next.get.getValue.exceptionStartTime != nextStateInfo.exceptionStartTime) {
            var nextLastExceptionEndTime = currentStateInfo.exceptionEndTime
            var nextItem = (next.get.getKey, nextStateInfo)
            var hasNextException = true
            var nExceptionWriteMode = ExceptionWriteMode.NO_WRITE

            // Determine write model
            if (nextItem._2.processingTimeMs != currentProcessingTimeMs && currentProcessingTimeMs != 0) {
              // If it was exception before, but now it is normal，DELETE
              if (next.get.getValue.exceptionStartTime > 0 && nextItem._2.exceptionStartTime == 0) {
                nExceptionWriteMode = ExceptionWriteMode.DELETE
              }

              // If it was normal before, but now it is exception，INSERT
              if (next.get.getValue.exceptionStartTime == 0 && nextItem._2.exceptionStartTime > 0) {
                nExceptionWriteMode = ExceptionWriteMode.INSERT
              }

              //              // If it was exception before, now it is also exception，UPDATE
              //              if (next.get.getValue.exceptionStartTime > 0 && nextItem._2.exceptionStartTime > 0) {
              //                nExceptionWriteMode = ExceptionWriteMode.UPDATE
              //              }
            }
            // Loop update until there is no exception data
            while (hasNextException) {
              var nTimePeriodMode = TimePeriodWriteMode.NO_WRITE

              if (nextItem._2.exceptionEndTime > 0 && nextItem._2.exceptionEndTime != nextLastExceptionEndTime) {
                nTimePeriodMode = TimePeriodWriteMode.INSERT
              }

              if (nextItem._2.processingTimeMs != currentProcessingTimeMs && currentProcessingTimeMs != 0 && nextLastExceptionEndTime > 0) {
                if (nextItem._2.exceptionEndTime > 0 && nextItem._2.exceptionEndTime != nextLastExceptionEndTime) {
                  nTimePeriodMode = TimePeriodWriteMode.UPDATE
                } else if (nextItem._2.exceptionEndTime == 0) {
                  nTimePeriodMode = TimePeriodWriteMode.DELETE
                }
              }

              state.put(nextItem._1, nextItem._2)
              updateDevices(nextItem._1) = handleDeviceWithException(device, nextItem._1, nextItem._2, nExceptionWriteMode, nTimePeriodMode)
              // Continue to determine if there is a next value
              val nextEntry = Option(state.higherEntry(nextItem._1))
              if (nextEntry.isDefined) {
                nextLastExceptionEndTime = nextItem._2.exceptionEndTime
                // Determine whether it is exception
                hasNextException = nextEntry.get.getValue.exceptionStartTime > 0
                nextItem = (nextEntry.get.getKey, nextEntry.get.getValue)
                //                nExceptionWriteMode = ExceptionWriteMode.UPDATE
              } else {
                hasNextException = false
              }
            }
          }
        }
      }
    })
    if (updateDevices.nonEmpty) {
      // 取最后一条数据
      val (lastKey, lastDevice) = updateDevices.last
      val lastState = state.get(lastKey)
      // 如果最后一条数据异常，且异常结束时间为0，设置结束时间为1
      if (lastState.exceptionStartTime > 0 && lastState.exceptionEndTime == 0) {
        state.put(lastKey, lastState.copy(exceptionEndTime = 1))
        updateDevices(lastKey) = handleLastDevice(lastDevice)
      }
    }
    historyState.update(state)
    updateDevices.values.iterator
  }

}


