package kr.rockmkd.util.sources

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class EnvironmentFactory {

  companion object{
    fun default(p: Int = 3): StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().apply {
      parallelism = p
      streamTimeCharacteristic = TimeCharacteristic.EventTime
    }

    fun withProcessingTime(p: Int = 3): StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().apply {
      parallelism = 3
      streamTimeCharacteristic = TimeCharacteristic.ProcessingTime
    }

    fun single(): StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().apply {
      parallelism = 1
      streamTimeCharacteristic = TimeCharacteristic.ProcessingTime
    }
  }

}
