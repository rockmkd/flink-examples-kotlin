package kr.rockmkd.common.model

data class SensorReading(
    val id: String,
    val timestamp: Long,
    val temperature: Double
)
