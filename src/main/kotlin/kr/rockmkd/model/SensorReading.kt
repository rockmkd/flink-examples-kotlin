package kr.rockmkd.model

data class SensorReading(
        val id: String,
        val timestamp: Long,
        val temperature: Double
)