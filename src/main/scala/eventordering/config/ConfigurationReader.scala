package eventordering.config

import java.io.InputStream
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import eventordering.Config
import eventordering.model.OrderConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.slf4j.{Logger, LoggerFactory}

object ConfigurationReader {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def readOrderConfigurations(filePath: String): Map[String, List[String]] = {
    try {
      val orderConfigJson: InputStream = getClass.getClassLoader.getResourceAsStream(filePath)
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(orderConfigJson, classOf[Array[OrderConfig]]).map(config => config.correlation_id -> config.order).toMap
    } catch {
      case e: Exception =>
        logger.error(s"Error reading order configuration JSON from $filePath", e)
        Map.empty[String, List[String]]
    }
  }

  def createKafkaSource(): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setBootstrapServers(Config.getProperty("kafka.bootstrap.servers"))
      .setTopics(Config.getProperty("kafka.topics"))
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
  }
}
