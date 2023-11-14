package eventordering.processing

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.functions.MapFunction
import eventordering.model.Event
import org.slf4j.{Logger, LoggerFactory}

class EventMapper extends MapFunction[String, Event] {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def map(jsonString: String): Event = {
    try {
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(jsonString, classOf[Event])
    } catch {
      case e: Exception =>
        logger.error("Error parsing event JSON", e)
        Event("", "", "", "")
    }
  }
}
