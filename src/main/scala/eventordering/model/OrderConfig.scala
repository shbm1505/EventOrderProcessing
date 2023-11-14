package eventordering.model

case class OrderConfig(correlation_id: String, order: List[String])
