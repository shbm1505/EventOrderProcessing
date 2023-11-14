package eventordering.model

case class Event(microservice: String, event_type: String, correlation_id: String, timestamp: String)
