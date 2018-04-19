package producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object DemoProducer extends App {

  val TOPIC = "test"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  for (i <- 1 to 10000) {
    val producerRecord = new ProducerRecord[String, String](TOPIC, null, null, i.toString)
    producer.send(producerRecord)
  }

  val lastRecord = new ProducerRecord[String, String](TOPIC, null, null, "the end " + new java.util.Date)

  producer.close()
}
