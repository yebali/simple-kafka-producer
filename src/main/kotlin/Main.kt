import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.UUID

class SimpleProducer {
    companion object {
        const val TOPIC_NAME = "test"
        const val BOOTSTRAP_SERVERS = "127.0.0.1:9092"
    }
}

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(SimpleProducer.javaClass)

    val configs = Properties()

    configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = SimpleProducer.BOOTSTRAP_SERVERS
    configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

    val producer = KafkaProducer<String, String>(configs)

    while (true) {
        val messageValue = UUID.randomUUID().toString()
        val record = ProducerRecord<String, String>(SimpleProducer.TOPIC_NAME, messageValue)

        producer.send(record) // 즉각적인 전송이 아닌 내부에서 배치로 묶어서 브로커에 전송함.
        logger.info("{}", record)
        producer.flush() // 프로듀서 내부 버퍼에 가지고 있는 레코드 배치를 브로커로 전송한다.
    }

    producer.close()
}
