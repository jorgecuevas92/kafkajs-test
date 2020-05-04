const { Kafka } = require('kafkajs')
const JSONStream = require('JSONStream')
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['127.0.0.1:9094']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  // Producing
  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: 'Hello KafkaJS user!' },
    ],
  })

  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const content = JSON.parse(message.value);
        console.log({
          partition,
          offset: message.offset,
          value: content,
        })

      } catch (error) {
        console.log(error)
      }
    },
  })
}

run().catch(console.error)