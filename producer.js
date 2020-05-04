const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['127.0.0.1:9094']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  // Producing
  await producer.connect()
  const send = async() => {
    await producer.send({
      topic: 'test-topic',
      messages: [
        { value: 'Hello KafkaJS user!' },
      ],
    })
  }

  const message = {
    this: 'that',
    that: 'this',
    testing: 'test'
  }
  setInterval( async() => {
    var buf = Buffer.from(JSON.stringify(message));
    await producer.send({
      topic: 'test-topic',
      messages: [
        { value: buf },
      ],
    })
  }, 5000)

}

run().catch(console.error)