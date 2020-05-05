const { Kafka } = require('kafkajs')
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['127.0.0.1:9094']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })

const checkJson = async (string) => {
  return new Promise((resolve, reject) => {
    try {
      const parsed = JSON.parse(string)
      resolve(true)
    } catch (error) {
      resolve(false)
    }
  })
}

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
        let content 
        const isJSON = await checkJson(message.value)
        if (isJSON) {
          content = JSON.parse(message.value)
        } else {
          content = message.value.toString()
        }
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