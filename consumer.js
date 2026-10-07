// Consumer Node.js (kafkajs) - outra linguagem lendo o mesmo topico. Uso: node consumer.js <group> <nome>
const { Kafka } = require('kafkajs');
const [group, nome] = process.argv.slice(2);
const kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: 0 });
(async () => {
  const c = kafka.consumer({ groupId: group });
  await c.connect();
  await c.subscribe({ topic: 'pagamentos', fromBeginning: true });
  await c.run({ eachMessage: async ({ partition, message }) => {
    const e = JSON.parse(message.value.toString());
    console.log(`[${nome}|${group}] p${partition} off=${message.offset} ${e.conta} R$${e.valor} (${e.origem})`);
  }});
})();
