// Producer Node.js: mesmo topico, outra linguagem.
const { Kafka } = require('kafkajs');
const kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: 0 });
(async () => {
  const p = kafka.producer(); await p.connect();
  for (let i = 100; i < 104; i++) {
    const conta = `conta-${i % 4}`;
    const r = await p.send({ topic: 'pagamentos', acks: -1,
      messages: [{ key: conta, value: JSON.stringify({ id: i, conta, valor: i * 1.5, origem: 'node' }) }] });
    console.log(`[producer-node] ${conta} -> particao ${r[0].partition} offset ${r[0].baseOffset}`);
  }
  await p.disconnect();
})();
