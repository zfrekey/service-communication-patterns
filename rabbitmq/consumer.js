// Serviço C em Node: consome a mesma fila `trabalho` que o consumer.py.
// Mesmo broker, mesmo protocolo (AMQP 0-9-1), outra linguagem.
//
// Uso: node consumer.js [--nome C/node] [--processamento 0]
import amqp from "amqplib";
import { parseArgs } from "node:util";

const { values: args } = parseArgs({
  options: {
    nome: { type: "string", default: "C/node" },
    processamento: { type: "string", default: "0" },
  },
});

const FILA = "trabalho";
const conexao = await amqp.connect("amqp://localhost");
const canal = await conexao.createChannel();
// Mesmos argumentos do FILAS["trabalho"] em comum.py, senão PRECONDITION_FAILED.
await canal.assertQueue(FILA, { durable: true });
await canal.prefetch(1);

let total = 0;
console.log(`[${args.nome}] consumindo a fila ${FILA}, ack manual, prefetch 1. Aguardando...`);
await canal.consume(FILA, async (msg) => {
  const p = JSON.parse(msg.content.toString());
  await new Promise((r) => setTimeout(r, Number(args.processamento) * 1000));
  total++;
  const reentrega = msg.fields.redelivered ? " (REENTREGA)" : "";
  console.log(`[${args.nome}] pagamento ${String(p.id).padStart(3)} (${p.tipo.padEnd(6)})${reentrega}`);
  canal.ack(msg);
});

process.on("SIGINT", async () => {
  console.log(`\n[${args.nome}] total: ${total}`);
  await conexao.close();
  process.exit(0);
});
