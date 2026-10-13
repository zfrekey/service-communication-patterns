#!/usr/bin/env bash
# Atalhos para as ferramentas administrativas do Kafka, executadas dentro do container.
set -euo pipefail

BIN=/opt/kafka/bin
BROKER=localhost:9092
TOPICO=pagamentos

kafka() { docker exec kafka "$BIN/$1" --bootstrap-server "$BROKER" "${@:2}"; }

case "${1:-}" in
  criar-topico)
    kafka kafka-topics.sh --create --if-not-exists --topic "$TOPICO" --partitions 3 --replication-factor 1
    kafka kafka-topics.sh --describe --topic "$TOPICO" ;;
  recriar-topico)
    kafka kafka-topics.sh --delete --if-exists --topic "$TOPICO"
    sleep 2
    "$0" criar-topico ;;
  topico)
    kafka kafka-topics.sh --describe --topic "$TOPICO" ;;
  offsets)
    kafka kafka-get-offsets.sh --topic "$TOPICO" ;;
  grupos)
    kafka kafka-consumer-groups.sh --list ;;
  lag)
    kafka kafka-consumer-groups.sh --describe --group "${2:?informe o grupo, ex.: ./comandos.sh lag grupo-b}" ;;
  ler)
    # Lê o tópico inteiro sem consumer group: mostra que ler não apaga nada.
    kafka kafka-console-consumer.sh --topic "$TOPICO" --from-beginning --timeout-ms 5000 \
      --property print.partition=true --property print.offset=true --property print.key=true ;;
  *)
    echo "uso: $0 {criar-topico|recriar-topico|topico|offsets|grupos|lag <grupo>|ler}"
    exit 1 ;;
esac
