# Kafka-Streams

## Развернуть сервер с блокером Kafka

docker-compose up -d

## Создать необходимые топики:

```bash
docker exec -it kafka-1 kafka-topics --create --topic messages --bootstrap-server kafka-1:9092 --partitions 1 --replication-factor 1
docker exec -it kafka-1 kafka-topics --create --topic filtered_messages --bootstrap-server kafka-1:9092 --partitions 1 --replication-factor 1
docker exec -it kafka-1 kafka-topics --create --topic blocked_users --bootstrap-server kafka-1:9092 --partitions 1 --replication-factor 1
```

## Отправить сообщения блокировки пользователей

```bash
docker exec -it kafka-1 kafka-console-producer --broker-list kafka-1:9092 --topic blocked_users --property "parse.key=true" --property "key.separator=:"
```

Данные по блокировке имеют вид
```bash
nickname1: nicknameXXX
nicknameXXX: user666
```

ключ - это пользователь, блокирующий нежелательных отправителей

## Отправьте тестовые данные в топик messages
Сообщения имеют вид JSON
```json
{
    "to": "nickname1",
    "from": "user666",
    "text": "Hello it is test message"
}
```
При этом ключ сообщения в кафку должен содержать пользователя отправителя
user666: {"to": "nickname1","from": "user666","text": "Hello it is test message"}

```bash
docker exec -it kafka-1 kafka-console-producer --broker-list kafka-1:9092 --topic messages --property "parse.key=true" --property "key.separator=:"
```

## Проверить топик filtered_messages

```bash
docker exec -it kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 --topic filtered_messages --from-beginning
```