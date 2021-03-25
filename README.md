# kconnect-jdbc-outbox-transform

Implementação de transforms do [Kafka Connect](https://docs.confluent.io/platform/current/connect) para usar com [io.confluent.connect.jdbc.JdbcSourceConnector](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc) fazendo queries de uma tabela que implementa [transaction outbox](https://microservices.io/patterns/data/transactional-outbox.html).

## Motivação
---

Podemos usar JdbcSourceConnector e fazer streaming de uma tabela especifica, porém, por default, esse streaming será feito com os dados exatos da tabela (coluna a coluna).

Por exemplo, uma tabela de **user** com colunas: id, name, email, created_at e updated_at, geraria [esse schema avro](docto/schema-user-table-value.avsc). Caso precisássemos alterar a estrutura da tabela, removendo por exemplo alguma coluna, o formato da mensagem mudaria (já que ela reflete nossa tabela), o que poderia quebrar algum consumidor (e ai independente de usar avro ou não, caso um consumidor entendesse um atributo da mensagem como obrigatório e esse atributo deixasse de chegar, ele quebraria).

Usar o [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html) ajuda a manter a compatibilidade das mensagens, porém esse acoplamento da estrutura da tabela da aplicação com a mensagem notificada, deixa o modelo da aplicação refém para fazer alterações, o que é péssimo para um sistema.

Outro problema é que a estrutura do evento é simples, limitada aos dados da tabela. E se quisessemos saber qual era o valor alterior de um dado? E se quisessemos trabalhar com um evento mais complexo, usando atributos com tipos complexos?

Para isso, ao invés de notificar a tabela, criamos uma estrutura de mensagens de eventos diferente da estrutura das tabelas da aplicação, dai a chamada outbox table. Nela, vamos gravar de forma transacional o evento que queremos enviar, e ao invés de notificar as tabelas da nossa aplicação, passamos a notificar a tabela de outbox. 

Porém a tabela de outbox é como outra tabela qualquer, tem suas colunas, não queremos notificar cada coluna dela, apenas o evento gravado dentro dela, e como fazer então para gravar o evento na coluna e notificar apenas ele com kafka connect? usamos [kafka transforms](https://docs.confluent.io/platform/current/connect/transforms/overview.html). Existem até alguns transforms como o [ExtraField](https://docs.confluent.io/platform/current/connect/transforms/extractfield.html) que permite notificar apenas uma coluna, mas quando usamos avro, como gravar essa coluna? e principalmente, como gravar essa coluna e não ter problemas de compatibilidade caso o schema do avro mude?

Assumindo que a mensagem pode ser gravada na tabela de outbox em T1 usando o schema 1, então em T2 ocorre um deploy que atualiza o schema para a versão 2 e então em T3 roda o kafka connect e lê esses dados, o kafka connect teria que: 1) Enviar exatamente os dados do evento persistindos na coluna da tabela de outbox para o tópico especifico, no formato especificado pelo schema e 2) não pode dar erro de falta de compatibilidade da mensagem gravada na tabela com o que está registrado no schema.

Usando debezium, existe um [transform](https://debezium.io/documentation/reference/configuration/outbox-event-router.html) que pode ser usado e que faz isso, mas não queremos usar debezium, queremos usar JdbcSourceConnector. Então criamos um transform que suporta leituras de tabelas de outbox, de eventos serializados usando [KafkaAvroSerializer](https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/KafkaAvroSerializer.java), suportando mudança de versão dos schemas.

## Como usar 

Essa lib foi usada para usar como exemplo em aula de implementação de outbox, pode conferir [aqui](https://github.com/luizroos/hands-on-microservices/tree/e3/vm), nele ensino a subir o kafka, criar uma imagem do kafka connect com esse transform e executar lendo de uma aplicação de teste.

De qualquer forma, os parâmetros básicos para se usar são esses:

```console
{
  "name": "outbox-connect",
  "config": {
    "name": "outbox-connect",

    "tasks.max": "1",

    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",   // (1) configuramos JdbcSourceConnector
    "mode": "timestamp",
    "timestamp.column.name": "created_at",
    "query": "select message_key, message_payload, message_topic, created_at from outbox_table", // (2) monitoramos a tabela de outbox
    "poll.interval.ms": "1000",
    "batch.max.rows": "5000",

    "connection.url": "jdbc:mysql://mysql:3306/sample-db",
    "connection.user": "db_user",
    "connection.password": "db_pass",
    "connection.attempts": "5",
    "connection.backoff.ms": "1000",
    "schema.pattern": "sample-db",

    "value.converter": "io.confluent.connect.avro.AvroConverter",  // (3) usamos o AvroConverter pois o transform hoje só funciona para tabelas outbox que gravam a mensagem serializada em avro
    "value.converter.use.latest.version": "true",
    "value.converter.enhanced.avro.schema.support": "true",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.auto.register.schemas": "true",
    "value.converter.schemas.enable": "true",
    "value.converter.connect.meta.data": "false",

    "transforms": "outbox",
    "transforms.outbox.type": "transform.outbox.AvroJdbcOutbox",   // (4) declamaramos o transform.
    "transforms.outbox.schema.registry.url": "http://schema-registry:8081",  // (5) informamos qual a url do schema registry.
    "transforms.outbox.table.column.payload": "message_payload",  // (6) informamos qual a coluna que tem gravado o payload da mensagem.
    "transforms.outbox.table.column.key": "message_key",  // (7) informamos qual a coluna que grava a chave que será usada para enviar a mensagem.
    "transforms.outbox.table.column.topic": "message_topic"  // (8) informamos qual a coluna que grava o tópico que aquela mensagem pertence
  }
}
```

Descrição de todos os parâmetros:

| Nome                        | Obrigatório | Descrição |
|---------------------------- |:-------------:| -----:|
| schema.registry.url         | sim           | Endpoint do schema registry. |
| schema.cache.ttl            | não           | Tempo em minutos que o schema do tópico vai ficar cacheado, default é 60 minutos. Na prática é o tempo máximo para que uma mudança de schema na gravação do schema demora para replicar para o tópico |
| table.column.payload        | sim           | Nome da coluna que tem os dados da mensagem |`| table.column.payload.encode | não           | Como o payload está encodado na tabela, opções possíveis são base64 (valor default) e byte_array) |
| table.column.key            | sim           | Nome da coluna que tem a chave da mensagem. Não é a PK da tabela, é a chave que será usada como partition key no envio da mensagem |
| table.column.topic          | não           | Nome da coluna que tem o nome do tópico que deve ser enviado a mensagem. Apesar de opcional, se não for informado, deve ser informado o parâmetro routing.topic |
| routing.topic               | não           | Nome do tópico que deve ser encaminhado a mensagem, sobrescreve table.column.topic. Use se você tem várias tabelas de outbox ou vai filtrar os eventos de cada tópico via query |



