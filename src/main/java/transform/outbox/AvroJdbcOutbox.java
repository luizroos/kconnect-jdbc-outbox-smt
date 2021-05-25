package transform.outbox;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import transform.TransformField;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class AvroJdbcOutbox<R extends ConnectRecord<R>> implements Transformation<R> {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroJdbcOutbox.class);

	private static final String SCHEMA_REGISTRY_TOPIC_SUFFIX = "-value";

	private String messagePayloadField;

	private String messagePayloadEncode;

	private String messageKeyField;

	private String messageTopicField;

	private Integer schemaCacheTtl;

	private String routingTopic;

	private List<String> columnsHeaders;

	private SchemaRegistryClient schemaRegistryClient;

	private Map<String, AvroSchemaCached> avroSchemasCache = new HashMap<>();

	private AvroData avroData;

	private Deserializer deserializer;

	@Override
	public void configure(Map<String, ?> configMap) {
		configMap.forEach((k, v) -> LOGGER.info(("key {" + k + "} , value {" + v + "}")));

		final AvroJdbcOutboxFields jdbcOutboxFields = new AvroJdbcOutboxFields(configMap);

		this.messagePayloadField = jdbcOutboxFields.getMessagePayloadField();
		LOGGER.info("configure, messagePayloadField={} ", messagePayloadField);

		this.messageKeyField = jdbcOutboxFields.getMessageKeyField();
		LOGGER.info("configure, messageKeyField={} ", messageKeyField);

		this.messagePayloadEncode = jdbcOutboxFields.getMessagePayloadEncodeField();
		LOGGER.info("configure, messagePayloadEncode={} ", messagePayloadEncode);

		this.messageTopicField = jdbcOutboxFields.getMessageTopicField();
		LOGGER.info("configure, messageTopicField={} ", messageTopicField);

		this.routingTopic = jdbcOutboxFields.getRoutingTopic();
		LOGGER.info("configure, routingTopic={} ", routingTopic);

		this.schemaCacheTtl = jdbcOutboxFields.getSchemaCacheTtlField();
		LOGGER.info("configure, schemaCacheTtl={} ", schemaCacheTtl);

		this.columnsHeaders = jdbcOutboxFields.getColumnsHeaders();
		LOGGER.info("configure, columnsHeaders={} ", columnsHeaders);

		final String schemaRegistryUrl = jdbcOutboxFields.getSchemaRegistryUrlField();
		LOGGER.info("configure, schemaRegistry={} ", schemaRegistryUrl);

		this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10);

		final AvroDataConfig avroDataConfig = new AvroDataConfig.Builder().with("schemas.cache.config", 10)
			.build();
		this.avroData = new AvroData(avroDataConfig);

		this.deserializer = new Deserializer(schemaRegistryClient);

		// validações
		if (messageTopicField == null && routingTopic == null) {
			throw new IllegalStateException(
				String.format("Either %s or %s must be filled", AvroJdbcOutboxFields.FIELD_ROUTING_TOPIC.getName(),
					AvroJdbcOutboxFields.FIELD_TOPIC_COLUMN.getName()
				));
		}

		if (messagePayloadEncode.equals("byte_array") && messagePayloadEncode.equals("base64")) {
			throw new IllegalStateException(
				String.format("invalid value to %s", AvroJdbcOutboxFields.FIELD_PAYLOAD_ENCODE.getName()));
		}

	}

	private final org.apache.avro.Schema getTopicAvroSchema(String topic) {
		return Optional.ofNullable(avroSchemasCache.get(topic)) //
			.filter(schema -> schema.getCachedTime()
				.isAfter(LocalDateTime.now()
					.minusMinutes(schemaCacheTtl))) //
			.map(AvroSchemaCached::getSchema)//
			.orElseGet(() -> {
				LOGGER.debug("getTopicAvroSchema, updating_schema, topic={}", topic);
				synchronized (topic) {
					try {
						final SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(
							String.format("%s%s", topic, SCHEMA_REGISTRY_TOPIC_SUFFIX));
						final org.apache.avro.Schema schema = schemaRegistryClient.getById(
							latestSchemaMetadata.getId());
						avroSchemasCache.put(topic, new AvroSchemaCached(schema));
						return schema;
					} catch (IOException | RestClientException e) {
						e.printStackTrace();
						throw new ConnectException("Unable to connect to schema-registry", e);
					}
				}
			});
	}

	private String getMessageTopic(Struct eventStruct) {
		if (routingTopic != null) {
			return routingTopic;
		}
		return eventStruct.getString(messageTopicField);
	}

	@Override
	public R apply(R recordToApply) {
		if (recordToApply.value() == null) {
			LOGGER.error("recordToApply null value, messageKey={}", recordToApply.key());
			return null;
		}

		final Struct eventStruct = requireStruct(recordToApply.value(), "Read Outbox Event");
		LOGGER.debug("apply, eventStruct={}", eventStruct);

		final String messageKey = eventStruct.getString(messageKeyField);
		final Object messagePayload = eventStruct.get(messagePayloadField);
		final String messageTopic = getMessageTopic(eventStruct);

		LOGGER.debug("apply, messageTopic={}, messageKey={}, messagePayload={}", messageTopic, messageKey,
			messagePayload
		);

		// deserializa o que foi serializado com KafkaAvroSerializer
		final byte[] messagePayloadBytes;
		if (messagePayloadEncode.equals("base64")) {
			messagePayloadBytes = Base64.getDecoder()
				.decode(messagePayload.toString());
		} else {
			messagePayloadBytes = (byte[]) messagePayload;
		}
		final GenericContainerWithVersion genericContainer = deserializer.deserialize(messageTopic, false,
			messagePayloadBytes
		);

		final org.apache.avro.Schema schemaTransform = getTopicAvroSchema(messageTopic);
		final GenericContainer avroMessage = SpecificData.get()
			.deepCopy(schemaTransform, genericContainer.container());
		LOGGER.debug("apply, avroMessage={}", avroMessage);

		final SchemaAndValue schemaAndValue = avroData.toConnectData(schemaTransform, avroMessage);
		LOGGER.debug("apply, schemaAndValue={}", genericContainer);

		final R newRecord = recordToApply.newRecord(messageTopic, //
			null, //
			Schema.STRING_SCHEMA, //
			messageKey, //
			schemaAndValue.schema(), //
			schemaAndValue.value(), //
			LocalDateTime.now()
				.toEpochSecond(ZoneOffset.of("Z")), //
			null
		);

		applyHeaders(newRecord, eventStruct);

		LOGGER.debug("apply, recordToApply={}", newRecord);

		return newRecord;
	}

	private void applyHeaders(final R newRecord, final Struct eventStruct) {

		if (this.columnsHeaders == null || this.columnsHeaders.isEmpty()) {
			return;
		}

		for (final String headerKey : this.columnsHeaders) {
			final Field field = eventStruct.schema()
				.field(headerKey);
			final Object headerValue = eventStruct.get(headerKey);
			newRecord.headers()
				.add(headerKey, new SchemaAndValue(field.schema(), headerValue));
		}
	}

	@Override
	public ConfigDef config() {
		final ConfigDef config = new ConfigDef();
		for (TransformField field : AvroJdbcOutboxFields.ALL_FIELDS) {
			field.define(config);
		}
		return config;
	}

	@Override
	public void close() {
	}

	private static class AvroSchemaCached {

		private final LocalDateTime cachedTime;

		private final org.apache.avro.Schema schema;

		public AvroSchemaCached(org.apache.avro.Schema schema) {
			this.cachedTime = LocalDateTime.now();
			this.schema = Objects.requireNonNull(schema);
		}

		public LocalDateTime getCachedTime() {
			return cachedTime;
		}

		public org.apache.avro.Schema getSchema() {
			return schema;
		}

	}

	protected static class Deserializer extends AbstractKafkaAvroDeserializer {

		public Deserializer(SchemaRegistryClient client) {
			schemaRegistry = client;
		}

		public GenericContainerWithVersion deserialize(String topic, boolean isKey, byte[] payload) {
			return deserializeWithSchemaAndVersion(topic, isKey, payload);
		}
	}
}
