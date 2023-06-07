package transform.outbox;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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

	private String messageKeyEncodeField;

	private String messageKeyField;

	private String messageTopicField;

	private Integer schemaCacheTtl;

	private String routingTopic;

	private List<String> headerFields;

	private String partitionField;

	private SchemaRegistryClient schemaRegistryClient;

	private Map<String, AvroSchemaCached> avroSchemasCache = new HashMap<>();

	private AvroData avroData;

	private Deserializer deserializer;

	public static final Set<String> PAYLOAD_VALID_ENCODING = Collections.unmodifiableSet(
		new HashSet<>(Arrays.asList("byte_array", "base64")));
	public static final Set<String> KEY_VALID_ENCODING = Collections.unmodifiableSet(
		new HashSet<>(Arrays.asList("byte_array", "base64", "string")));

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

		this.messageKeyEncodeField = jdbcOutboxFields.getMessageKeyEncodeField();
		LOGGER.info("configure, messageKeyEncodeField={} ", messagePayloadEncode);

		this.messageTopicField = jdbcOutboxFields.getMessageTopicField();
		LOGGER.info("configure, messageTopicField={} ", messageTopicField);

		this.routingTopic = jdbcOutboxFields.getRoutingTopic();
		LOGGER.info("configure, routingTopic={} ", routingTopic);

		this.schemaCacheTtl = jdbcOutboxFields.getSchemaCacheTtlField();
		LOGGER.info("configure, schemaCacheTtl={} ", schemaCacheTtl);

		this.headerFields = jdbcOutboxFields.getColumnsHeaders();
		LOGGER.info("configure, headerFields={} ", headerFields);

		this.partitionField = jdbcOutboxFields.getPartition();
		LOGGER.info("configure, partitionField={} ", this.partitionField);

		final String schemaRegistryUrl = jdbcOutboxFields.getSchemaRegistryUrlField();
		LOGGER.info("configure, schemaRegistry={} ", schemaRegistryUrl);

		this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10);

		final AvroDataConfig avroDataConfig = new AvroDataConfig.Builder().with("schemas.cache.config", 10)
			.build();
		this.avroData = new AvroData(avroDataConfig);

		this.deserializer = new Deserializer(schemaRegistryClient);

		// validações
		validateMessageTopicAndRouting();
		validateEncodeSelectedFor(this.messagePayloadEncode, AvroJdbcOutboxFields.FIELD_PAYLOAD_ENCODE.getName(),
			PAYLOAD_VALID_ENCODING
		);
		validateEncodeSelectedFor(this.messageKeyEncodeField, AvroJdbcOutboxFields.FIELD_KEY_ENCODE.getName(),
			KEY_VALID_ENCODING
		);
	}

	static void validateEncodeSelectedFor(String encodeSelected, String fieldName, Set<String> validEncoding) {
		if (encodeSelected == null || !validEncoding.contains(encodeSelected)) {
			throw new IllegalStateException(String.format("invalid value to %s", fieldName));
		}
	}

	private void validateMessageTopicAndRouting() {
		if (messageTopicField == null && routingTopic == null) {
			throw new IllegalStateException(
				String.format("Either %s or %s must be filled", AvroJdbcOutboxFields.FIELD_ROUTING_TOPIC.getName(),
					AvroJdbcOutboxFields.FIELD_TOPIC_COLUMN.getName()
				));
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

		final String messageKey = getMessageKey(eventStruct, this.messageKeyEncodeField, this.messageKeyField);
		final Object messagePayload = eventStruct.get(messagePayloadField);
		final String messageTopic = getMessageTopic(eventStruct);
		final Integer partitionNumber = getPartitionNumber(eventStruct);

		LOGGER.debug("apply, messageTopic={}, messageKey={}, messagePayload={}", messageTopic, messageKey,
			messagePayload
		);

		// deserializa o que foi serializado com KafkaAvroSerializer
		final byte[] messagePayloadBytes = getBytesDecoded(messagePayload, this.messagePayloadEncode);
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
			partitionNumber, //
			Schema.STRING_SCHEMA, //
			messageKey, //
			schemaAndValue.schema(), //
			schemaAndValue.value(), //
			LocalDateTime.now()
				.toInstant(ZoneOffset.of("Z"))
					.toEpochMilli(), //
			null
		);

		applyHeaders(newRecord, eventStruct);

		LOGGER.debug("apply, recordToApply={}", newRecord);

		return newRecord;
	}

	static byte[] getBytesDecoded(final Object stringEncoded, final String selectedEncoding) {
		if (selectedEncoding.equals("base64")) {
			return Base64.getDecoder()
				.decode(stringEncoded.toString());
		} else {
			return (byte[]) stringEncoded;
		}
	}

	static String getMessageKey(final Struct eventStruct, final String keyEncodingSelected,
		final String keyFieldName) {
		Object keyEncoded = eventStruct.get(keyFieldName);

		// Quando a key é nula, temos problemas no kafka connector,
		// então, melhor aplicarmos um valor qualquer randômico.
		if (keyEncoded == null) {
			keyEncoded = UUID.randomUUID()
				.toString();
		} else {
			// Só precisa decodifica se a key não é string (default).
			if (!keyEncodingSelected.equals("string")) {
				final byte[] keyInBytes = getBytesDecoded(keyEncoded, keyEncodingSelected);
				keyEncoded = new String(keyInBytes);
			}
		}

		return keyEncoded.toString();
	}

	private Integer getPartitionNumber(final Struct eventStruct) {
		if (this.partitionField != null) {
			final String partitionValue = (String) eventStruct.get(this.partitionField);

			if (partitionValue == null) {
				return null;
			}

			return Integer.parseInt(partitionValue);
		}
		return null;
	}

	private void applyHeaders(final R newRecord, final Struct eventStruct) {

		if (this.headerFields == null || this.headerFields.isEmpty()) {
			return;
		}

		for (final String headerKey : this.headerFields) {
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
