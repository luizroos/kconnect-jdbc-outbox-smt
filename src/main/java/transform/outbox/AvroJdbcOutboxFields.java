package transform.outbox;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import transform.TransformField;

public class AvroJdbcOutboxFields {

	static TransformField FIELD_SCHEMA_REGISTRY = new TransformField(1, //
			"outbox.schema", //
			"schema.registry.url", //
			"Endpoint Schema Registry Field", //
			"The column which contains the event schema version endpoint", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			null);

	static TransformField FIELD_SCHEMA_CACHE_TTL = new TransformField(2, //
			"outbox.schema", //
			"schema.cache.ttl", //
			"schema cache ttl (in minutes)", //
			"schema cache ttl (in minutes)", //
			ConfigDef.Type.INT, //
			ConfigDef.Importance.LOW, //
			ConfigDef.Width.SHORT, //
			60);

	static TransformField FIELD_KEY_COLUMN = new TransformField(3, //
			"outbox.table", //
			"table.column.key", //
			"message key column", //
			"The column which contains the message key within the outbox table", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			null);

	static TransformField FIELD_PAYLOAD_COLUMN = new TransformField(4, //
			"outbox.table", //
			"table.column.payload", //
			"message payload column", //
			"The column which contains the message payload within the outbox table", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			null);

	static TransformField FIELD_PAYLOAD_ENCODE = new TransformField(4, //
			"outbox.table", //
			"table.column.payload.encode", //
			"message payload encode", //
			"The way how the payload is encoded (valid values are base64 or byte_array). Default value is base64", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			"base64");

	static TransformField FIELD_TOPIC_COLUMN = new TransformField(5, //
			"outbox.table", //
			"table.column.topic", //
			"message topic column", //
			"The column which contains the message topic within the outbox table", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.MEDIUM, //
			ConfigDef.Width.MEDIUM, //
			null);

	static TransformField FIELD_ROUTING_TOPIC = new TransformField(6, //
			"outbox", //
			"routing.topic", //
			"routing topic", //
			"The routing topic for all messages (overrides the table column topic)", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.MEDIUM, //
			ConfigDef.Width.MEDIUM, //
			null);

	static TransformField FIELD_HEADERS_COLUMNS = new TransformField(7, //
			"outbox.table", //
			"table.column.headers", //
			"columns headers mapping", //
			"All columns that should be add to a event header", //
			ConfigDef.Type.LIST, //
			ConfigDef.Importance.LOW, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final List<TransformField> ALL_FIELDS = Arrays.asList(AvroJdbcOutboxFields.FIELD_KEY_COLUMN, //
			AvroJdbcOutboxFields.FIELD_PAYLOAD_COLUMN, //
			AvroJdbcOutboxFields.FIELD_SCHEMA_REGISTRY, //
			AvroJdbcOutboxFields.FIELD_SCHEMA_CACHE_TTL, //
			AvroJdbcOutboxFields.FIELD_TOPIC_COLUMN);

	private final Map<String, ?> transformConfigMap;

	public AvroJdbcOutboxFields(Map<String, ?> transformConfigMap) {
		this.transformConfigMap = transformConfigMap;
	}

	public String getMessageKeyField() {
		return FIELD_KEY_COLUMN.getReqString(transformConfigMap);
	}

	public String getMessagePayloadField() {
		return FIELD_PAYLOAD_COLUMN.getReqString(transformConfigMap);
	}

	public String getMessagePayloadEncodeField() {
		return FIELD_PAYLOAD_ENCODE.getReqString(transformConfigMap);
	}

	public String getMessageTopicField() {
		return FIELD_TOPIC_COLUMN.getString(transformConfigMap);
	}

	public String getSchemaRegistryUrlField() {
		return FIELD_SCHEMA_REGISTRY.getReqString(transformConfigMap);
	}

	public int getSchemaCacheTtlField() {
		return FIELD_SCHEMA_CACHE_TTL.getReqInteger(transformConfigMap);
	}

	public String getRoutingTopic() {
		return FIELD_ROUTING_TOPIC.getString(transformConfigMap);
	}

	public List<String> getColumnsHeaders() {
		return FIELD_HEADERS_COLUMNS.getList(transformConfigMap);
	}

}
