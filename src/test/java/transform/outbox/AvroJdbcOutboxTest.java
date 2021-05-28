package transform.outbox;

import java.util.Base64;
import java.util.Collections;
import java.util.UUID;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Cast;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroJdbcOutboxTest {

	@Test
	void inputTimestampToHeaderWithRightType() {
		final Cast<SourceRecord> xform = new Cast.Value<>();
		xform.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int64:boolean"));

		SchemaBuilder builder = SchemaBuilder.struct();
		builder.field("timestamp", Timestamp.SCHEMA);
		Schema supportedTypesSchema = builder.build();

		Struct recordValue = new Struct(supportedTypesSchema);
		recordValue.put("timestamp", new java.sql.Timestamp(0L));

		final Object timestamp = recordValue.get("timestamp");
		final SourceRecord recordToApplyHeader = new SourceRecord(null, null, "topic", 0, supportedTypesSchema,
			recordValue
		);
		final Field field = recordValue.schema()
			.field("timestamp");

		final String headerKeyExpected = "key";
		recordToApplyHeader.headers()
			.add(headerKeyExpected, new SchemaAndValue(field.schema(), timestamp));

		final Header header = recordToApplyHeader.headers()
			.iterator()
			.next();
		assertEquals(header.key(), headerKeyExpected);
		assertEquals(header.value(), timestamp);
		assertEquals(header.schema(), field.schema());
	}

	@ParameterizedTest
	@ValueSource(strings = {"string", "byte_array", "base64"})
	void validEncodingForKey(String encodeSelected) {
		AvroJdbcOutbox.validateEncodeSelectedFor(encodeSelected, "keyfield", AvroJdbcOutbox.KEY_VALID_ENCODING);
	}

	@ParameterizedTest
	@ValueSource(strings = {"", "another_one"})
	void invalidEncodingForKey(String encodeSelected) {
		final String keyfieldExpected = "keyfield";

		final IllegalStateException ex = assertThrows(IllegalStateException.class,
			() -> AvroJdbcOutbox.validateEncodeSelectedFor(encodeSelected, keyfieldExpected,
				AvroJdbcOutbox.KEY_VALID_ENCODING
			)
		);

		assertEquals("invalid value to " + keyfieldExpected, ex.getMessage());
	}

	@Test
	void invalidNullEncodingForKey() {
		final String encodeSelected = null;
		final String keyfieldExpected = "keyfield";

		final IllegalStateException ex = assertThrows(IllegalStateException.class,
			() -> AvroJdbcOutbox.validateEncodeSelectedFor(encodeSelected, keyfieldExpected,
				AvroJdbcOutbox.KEY_VALID_ENCODING
			)
		);

		assertEquals("invalid value to " + keyfieldExpected, ex.getMessage());
	}

	@Test
	void decodeKeyInString() {
		final String expectedKey = UUID.randomUUID()
			.toString();
		final String keyColumnFieldName = "keyColumn";

		SchemaBuilder builder = SchemaBuilder.struct();
		builder.field(keyColumnFieldName, Schema.STRING_SCHEMA);
		Schema supportedTypesSchema = builder.build();

		Struct recordValue = new Struct(supportedTypesSchema);
		recordValue.put(keyColumnFieldName, expectedKey);

		// when
		final String actualKey = AvroJdbcOutbox.getMessageKey(recordValue, "string", keyColumnFieldName);

		// then
		assertEquals(expectedKey, actualKey);
	}

	@Test
	void decodeKeyInBase64() {
		final String expectedKey = UUID.randomUUID()
			.toString();
		final String expectedKeyEncoded = Base64.getEncoder()
			.encodeToString(expectedKey.getBytes());
		final String keyColumnFieldName = "keyColumn";

		SchemaBuilder builder = SchemaBuilder.struct();
		builder.field(keyColumnFieldName, Schema.STRING_SCHEMA);
		Schema supportedTypesSchema = builder.build();

		Struct recordValue = new Struct(supportedTypesSchema);
		recordValue.put(keyColumnFieldName, expectedKeyEncoded);

		// when
		final String actualKey = AvroJdbcOutbox.getMessageKey(recordValue, "base64", keyColumnFieldName);

		// then
		assertEquals(expectedKey, actualKey);
	}

	@Test
	void decodeKeyInByteArray() {
		final String expectedKey = UUID.randomUUID()
			.toString();
		final byte[] expectedKeyEncoded = expectedKey.getBytes();
		final String keyColumnFieldName = "keyColumn";

		SchemaBuilder builder = SchemaBuilder.struct();
		builder.field(keyColumnFieldName, Schema.BYTES_SCHEMA);
		Schema supportedTypesSchema = builder.build();

		Struct recordValue = new Struct(supportedTypesSchema);
		recordValue.put(keyColumnFieldName, expectedKeyEncoded);

		// when
		final String actualKey = AvroJdbcOutbox.getMessageKey(recordValue, "byte_array", keyColumnFieldName);

		// then
		assertEquals(expectedKey, actualKey);
	}

	@ParameterizedTest
	@ValueSource(strings = {"string", "byte_array", "base64"})
	void generateRandomKeyWhenKeyIsNullAndEncodingIs(final String encodeSelected) {
		final byte[] expectedKeyEncoded = null;
		final String keyColumnFieldName = "keyColumn";

		SchemaBuilder builder = SchemaBuilder.struct();
		builder.field(keyColumnFieldName, Schema.OPTIONAL_STRING_SCHEMA);
		Schema supportedTypesSchema = builder.build();

		Struct recordValue = new Struct(supportedTypesSchema);
		recordValue.put(keyColumnFieldName, expectedKeyEncoded);

		// when
		final String actualKey = AvroJdbcOutbox.getMessageKey(recordValue, encodeSelected, keyColumnFieldName);

		// then
		assertNotNull(actualKey);
		// garante que é um UUID válido.
		assertNotNull(UUID.fromString(actualKey));
	}
}
