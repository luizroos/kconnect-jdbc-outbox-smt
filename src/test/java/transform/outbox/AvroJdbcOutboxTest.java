package transform.outbox;

import java.util.Collections;

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

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
