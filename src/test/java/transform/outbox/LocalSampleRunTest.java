package transform.outbox;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;

class LocalSampleRunTest {

    @Test
    @Disabled
    void decodeSample() throws RestClientException, IOException {
        String message = "AAAAAAdIYmM1NGJiZjEtMGY2OC00ZmIwLWJiYzItMzZiZWViYTc1NzViCnRlc3RlAhBFVmpJRXdueQ==";
        byte[] b = Base64.getDecoder().decode(message);
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:8081", 10);
        AvroJdbcOutbox.Deserializer deserializer = new AvroJdbcOutbox.Deserializer(schemaRegistryClient);
        GenericContainerWithVersion aa = deserializer.deserialize("user.changed", false, b);
        System.out.println(aa.container());
        SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata("user.changed-value");
        org.apache.avro.Schema schema = schemaRegistryClient.getById(latestSchemaMetadata.getId());
        GenericContainer rec = SpecificData.get().deepCopy(schema, aa.container());

        AvroDataConfig avroDataConfig = new AvroDataConfig.Builder().with("schemas.cache.config", 10).build();
        AvroData avroData = new AvroData(avroDataConfig);
        SchemaAndValue schemaAndValue = avroData.toConnectData(schema, rec);
        System.out.println(schemaAndValue);
        System.out.println(rec);
    }
}