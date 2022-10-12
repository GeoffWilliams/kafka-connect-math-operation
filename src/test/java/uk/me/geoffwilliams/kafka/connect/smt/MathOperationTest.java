package uk.me.geoffwilliams.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MathOperationTest {

    private final MathOperation<SourceRecord> xform = new MathOperation.Value<>();


    @Test
    public void testIntegerMath() {
        Integer i = 10;
        Integer result;

        result = (Integer) xform.doMath(i, "*", "10");
        assertEquals(100, result);

        result = (Integer) xform.doMath(i, "/", "10");
        assertEquals(1, result);

        result = (Integer) xform.doMath(i, "+", "10");
        assertEquals(20, result);

        result = (Integer) xform.doMath(i, "-", "10");
        assertEquals(0, result);
    }

    @Test
    public void testLongMath() {
        Long l = 10L;
        Long result;

        result = (Long) xform.doMath(l, "*", "10");
        assertEquals(100, result);

        result = (Long) xform.doMath(l, "/", "10");
        assertEquals(1, result);

        result = (Long) xform.doMath(l, "+", "10");
        assertEquals(20, result);

        result = (Long) xform.doMath(l, "-", "10");
        assertEquals(0, result);
    }

    @Test
    public void testFloatMath() {
        Float f = 10.10f;
        Float result;

        result = (Float) xform.doMath(f, "*", "10");
        assertEquals(101.0f, result);

        result = (Float) xform.doMath(f, "/", "10");
        assertEquals(1.01f, result);

        result = (Float) xform.doMath(f, "+", "10");
        assertEquals(20.10f, result);

        result = (Float) xform.doMath(f, "-", "10");
        assertEquals(0.10f, result);
    }

    @Test
    public void testDoubleMath() {
        Double d = 10.10d;
        Double result;

        result = (Double) xform.doMath(d, "*", "10");
        assertEquals(101.0d, Math.round(result*100)/100d);

        result = (Double) xform.doMath(d, "/", "10");
        assertEquals(1.01d, result);

        result = (Double) xform.doMath(d, "+", "10");
        assertEquals(20.10d, result);

        result = (Double) xform.doMath(d, "-", "10");
        assertEquals(0.10d, Math.round(result * 100)/100d);
    }

    @Test
    public void schemaMathOperation() {
        final Map<String, Object> props = new HashMap<>();

        props.put("operator", "+");
        props.put("operand", "1");
        props.put("field", "myfield");

        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("myfield", Schema.OPTIONAL_INT64_SCHEMA)
                .field("another_field", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        final Struct simpleStruct = new Struct(simpleStructSchema)
                .put("myfield", 1L)
                .put("another_field", 6L);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);

        // shouldn't be any schema changes but check anyway
        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("myfield").schema());
        assertEquals(2, ((Struct) transformedRecord.value()).getInt64("myfield"));
        assertEquals(6, ((Struct) transformedRecord.value()).getInt64("another_field"));

    }

    @Test
    public void schemalessMathOperation() {
        final Map<String, Object> props = new HashMap<>();

        props.put("operator", "+");
        props.put("operand", "1");
        props.put("field", "myfield");

        xform.configure(props);
        final Map<String, Object> data = new HashMap<>();
        data.put("myfield",1);
        data.put("another_field",6);
        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, data);

        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals(2, ((Map<?, ?>) transformedRecord.value()).get("myfield"));
        assertEquals(6, ((Map<?, ?>) transformedRecord.value()).get("another_field"));
    }

}
