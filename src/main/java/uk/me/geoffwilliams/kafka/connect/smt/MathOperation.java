package uk.me.geoffwilliams.kafka.connect.smt;

import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class MathOperation<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Perform math operation on a connect field";

    interface ConfigName {
        String OPERATOR = "operator";
        String OPERAND = "operand";
        String FIELD = "field";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.OPERATOR, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                "Math operation to carry out one of (*,/,+,-)")
        .define(ConfigName.OPERAND, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                "Operand to use with operator and field")
        .define(ConfigName.FIELD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                "Field to apply math operation to");

    private static final String PURPOSE = "math operation on field";

    private static final Set<String> SUPPORTED_OPERATIONS = Set.of("*","/", "+", "-");

    private String field;
    private String operator;
    private String operand;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        field = config.getString(ConfigName.FIELD);
        operator = config.getString(ConfigName.OPERATOR);
        operand = config.getString(ConfigName.OPERAND);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }


    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final HashMap<String, Object> updatedValue = new HashMap<>(value);

        Object updatedFieldValue = doMath(value.get(field), operator, operand);
        updatedValue.put(field, updatedFieldValue);
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());
        final Field connectField = value.schema().field(field);
        final Object origFieldValue = value.get(field);
        Object updatedFieldValue = doMath(origFieldValue, operator, operand);
        updatedValue.put(connectField, updatedFieldValue);

        return newRecord(record, updatedValue);
    }

    Object doMath(Object origFieldValue, String operator, String operand) {
        Expression expression = new ExpressionBuilder(String.format(origFieldValue.toString() + operator + operand)).build();
        double doubleResult =  expression.evaluate();
        Number result;

        // we are not changing the schema so integers need to stay integers
        if (origFieldValue instanceof Integer) {
            result = (int) doubleResult;
        } else if (origFieldValue instanceof Long) {
            result = (long) doubleResult;
        } else if (origFieldValue instanceof Float) {
            result = (float) doubleResult;
        } else {
            result = doubleResult;
        }
        return result;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R base, Object value);

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    public static class Value<R extends ConnectRecord<R>> extends MathOperation<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }

    }
}
