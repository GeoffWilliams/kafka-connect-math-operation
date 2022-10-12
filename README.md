# kafka-connect-math-operation

An SMT for kafka connect to perform math operation on a connect field.

## Supported Operators
`*`, `/`, `+`, `-`

## Data types
* Does not attempt to convert datatypes (eg `integer` divided by `float`) will still result in an integer
* If type conversion is needed, chain a [cast](https://docs.confluent.io/platform/current/connect/transforms/cast.html)
  SMT before or after this SMT, as required.

## Example usage

_Multiply the `time` field by `1000` (eg to convert seconds since epoch to ms since epoch)_

```json
  "transforms.multiplyTime.type": "uk.me.geoffwilliams.kafka.connect.smt.MathOperation$Value",
  "transforms.multiplyTime.field": "time",
  "transforms.multiplyTime.operator": "*",
  "transforms.multiplyTime.operand": "1000",
```

## Building

```shell
gradle shadowJar
```