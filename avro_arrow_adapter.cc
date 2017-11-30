#include <sstream>
#include <avro.h>
#include "arrow/api.h"
#include "arrow/table_builder.h"

using namespace arrow;

#define RETURN_AVRO_ERROR(res)           \
  do {                                   \
    if (res != 0) {                      \
        return Status::IOError(avro_strerror()); \
    } \
  } while (false) \


std::shared_ptr<DataType> get_arrow_type(avro_schema_t schema) {
    auto avro_type = schema->type;
    switch (avro_type) {
        case AVRO_BOOLEAN:
            return boolean();
        case AVRO_INT32:
            return int32();
        case AVRO_INT64:
            return int64();
        case AVRO_FLOAT:
            return float32();
        case AVRO_DOUBLE:
            return float64();
        case AVRO_STRING:
            return utf8();
        case AVRO_BYTES:
            return binary();
        case AVRO_FIXED: {
                             auto length = avro_schema_fixed_size(schema);
                             return fixed_size_binary(length);
                         }
        case AVRO_ARRAY: {
                             auto elem_type = get_arrow_type(avro_schema_array_items(schema));
                             return list(elem_type);
                         }

        case AVRO_MAP: {
                           auto value_type = get_arrow_type(avro_schema_map_values(schema));
                           auto fields = {field("key", utf8()), field("value", value_type)};
                           return list(struct_(fields));
                       }

        case AVRO_UNION: {
                             size_t size = avro_schema_union_size(schema);
                             auto fields = std::vector<std::shared_ptr<Field>>();
                             std::vector<uint8_t> type_codes;

                             for (size_t idx = 0; idx < size; idx++) {
                                 auto child = avro_schema_union_branch(schema, idx);
                                 auto f = field("_union_" + std::to_string(idx),
                                         get_arrow_type(child));
                                 fields.push_back(f);
                                 type_codes.push_back((uint8_t) idx);
                             };
                             return union_(fields, type_codes);
                         }

        case AVRO_RECORD: {
                              size_t size = avro_schema_record_size(schema);
                              auto fields = std::vector<std::shared_ptr<Field>>();
                              for (size_t idx = 0; idx < size; idx++) {
                                  auto child = avro_schema_record_field_get_by_index(schema, idx);
                                  auto elem_type = get_arrow_type(child);
                                  auto name = avro_schema_record_field_name(schema, idx);
                                  auto f = field(name, elem_type);
                                  fields.insert(fields.end(), f);
                              }
                              return struct_(fields);
                          }
        case AVRO_NULL:return null();
        case AVRO_ENUM: break;
        case AVRO_LINK: break;
    }
    // TODO: unhandled case ??
    return null();
}

std::shared_ptr<Schema> avro_to_arrow(avro_schema_t schema) {
    size_t size = avro_schema_record_size(schema);
    auto fields = std::vector<std::shared_ptr<Field>>();
    for (size_t idx = 0; idx < size; idx++) {
        auto child = avro_schema_record_field_get_by_index(schema, idx);
        auto elem_type = get_arrow_type(child);
        auto name = avro_schema_record_field_name(schema, idx);
        auto f = field(name, elem_type);
        fields.insert(fields.end(), f);
    }
    return std::make_shared<Schema>(fields, nullptr);;
}

Status generic_read(const avro_value_t val, ArrayBuilder *builder);

Status read_string(const avro_value_t val, ArrayBuilder *builder) {
  size_t str_size;
  const char *c_str = NULL;
  RETURN_AVRO_ERROR(avro_value_get_string(&val, &c_str, &str_size));
  return static_cast<StringBuilder *>(builder)->Append(c_str);
}

Status read_binary(const avro_value_t val, ArrayBuilder *builder) {
  size_t buf_len;
  const void *buf;

  RETURN_AVRO_ERROR(avro_value_get_bytes(&val, &buf, &buf_len));
  return static_cast<BinaryBuilder *>(builder)->Append(reinterpret_cast<const uint8_t *>(buf), buf_len);
};

Status read_fixed(const avro_value_t val, ArrayBuilder *builder) {
  size_t buf_len;
  const void *buf = NULL;
  RETURN_AVRO_ERROR(avro_value_get_fixed(&val, &buf, &buf_len));
  return static_cast<FixedSizeBinaryBuilder *>(builder)->Append(reinterpret_cast<const uint8_t *>(buf));
}

Status read_int32(const avro_value_t val, ArrayBuilder *builder) {
  int32_t out;
  RETURN_AVRO_ERROR(avro_value_get_int(&val, &out));
  return static_cast<Int32Builder *>(builder)->Append(out);
}

Status read_int64(const avro_value_t val, ArrayBuilder *builder) {
  int64_t out;
  RETURN_AVRO_ERROR(avro_value_get_long(&val, &out));
  return static_cast<Int64Builder *>(builder)->Append(out);
}

Status read_float64(const avro_value_t val, ArrayBuilder *builder) {
  double out;
  RETURN_AVRO_ERROR(avro_value_get_double(&val, &out));
  return static_cast<DoubleBuilder *>(builder)->Append(out);
}

Status read_float32(avro_value_t val, ArrayBuilder *builder) {
  float out;
  RETURN_AVRO_ERROR(avro_value_get_float(&val, &out));
  return static_cast<FloatBuilder *>(builder)->Append(out);
}

Status read_bool(const avro_value_t val, ArrayBuilder *builder) {
  int temp;
  RETURN_AVRO_ERROR(avro_value_get_boolean(&val, &temp));
  auto out = (bool) temp;
  return static_cast<BooleanBuilder *>(builder)->Append(out);
}

Status read_array(const avro_value_t val, ArrayBuilder *builder) {
  size_t array_size;
  size_t i;
  const char *map_key = NULL;
  avro_value_t child;
  // ArrayBuilder *child_builder;//

  RETURN_AVRO_ERROR(avro_value_get_size(&val, &array_size));
  static_cast<ListBuilder *>(builder)->Append(true);
  auto child_builder = static_cast<ListBuilder *>(builder)->value_builder();
  for (i = 0; i < array_size; i++) {
    RETURN_AVRO_ERROR(avro_value_get_by_index(&val, i, &child, &map_key));
    RETURN_NOT_OK(generic_read(child, child_builder));
  }
  return Status::OK();
};

Status read_map(const avro_value_t val, ArrayBuilder *builder) {
  size_t num_values, i;
  auto listB = static_cast<ListBuilder *>(builder);
  auto structB = static_cast<StructBuilder *>(listB->child(0));
  auto keyBuilder = static_cast<StringBuilder *>(structB->child(0));
  auto valueBuilder = structB->child(1);
  avro_value_t child;
  const char *map_key;

  avro_value_get_size(&val, &num_values);

  listB->Append(true);
  for (i = 0; i < num_values; i++) {
    structB->Append(true);
    RETURN_AVRO_ERROR(avro_value_get_by_index(&val, i, &child, &map_key));
    RETURN_NOT_OK(keyBuilder->Append(map_key));
    RETURN_NOT_OK(generic_read(child, valueBuilder));
  }
  return Status::OK();
}

Status read_record(const avro_value_t val, ArrayBuilder *builder) {
    avro_value_t child;
    ArrayBuilder *child_builder;
    StructBuilder *typed_builder;
    Status result;

    int container_length = builder->num_children();
    typed_builder = static_cast<StructBuilder *>(builder);
    RETURN_NOT_OK(typed_builder->Append());

    for (auto i = 0; i < container_length; i++) {
        avro_value_get_by_index(&val, i, &child, NULL);
        child_builder = typed_builder->child(i);
        RETURN_NOT_OK(generic_read(child, child_builder));
    }
    return Status::OK();
}

Status generic_read(const avro_value_t val, ArrayBuilder *builder) {
    // Generic avro type read dispatcher. Dispatches to the various specializations by AVRO_TYPE.
    // This is used by the various readers for complex types"""
    auto avro_type = avro_value_get_type(&val);

    switch (avro_type) {
        case AVRO_BOOLEAN:
            return read_bool(val, builder);
        case AVRO_INT32:
            return read_int32(val, builder);
        case AVRO_INT64:
            return read_int64(val, builder);
        case AVRO_FLOAT:
            return read_float32(val, builder);
        case AVRO_DOUBLE:
            return read_float64(val, builder);
        case AVRO_STRING:
            return read_string(val, builder);
        case AVRO_BYTES:
            return read_binary(val, builder);
        case AVRO_FIXED:
            return read_fixed(val, builder);
        case AVRO_MAP:
            return read_map(val, builder);
        case AVRO_RECORD:
            return read_record(val, builder);
        case AVRO_ARRAY:
            return read_array(val, builder);
        default:
            std::stringstream ss;
            ss << "Unhandled avro type: " << avro_type;
            return Status::NotImplemented(ss.str());
    }
}

Status read_record(const avro_value_t val, RecordBatchBuilder *builder) {
    avro_value_t child;
    int container_length = builder->num_fields();
    for (auto i = 0; i < container_length; i++) {
        avro_value_get_by_index(&val, i, &child, NULL);
        RETURN_NOT_OK(generic_read(child, builder->GetField(i)));
    }
    return Status::OK();
}
