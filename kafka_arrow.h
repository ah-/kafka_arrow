#include <string>
#include <sstream>

extern "C" {
#include <libserdes/serdes.h>
#include <libserdes/serdes-avro.h>
}
#include <librdkafka/rdkafka.h>
#include <avro.h>
#include "arrow/api.h"
#include "arrow/table_builder.h"

using namespace arrow;

#define FATAL(reason...) do {                           \
                fprintf(stderr, "FATAL: " reason);      \
                exit(1);                                \
        } while (0)

std::shared_ptr<arrow::DataType> get_arrow_type(avro_schema_t schema) {
    auto avro_type = schema->type;
    switch (avro_type) {
        case AVRO_BOOLEAN:
            return arrow::boolean();
        case AVRO_INT32:
            return arrow::int32();
        case AVRO_INT64:
            return arrow::int64();
        case AVRO_FLOAT:
            return arrow::float32();
        case AVRO_DOUBLE:
            return arrow::float64();
        case AVRO_STRING:
            return arrow::utf8();
        case AVRO_BYTES:
            return arrow::binary();
        case AVRO_FIXED: {
                             auto length = avro_schema_fixed_size(schema);
                             return arrow::fixed_size_binary(length);
                         }
        case AVRO_ARRAY: {
                             auto elem_type = get_arrow_type(avro_schema_array_items(schema));
                             return arrow::list(elem_type);
                         }

        case AVRO_MAP: {
                           auto value_type = get_arrow_type(avro_schema_map_values(schema));
                           auto fields = {field("key", arrow::utf8()), field("value", value_type)};
                           return arrow::list(arrow::struct_(fields));
                       }

        case AVRO_UNION: {
                             size_t size = avro_schema_union_size(schema);
                             auto fields = std::vector<std::shared_ptr<arrow::Field>>();
                             std::vector<uint8_t> type_codes;

                             for (size_t idx = 0; idx < size; idx++) {
                                 auto child = avro_schema_union_branch(schema, idx);
                                 auto f = field("_union_" + std::to_string(idx),
                                         get_arrow_type(child));
                                 fields.push_back(f);
                                 type_codes.push_back((uint8_t) idx);
                             };
                             return arrow::union_(fields, type_codes);
                         }

        case AVRO_RECORD: {
                              size_t size = avro_schema_record_size(schema);
                              auto fields = std::vector<std::shared_ptr<arrow::Field>>();
                              for (size_t idx = 0; idx < size; idx++) {
                                  auto child = avro_schema_record_field_get_by_index(schema, idx);
                                  auto elem_type = get_arrow_type(child);
                                  auto name = avro_schema_record_field_name(schema, idx);
                                  auto f = field(name, elem_type);
                                  fields.insert(fields.end(), f);
                              }
                              return arrow::struct_(fields);
                          }
        case AVRO_NULL:return arrow::null();
        case AVRO_ENUM: break;
        case AVRO_LINK: break;
    }
    // TODO: unhandled case ??
    return arrow::null();
}

std::shared_ptr<arrow::Schema> avro_to_arrow(avro_schema_t schema) {
    size_t size = avro_schema_record_size(schema);
    auto fields = std::vector<std::shared_ptr<arrow::Field>>();
    for (size_t idx = 0; idx < size; idx++) {
        auto child = avro_schema_record_field_get_by_index(schema, idx);
        auto elem_type = get_arrow_type(child);
        auto name = avro_schema_record_field_name(schema, idx);
        auto f = field(name, elem_type);
        fields.insert(fields.end(), f);
    }
    return std::make_shared<arrow::Schema>(fields, nullptr);;
}

arrow::Status read_string(const avro_value_t val, arrow::ArrayBuilder *builder) {
  size_t str_size;
  const char *c_str = NULL;
  //RETURN_AVRO_ERROR(avro_value_get_string(&val, &c_str, &str_size));
  avro_value_get_string(&val, &c_str, &str_size);
  return static_cast<arrow::StringBuilder *>(builder)->Append(c_str);
}

/*
Status read_binary(const avro_value_t val, ArrayBuilder *builder) {
  size_t buf_len;
  const void *buf;

  RETURN_AVRO_ERROR(avro_value_get_bytes(&val, &buf, &buf_len));
  return static_cast<BinaryBuilder *>(builder)->Append(reinterpret_cast<uint8_t *>(buf), buf_len);
};

Status read_fixed(const avro_value_t val, ArrayBuilder *builder) {
  size_t buf_len;
  const void *buf = NULL;
  RETURN_AVRO_ERROR(avro_value_get_fixed(&val, &buf, &buf_len));
  return static_cast<FixedSizeBinaryBuilder *>(builder)->Append(reinterpret_cast<uint8_t *>(buf));
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
*/

arrow::Status generic_read(const avro_value_t val, arrow::ArrayBuilder *builder) {
    // Generic avro type read dispatcher. Dispatches to the various specializations by AVRO_TYPE.
    // This is used by the various readers for complex types"""
    auto avro_type = avro_value_get_type(&val);

    switch (avro_type) {
        /*
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
            */
        case AVRO_STRING:
            return read_string(val, builder);
            /*
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
            */
        default:
            std::stringstream ss;
            ss << "Unhandled avro type: " << avro_type;
            return arrow::Status::NotImplemented(ss.str());
    }
}

arrow::Status read_record(const avro_value_t val, arrow::ArrayBuilder *builder) {
    avro_value_t child;
    arrow::ArrayBuilder *child_builder;
    arrow::StructBuilder *typed_builder;
    arrow::Status result;

    int container_length = builder->num_children();
    typed_builder = static_cast<arrow::StructBuilder *>(builder);
    RETURN_NOT_OK(typed_builder->Append());

    for (auto i = 0; i < container_length; i++) {
        avro_value_get_by_index(&val, i, &child, NULL);
        child_builder = typed_builder->child(i);
        RETURN_NOT_OK(generic_read(child, child_builder));
    }
    return arrow::Status::OK();
}

arrow::Status read_record(const avro_value_t val, arrow::RecordBatchBuilder *builder) {
    avro_value_t child;
    int container_length = builder->num_fields();
    for (auto i = 0; i < container_length; i++) {
        avro_value_get_by_index(&val, i, &child, NULL);
        RETURN_NOT_OK(generic_read(child, builder->GetField(i)));
    }
    return arrow::Status::OK();
}

std::shared_ptr<arrow::RecordBatch> poll_arrow(void *rkv, int max_messages) {
    rd_kafka_t *rk = (rd_kafka_t*) rkv;

    char errstr[512];
    serdes_conf_t *sconf = serdes_conf_new(NULL, 0, "schema.registry.url", "http://localhost:8081", NULL);

    serdes_err_t err;
    serdes_t *serdes = serdes_new(sconf, errstr, sizeof(errstr));
    if (!serdes) {
        FATAL("serdes_new");
    }

    int num_messages = 0;
    int current_schema_id = -1;

    std::unique_ptr<arrow::RecordBatchBuilder> builder;

    while (num_messages < max_messages) {
        rd_kafka_message_t *rkm; 
        rkm = rd_kafka_consumer_poll(rk, 1000);

        if (!rkm) {
            break;
        } else {
            if (rkm->err) {
                if (rkm->err == RD_KAFKA_RESP_ERR__PARTITION_EOF){
                    // ignore
                } else {
                    fprintf(stderr, "Consumed message error: %s\n",
                            rd_kafka_message_errstr(rkm));
                    // do something about this?
                    FATAL("message error\n");
                }
            }

            if (rkm->len == 0) {
                FATAL("short msg\n");
            }

            avro_value_t avro;
            serdes_schema_t *schema;
            err = serdes_deserialize_avro(serdes, &avro, &schema, rkm->payload, rkm->len, errstr, sizeof(errstr));
            if (err) {
                fprintf(stderr, "%s", errstr);
                FATAL("serdes_deserialize_arrow error");
            }

            int schema_id = serdes_schema_id(schema);
            if (current_schema_id == -1) {
                avro_schema_t avro_schema = serdes_schema_avro(schema);
                auto arrow_schema = avro_to_arrow(avro_schema);
                auto pool = arrow::default_memory_pool();
                arrow::RecordBatchBuilder::Make(arrow_schema, pool, &builder);
                current_schema_id = schema_id;
            } else if (schema_id != current_schema_id) {
                // schema changed, what now?
                break;
            }

            read_record(avro, builder.get());
            num_messages += 1;
        }
    }

    serdes_destroy(serdes);

    std::shared_ptr<arrow::RecordBatch> out;
    builder->Flush(&out);
    return out;
}
