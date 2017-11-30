#include <avro.h>
#include "arrow/api.h"

std::shared_ptr<arrow::Schema> avro_to_arrow(avro_schema_t schema);
arrow::Status read_record(const avro_value_t val, arrow::RecordBatchBuilder *builder);
