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

#include "avro_arrow_adapter.h"


// TODO: option to ignore serialization errors and just carry on
arrow::Status poll_arrow(void *rkv, int max_messages, 
    std::shared_ptr<arrow::RecordBatch>* out) {
    rd_kafka_t *rk = (rd_kafka_t*) rkv;

    arrow::Status status = arrow::Status::OK();

    char errstr[512];
    serdes_conf_t *sconf = serdes_conf_new(NULL, 0, "schema.registry.url", "http://localhost:8081", NULL);

    serdes_err_t err;
    serdes_t *serdes = serdes_new(sconf, errstr, sizeof(errstr));
    if (!serdes) {
        return arrow::Status::IOError("serdes_new");
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
                status = arrow::Status::IOError(rd_kafka_message_errstr(rkm));
                goto end;
            }

            avro_value_t avro;
            serdes_schema_t *schema;
            err = serdes_deserialize_avro(serdes, &avro, &schema, rkm->payload, rkm->len, errstr, sizeof(errstr));
            if (err) {
                status = arrow::Status::IOError(errstr);
                goto end;
                // TODO: make sure these shortcuts clean up properly
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
                // have the message, can't output it :(
                // if nothing else works, output a list via out2?
                break;
            }

            read_record(avro, builder.get());
            num_messages += 1;

            avro_value_decref(&avro);
            rd_kafka_message_destroy(rkm);
        }
    }

end:
    serdes_destroy(serdes);

    if (builder) {
        builder->Flush(out);
    }

    return status;
}
