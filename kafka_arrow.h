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


#define FATAL(reason...) do {                           \
                fprintf(stderr, "FATAL: " reason);      \
                exit(1);                                \
        } while (0)


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
