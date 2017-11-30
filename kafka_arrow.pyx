# distutils: language = c++
# distutils: sources = avro_arrow_adapter.cc

import ctypes
from libcpp.memory cimport shared_ptr
from pyarrow.includes.common cimport CStatus
from pyarrow.includes.libarrow cimport CRecordBatch
from pyarrow.lib cimport RecordBatch, pyarrow_wrap_batch, check_status

cdef extern from "Python.h":
    void* PyLong_AsVoidPtr(object)

cdef extern from "kafka_arrow.h":
    CStatus poll_arrow(void *rk, int max_messages, shared_ptr[CRecordBatch]* out)

def poll(consumer, max_messages):
    cdef:
        shared_ptr[CRecordBatch] out

    # TODO: check type of consumer?
    rk_offset = ctypes.sizeof(ctypes.c_long) + ctypes.sizeof(ctypes.c_voidp)
    rk_ptr = ctypes.c_voidp.from_address(id(consumer) + rk_offset)
    status = None
    try:
        check_status(poll_arrow(PyLong_AsVoidPtr(rk_ptr.value), max_messages, &out))
    except Exception as e:
        status = e

    batch = pyarrow_wrap_batch(out) if out else None
    return batch, status

# TODO: version of poll() that throws on errors
