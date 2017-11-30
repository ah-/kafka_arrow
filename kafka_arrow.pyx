# distutils: language = c++
# distutils: sources = avro_arrow_adapter.cc

import ctypes
from libcpp.memory cimport shared_ptr
from pyarrow.includes.libarrow cimport CRecordBatch
from pyarrow.lib cimport RecordBatch, pyarrow_wrap_batch

cdef extern from "Python.h":
    void* PyLong_AsVoidPtr(object)

cdef extern from "kafka_arrow.h":
    shared_ptr[CRecordBatch] poll_arrow(void *rk, int max_messages)

def poll(consumer, max_messages):
    # TODO: check type of consumer?
    rk_offset = ctypes.sizeof(ctypes.c_long) + ctypes.sizeof(ctypes.c_voidp)
    rk_ptr = ctypes.c_voidp.from_address(id(consumer) + rk_offset)
    batch = poll_arrow(PyLong_AsVoidPtr(rk_ptr.value), max_messages)
    return pyarrow_wrap_batch(batch)
