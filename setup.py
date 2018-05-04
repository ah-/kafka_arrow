import sys
#from distutils.core import setup, Extension
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import pyarrow
import itertools as it

requires=["setuptools", "cython", "numpy", "pyarrow"]

# c/c++ compiler params
name = 'kafka_arrow'
srcs = ['kafka_arrow.pyx']
compiler_lang = 'c++'
compiler_args = ['-std=c++11']
inc_dirs = [np.get_include(), pyarrow.get_include()]
lib_dirs = [pyarrow.get_library_dirs()]
libs = [pyarrow.get_libraries(), ['serdes']]

# flatten
libs = list(it.chain.from_iterable(libs))
lib_dirs = list(it.chain.from_iterable(lib_dirs))

setup(
    name='kafka_arrow_py',
    version='0.1.0.3',
    author='ah',
    author_email='andreas@heider.io',
    url='https://github.com/ah-/kafka_arrow',
    license='Apache 2.0',
    install_requires=requires,
    setup_requires=requires,
    ext_modules = cythonize(Extension(name,
                                      sources=srcs,
                                      libraries=libs,
                                      library_dirs=lib_dirs,
                                      language=compiler_lang,
                                      extra_compile_args = compiler_args)),
    include_dirs = inc_dirs
)
