import sys
from distutils.core import setup, Extension
from Cython.Build import cythonize
import numpy as np

extra_compile_args = ['-std=c++11']
if sys.platform == 'darwin':
    #extra_compile_args.append('-mmacosx-version-min=10.9')
    extra_compile_args.append('-stdlib=libc++')
    extra_compile_args.append('-I/Users/andreas/local/include')

setup(
        ext_modules = cythonize(Extension("kafka_arrow", sources=['kafka_arrow.pyx'],
            libraries=['arrow_python', 'serdes'],
            library_dirs=['/Users/andreas/local/lib'],
            language="c++", extra_compile_args = extra_compile_args)),
        include_dirs = [np.get_include()],
)
