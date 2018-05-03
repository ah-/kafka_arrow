

import sys
#from distutils.core import setup, Extension
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import pyarrow

requires=["setuptools", "cython", "numpy", "pyarrow"]

extra_compile_args = ['-std=c++11']
aux_lib_dirs = []

if sys.platform == 'darwin':
    #extra_compile_args.append('-mmacosx-version-min=10.9')
    extra_compile_args.append('-stdlib=libc++')
if sys.platform == 'linux':
    extra_compile_args.append('-I/opt/python/miniconda3/envs/dud/lib/python3.6/site-packages/pyarrow/include')
    aux_lib_dirs = ['/opt/python/miniconda3/envs/dud/lib/python3.6/site-packages/pyarrow/']


print(pyarrow.get_include())
    
setup(
    name='kafka_arrow_py',
    version='0.1.0.1',
    author='ah',
    author_email='andreas@heider.io',
    url='https://github.com/ah-/kafka_arrow',
    license='Apache 2.0',
    install_requires=requires,
    setup_requires=requires,
    ext_modules = cythonize(Extension('kafka_arrow', sources=['kafka_arrow.pyx'],
                                      libraries=['arrow_python', 'serdes'],
                                      library_dirs=aux_lib_dirs,
                                      language="c++", extra_compile_args = extra_compile_args)),
    include_dirs = [np.get_include()],
)
