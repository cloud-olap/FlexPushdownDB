import os
from distutils.core import setup, Extension
import imp

(file, filename, (suffix, mode, type)) = imp.find_module('numpy')
numpy_include_dir = os.path.join(filename, "core/include")

scanmodule = Extension('scan',
                       include_dirs=[numpy_include_dir],
                       libraries=['aws-cpp-sdk-core',
                                  'aws-cpp-sdk-s3',
                                  'aws-c-common',
                                  'aws-checksums',
                                  'aws-c-event-stream'],
                       extra_objects=['/usr/local/lib/libaws-c-common.a',
                                      '/usr/local/lib/libaws-checksums.a',
                                      '/usr/local/lib/libaws-c-event-stream.a'],
                       extra_compile_args=['-std=c++11'],
                       sources=['scan/src/scan.cpp'])

setup(name='scan',
      version='1.0',
      description='This is a demo package',
      packages=['scan'],
      package_data={'scan': ['scan.so']},
      ext_modules=[scanmodule])
