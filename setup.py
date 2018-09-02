import os
from distutils.core import setup, Extension
import imp

(file, filename, (suffix, mode, type)) = imp.find_module('numpy')
numpy_include_dir = os.path.join(filename, "core/include")

RELEASE = True
# RELEASE = False

if RELEASE:
    undef_macros = []
    extra_compile_args = ['-std=c++11', '-Ofast']
    extra_link_args = []
else:
    undef_macros = ["NDEBUG"]
    extra_compile_args = ['-std=c++11', '-g', '-O0', '-fno-omit-frame-pointer']
    extra_link_args = ['-g']

scanmodule = Extension('scan',
                       include_dirs=[numpy_include_dir,
                                     'build/thirdparty'],
                       libraries=['aws-cpp-sdk-core',
                                  'aws-cpp-sdk-s3',
                                  'aws-c-common',
                                  'aws-checksums',
                                  'aws-c-event-stream'],
                       extra_objects=['/usr/local/lib/libaws-c-common.a',
                                      '/usr/local/lib/libaws-checksums.a',
                                      '/usr/local/lib/libaws-c-event-stream.a'],
                       extra_compile_args=extra_compile_args,
                       extra_link_args=extra_link_args,
                       undef_macros=undef_macros,
                       sources=['scan/src/scan.cpp'])

setup(name='scan',
      version='1.0',
      description='Fast s3',
      packages=['scan'],
      package_data={'scan': ['scan.so']},
      ext_modules=[scanmodule])
