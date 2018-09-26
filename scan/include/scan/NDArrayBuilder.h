#pragma once

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL _scan_module_ARRAY_API
#include <numpy/arrayobject.h>
#include <vector>
#include <string>

class NDArrayBuilder
{
  public:
    NDArrayBuilder() {}

    ~NDArrayBuilder() {}

    /**
     * Creates a numpy array from a vector of vectors of strings
     */
    PyArrayObject *Build(std::vector<std::vector<std::string>> &vec);
};