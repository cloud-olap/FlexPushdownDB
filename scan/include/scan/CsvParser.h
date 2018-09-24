#pragma once

#include <scan/NDArrayBuilder.h>
#include <memory>
#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL _scan_module_ARRAY_API
#include <numpy/arrayobject.h>

class CsvParser
    {
    public:

        CsvParser();
        ~CsvParser() {}

        /**
         * Builds a numpy array from a Python stream of CSV data
         */ 
        PyArrayObject* Parse(PyObject &stream);

    private:

        NDArrayBuilder m_ndArrayBuilder;

        /**
         * Builds a vector of vectors from the given string stream of CSV data.
         */ 
        std::shared_ptr<std::vector<std::vector<std::string>>> BuildVector(std::stringstream &ss);
    };