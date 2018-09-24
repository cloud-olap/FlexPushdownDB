#include <scan/NDArrayBuilder.h>

#include <Python.h>
#include <iostream>


PyArrayObject* NDArrayBuilder::Build(std::vector<std::vector<std::string>> &vec){

        PyObject *nd_array;

        // Define nd array shape
        int num_rows = vec.size();
        int num_cols = vec[0].size();

        int nd = 2;
        npy_intp dims[2];
        dims[0] = num_rows;
        dims[1] = num_cols;

        // Create nd array
        nd_array = PyArray_SimpleNew(nd, dims, NPY_OBJECT);
        nd_array = (PyObject*) PyArray_GETCONTIGUOUS((PyArrayObject*) nd_array);

        // Get reference to underlying buffer
        PyObject** data = (PyObject**) (PyArrayObject**) PyArray_DATA((PyArrayObject*)nd_array);

        // Write the vector data to the nd array
        for (auto& row : vec) {
            for (auto& field : row) {

//                std::cout << field.c_str() << ",";

                PyObject* py_field = Py_BuildValue("s", field.c_str());
                *data = py_field;
                // Py_INCREF(*data); // TODO: Is this necessary?
                if(&field != &row.back())
                    data += 1;
            }

//            std::cout << std::endl;

            if(&row != &vec.back())
                data += 1;
        }

        Py_INCREF(nd_array);

        return (PyArrayObject*)nd_array;
    }