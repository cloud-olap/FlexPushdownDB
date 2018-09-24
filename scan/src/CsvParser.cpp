#include <scan/CsvParser.h>

#include <iostream>
#include <sstream>
#include <csv-parser/parser.hpp>
#include <fast-cpp-csv-parser/csv.h>


CsvParser::CsvParser() : m_ndArrayBuilder()
{
}

// static void PrintRepr(PyObject &py_object){
//     PyObject* repr = PyObject_Repr(&py_object);
//     std::cout << PyString_AsString(repr) << std::endl;
// }


std::shared_ptr<std::vector<std::vector<std::string>>> CsvParser::BuildVector(std::stringstream &ss){

    auto vec = std::make_shared<std::vector<std::vector<std::string>>>();

    // Create parser
    aria::csv::CsvParser parser = aria::csv::CsvParser(ss)
                .delimiter(',')
                .quote('"')
                .terminator('\n');

    // Parse into vector
    for (auto& row : parser) {
        auto row_vec = std::vector<std::string>();
        for (auto& field : row) {
            row_vec.push_back(field);
//            std::cout << field << ", ";
        }

        vec->push_back(row_vec);
//        std::cout << std::endl;
    }

//    std::string fields[16];
//
//
//
//
//    io::CSVReader<16, io::trim_chars<' ', '\t', '_'>, io::double_quote_escape<',','\"'>>in("dummy.csv", ss);
////    in.set_header("_0", "_1", "_2");
//    while(in.read_row(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11], fields[12], fields[13], fields[14], fields[15])){
//        auto row_vec = std::vector<std::string>();
//        for(int i=0;i<16;i++){
//            row_vec.push_back(fields[i]);
//        }
//        vec->push_back(row_vec);
//    }

    return vec;
}


PyArrayObject* CsvParser::Parse(PyObject &stream){

    // Stringstream we will collect data in, can be on stack as we will discard
    std::stringstream ss;

    // Get read function and create invocation args
    PyObject* read_fn = PyObject_GetAttrString(&stream, "read"); // This does not need to be DECREF'd
    PyObject *args = Py_BuildValue("()");

    // Read bytes until EOF
    while(true){
        PyObject* bytes = PyObject_CallObject(read_fn, args);
        int length = PySequence_Length(bytes);
        if(length == 0){
            break;
        }
        else{
            ss << PyString_AsString(bytes);
        }
        Py_XDECREF(bytes);
    }

    Py_XDECREF(args);

    auto vec = BuildVector(ss);
    PyArrayObject* nd_array = this->m_ndArrayBuilder.Build(*vec); // The returned nd_array is already incref'd

    return nd_array;
}