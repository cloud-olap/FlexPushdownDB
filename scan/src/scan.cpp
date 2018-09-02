//============================================================================
// Name        : cpp-s3filter.cpp
//============================================================================

#include <assert.h>
#include <Python.h>
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
//#include <aws/core/client/CoreErrors.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/platform/Platform.h>
//#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
//#include <aws/s3/model/DeleteBucketRequest.h>
//#include <aws/s3/model/CreateBucketRequest.h>
//#include <aws/s3/model/HeadBucketRequest.h>
//#include <aws/s3/model/PutObjectRequest.h>
//#include <aws/core/utils/memory/stl/AWSStringStream.h>
//#include <aws/core/utils/HashingUtils.h>
//#include <aws/core/utils/StringUtils.h>
//#include <aws/core/utils/UUID.h>
//#include <aws/s3/model/GetObjectRequest.h>
//#include <aws/s3/model/DeleteObjectRequest.h>
//#include <aws/s3/model/HeadObjectRequest.h>
//#include <aws/s3/model/CreateMultipartUploadRequest.h>
//#include <aws/s3/model/UploadPartRequest.h>
//#include <aws/s3/model/CompleteMultipartUploadRequest.h>
//#include <aws/s3/model/ListObjectsRequest.h>
//#include <aws/s3/model/GetBucketLocationRequest.h>
//#include <aws/core/utils/DateTime.h>
//#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/utils/threading/Executor.h>
//#include <aws/core/utils/crypto/Factories.h>
//#include <aws/core/utils/crypto/Cipher.h>
//#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/transfer/TransferManager.h>
//
//#include <cstdlib>
#include <iostream>
//#include <string>
#include <aws/event-stream/event_stream.h>
#include <aws/common/common.h>
#include "csv-parser/parser.hpp"

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include "numpy/arrayobject.h"
//
using namespace std;
//using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
//using namespace Aws::S3::Model;
using namespace Aws::Utils;

class Reader
{
    int bytes_scanned = 0;
    int bytes_processed = 0;
    int bytes_returned = 0;

public:
    Reader() {
    }


    Aws::String buildURI(std::shared_ptr<S3Client> s3_client, Aws::String bucket_name, Aws::String TEST_OBJ_KEY){
        Aws::String uri = s3_client->GeneratePresignedUrl(bucket_name, TEST_OBJ_KEY, HttpMethod::HTTP_POST);
        return uri;
    }

    std::shared_ptr<HttpRequest> buildRequest(Aws::String url){
        std::shared_ptr<HttpRequest> request = Aws::Http::CreateHttpRequest(url, HttpMethod::HTTP_POST, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
        return request;
    }

    Aws::String buildRequestBody(Aws::String sql){
        Aws::StringStream body_ss;

        encode(sql);

        body_ss << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<SelectRequest>"
                "<Expression>" << sql << "</Expression>"
                "<ExpressionType>SQL</ExpressionType>"
                "<InputSerialization>"
                    "<CSV>"
                        "<FileHeaderInfo>USE</FileHeaderInfo>"
                        "<RecordDelimiter>|\n</RecordDelimiter>"
                        "<FieldDelimiter>|</FieldDelimiter>"
                    "</CSV>"
                "</InputSerialization>"
                "<OutputSerialization>"
                    "<CSV>"
                    "</CSV>"
               " </OutputSerialization>"
            "</SelectRequest> ";

        return body_ss.str();
    }

    Aws::String buildS3ObjectKey(const char* key){
        Aws::StringStream key_ss;
        key_ss << key << "?select&select-type=2";
        return key_ss.str();
    }

    bool starts_with(const Aws::String& s1, const Aws::String& s2) {
        return s2.size() <= s1.size() && s1.compare(0, s2.size(), s2) == 0;
    }

    static bool ends_with(const Aws::String& s1, const Aws::String& s2) {
        return s2.size() <= s1.size() && s1.compare(s1.size() - s2.size(), s1.size(), s2) == 0;
    }

    enum AwsEventType { records, stats, end };

    struct test_decoder_data {
        struct aws_event_stream_message_prelude latest_prelude;
        char latest_header_name[100];
        char latest_header_value[100];
        struct aws_allocator *alloc;
        int latest_error;
        Aws::String *last_line;
        Aws::StringStream buffer_stringstream;
        PyObject *on_data_callback;
        Reader *reader;
        AwsEventType latest_event_type;
        int new_event_sequence;
        Aws::String stats_str;
    };

    static void s_decoder_test_on_payload_segment(struct aws_event_stream_streaming_decoder *decoder,
                                              struct aws_byte_buf *data, int8_t final_segment, void *user_data) {
        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;

        // cout << endl << "--------------- PAYLOAD SEGMENT START!!!" << endl;
        // cout << "Event Type is: |||" << decoder_data->latest_event_type << "|||" << endl;

        Aws::String payload_str = Aws::String((char*)(data->buffer), data->len);
        // Aws::StringStream buffer_stringstream =  Aws::StringStream();

        // cout << "Payload is: " << endl;
        // cout << "|||" << payload_str << "|||" << endl << endl;

        if(decoder_data->last_line != NULL){
            // cout << "Last line exists, prepending last line: |||" << *(decoder_data->last_line) << "|||" << endl;
            payload_str.insert(0, *(decoder_data->last_line));
            decoder_data->last_line = NULL;
        }

        // cout << "Payload with last line prepended: " << endl;
        // cout << "|||" << payload_str << "|||" << endl << endl;

        if(decoder_data->latest_event_type == AwsEventType::records){
            if(payload_str.at(payload_str.size() - 1) == '\n'){
                // cout << "Payload is complete, appending payload to buffer" << endl;
                decoder_data->buffer_stringstream << payload_str;
            }
            else{
                // cout << "Payload is incomplete" << endl;
                int last_line_pos = payload_str.rfind('\n');
                if(last_line_pos > 0){
                    // cout << "Payload contains complete records, extracting last line and appending complete payload to buffer" << endl;
                    // cout << "Last line start position: " << last_line_pos << endl;
                    decoder_data->last_line = new Aws::String(payload_str.substr(last_line_pos + 1, payload_str.size()));
                    decoder_data->buffer_stringstream << payload_str.substr(0, last_line_pos + 1); 
                }
                else{
                    // cout << "Payload does not contain complete records, setting to last line" << endl;
                    decoder_data->last_line = new Aws::String(payload_str);
                }

                // cout << "New Last incomplete line: " << "|||" << *(decoder_data->last_line) << "|||" << endl;
            }
        }
        else if(decoder_data->latest_event_type == AwsEventType::stats){

            // HACK: Grab the remainder of the records before grabbing the stats
            int num_chars = decoder_data->buffer_stringstream.tellp();
            // cout << num_chars << endl;
            if(num_chars > 0){
                // cout << "PARSING REMAINING RECORDS PAYLOADS!!!" << decoder_data->buffer_stringstream.str() << endl;
                parse_records_payload(decoder_data);
                decoder_data->buffer_stringstream = Aws::StringStream();
            }

            decoder_data->buffer_stringstream << payload_str;
        }

        // cout << "Buffer: " << endl <<"|||" <<  decoder_data->buffer_stringstream.str() <<"|||" <<  endl;

        if(decoder_data->latest_event_type == AwsEventType::records){

            int num_chars = decoder_data->buffer_stringstream.tellp();
            // cout << num_chars << endl;
            if(num_chars > 1024 * 256){
                // cout << "PARSING RECORDS PAYLOADS!!!" << decoder_data->buffer_stringstream.str() << endl;
                parse_records_payload(decoder_data);
                decoder_data->buffer_stringstream = Aws::StringStream();
            }
        }
        else if(decoder_data->latest_event_type == AwsEventType::stats){
            // cout << "STATS PAYLOAD!!!" << decoder_data->buffer_stringstream.str() << endl;
            decoder_data->stats_str += decoder_data->buffer_stringstream.str();
        }

        // cout << "----------------------- PAYLOAD SEGMENT END!!!" << endl;
    }

    static void parse_records_payload(test_decoder_data *decoder_data){
        
        aria::csv::CsvParser parser = aria::csv::CsvParser(decoder_data->buffer_stringstream)
            .delimiter(',')
            .quote('"')
            .terminator('\n');

        auto data_v = std::vector<std::vector<std::string>>();

        #ifndef NDEBUG
            int num_cols = -1;
        #endif
        
        for (auto& row : parser) {
            std::vector<std::string> row_v = std::vector<std::string>();
            for (auto& field : row) {
                row_v.push_back(field);
                // std::cout << field << " | ";
            }

            #ifndef NDEBUG
                if(num_cols == -1){
                    num_cols = row_v.size();
                    // cout << "Number of columns: " << num_cols << endl;
                }
                assert(num_cols == (int)row_v.size());
            #endif

            data_v.push_back(row_v);
            // std::cout << std::endl;
        }

        PyArrayObject* pArray = Reader::build_nd_array4(data_v);
        PyObject* ret = PyArray_Return(pArray);

        PyObject *arglist = Py_BuildValue("(O)", ret);
        PyObject *result = PyObject_CallObject(decoder_data->on_data_callback, arglist);

        Py_XDECREF(arglist);
        Py_XDECREF(result);
    }

    void parse_stats_payload(test_decoder_data *decoder_data){

        // STATS - could use an xml parser but its pretty simple so we just hack through it

        Aws::String gt = Aws::String(">");
        Aws::String lt = Aws::String("<");

        int scanned_start = find_nth(decoder_data->stats_str, 0, gt, 1);
        int scanned_end = find_nth(decoder_data->stats_str, 0, lt, 2);
        Aws::String bytes_scanned_str = decoder_data->stats_str.substr(scanned_start + 1, scanned_end - scanned_start - 1);
        this->bytes_scanned = std::stoi(bytes_scanned_str.c_str(), nullptr, 10);

        int processed_start = find_nth(decoder_data->stats_str, 0, gt, 3);
        int processed_end = find_nth(decoder_data->stats_str, 0, lt, 4);
        Aws::String bytes_processed_str = decoder_data->stats_str.substr(processed_start + 1, processed_end - processed_start - 1);
        this->bytes_processed = std::stoi(bytes_processed_str.c_str(), nullptr, 10);

        int returned_start = find_nth(decoder_data->stats_str, 0, gt, 5);
        int returned_end = find_nth(decoder_data->stats_str, 0, lt, 6);
        Aws::String bytes_returned_str = decoder_data->stats_str.substr(returned_start + 1, returned_end - returned_start - 1);
        this->bytes_returned = std::stoi(bytes_returned_str.c_str(), nullptr, 10);
    }


    static void s_decoder_test_on_prelude_received(struct aws_event_stream_streaming_decoder *decoder,
                                               struct aws_event_stream_message_prelude *prelude, void *user_data) {

        // struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;
    }



    static void s_decoder_test_header_received(struct aws_event_stream_streaming_decoder *decoder,
                                           struct aws_event_stream_message_prelude *prelude,
                                           struct aws_event_stream_header_value_pair *header, void *user_data) {
        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;
        memset(decoder_data->latest_header_name, 0, sizeof(decoder_data->latest_header_name));
        memcpy(decoder_data->latest_header_name, header->header_name, (size_t) header->header_name_len);
        memset(decoder_data->latest_header_value, 0, sizeof(decoder_data->latest_header_value));
        memcpy(decoder_data->latest_header_value, header->header_value.variable_len_val, header->header_value_len);

//        cout << "event is: |||" << decoder_data->latest_header_value << "|||" << endl;

        if(strcmp(decoder_data->latest_header_value, "event") == 0){
//            cout << "New Event" << endl;
            decoder_data->new_event_sequence = 0;
            return;
        }

        if(decoder_data->new_event_sequence == 0){
            // cout << "New Event Type" << decoder_data->latest_header_value << endl;
            if(strcmp(decoder_data->latest_header_value, "Records") == 0){
                decoder_data->latest_event_type = AwsEventType::records;
            }
            else if(strcmp(decoder_data->latest_header_value, "Stats") == 0){
                decoder_data->latest_event_type = AwsEventType::stats;
            }
            else if(strcmp(decoder_data->latest_header_value, "End") == 0){
                decoder_data->latest_event_type = AwsEventType::end;
                decoder_data->reader->parse_stats_payload(decoder_data);
            }
            
            decoder_data->new_event_sequence = 1;
            return;
        }

        if(decoder_data->new_event_sequence == 1){
//            cout << "New Event Content Type" << endl;
            decoder_data->new_event_sequence = -1;
            return;
        }

    }

    static void s_decoder_test_on_error(struct aws_event_stream_streaming_decoder *decoder,
                                    struct aws_event_stream_message_prelude *prelude, int error_code,
                                    const char *message,
                                    void *user_data) {

        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;
        decoder_data->latest_error = error_code;

        cout << "An error occurred: (code: " << error_code << ", message: " << message << ")" << endl;
    }

    PyObject* get_bytes_scanned() {
        return Py_BuildValue("i", this->bytes_scanned);
    }

    PyObject* get_bytes_processed() {
        return Py_BuildValue("i", this->bytes_processed);
    }

    PyObject* get_bytes_returned() {
        return Py_BuildValue("i", this->bytes_returned);
    }

    static int find_nth(const Aws::String& haystack, size_t pos, const Aws::String& needle, size_t nth)
    {
        size_t found_pos = haystack.find(needle, pos);
        if(0 == nth || string::npos == found_pos)  return found_pos;
        return find_nth(haystack, found_pos+1, needle, nth-1);
    }

    static PyArrayObject* build_nd_array4(const std::vector<std::vector<std::string>> &vec){

        PyObject *out = nullptr;

        int num_rows = vec.size();
        int num_cols = vec[0].size();

        npy_intp dims[2];
        dims[0] = num_rows;
        dims[1] = num_cols;

        // cout << dims[0] << "," << dims[1] << endl;

        out = PyArray_SimpleNew(2, dims, NPY_OBJECT);
        out = (PyObject*) PyArray_GETCONTIGUOUS((PyArrayObject*) out);

        PyObject** data = (PyObject**) (PyArrayObject**) PyArray_DATA((PyArrayObject*)out);

        for (auto& row : vec) {
            for (auto& field : row) {

                // cout << field.c_str() << ",";

                PyObject* py_field = Py_BuildValue("s", field.c_str());
                *data = py_field;
                Py_INCREF(*data);
                if(&field != &row.back())
                    data += 1;
            }

            // cout << endl;

            if(&row != &vec.back())
                data += 1;
        }

        // data += 16;
        // PyObject* py_field = Py_BuildValue("s", vec[0][0].c_str());
        // *data = py_field;

        // cout << "Done" << endl;

        return (PyArrayObject*)out;

    }

    // static PyArrayObject* build_nd_array5(const std::vector<std::vector<std::string>> &vec){

    //     // int num_rows = vec.size();
    //     // int num_cols = vec[0].size();

    //     npy_intp dims[2];
    //     dims[0] = 2;
    //     dims[1] = 2;

    //     // PyArrayObject* pArray = (PyArrayObject*)PyArray_SimpleNew(2, dims, NPY_OBJECT);

    //     // PyObject* data = (PyObject*) PyArray_DATA(pArray);

    //     // for(size_t i=0;i<vec.size();i++){
    //     //     std::vector<std::string> row = vec[i];
    //     //     for(size_t j=0;j<row.size();j++){
    //     //         PyObject* field_value = Py_BuildValue("s", row[j]);
    //     //         memcpy(data, field_value, sizeof(PyObject*) * NumInputs);
    //     //     }
    //     // }


    //     PyObject *out = nullptr;

    //     PyObject* str1 = Py_BuildValue("s", "111");
    //     PyObject* str2 = Py_BuildValue("s", "222");
    //     PyObject* str3 = Py_BuildValue("s", "333");
    //     PyObject* str4 = Py_BuildValue("s", "444");

    //     out = PyArray_SimpleNew(2, dims, NPY_OBJECT);
    //     out = (PyObject*) PyArray_GETCONTIGUOUS((PyArrayObject*) out);

    //     cout << "1" << endl;

    //     PyObject** data = (PyObject**) (PyArrayObject**) PyArray_DATA((PyArrayObject*)out);
        

    //     cout << "2" << endl;

    //     // void* item_ptr = (char*)PyArray_GETPTR1(out, 0);

    //     // PyArray_SETITEM(out, (char*)item_ptr, py_int);

    //     // cout << "5" << endl;

    //     // PyObject* h = PyArray_GETITEM(out, (char*)item_ptr);

    //     // cout << "6" << endl;

    //     // cout << "REPR: " << PyString_AsString(PyObject_Repr(h)) << endl;

    //     // data +=  sizeof(PyObject*);
    //     *data = str1;

    //     data += 1;

    //     *data = str2;

    //     data += 1;

    //     *data = str3;

    //      data += 1;

    //     *data = str4;
        

    //     cout << "7" << endl;

    //     return (PyArrayObject*)out;
    // }

    // static PyArrayObject* build_nd_array3(const std::vector<std::vector<std::string>> &vec){

    //     // int num_rows = vec.size();
    //     // int num_cols = vec[0].size();

    //     // npy_intp dims[2];
    //     // dims[0] = num_rows;
    //     // dims[1] = num_cols;

    //     // PyArrayObject* pArray = (PyArrayObject*)PyArray_SimpleNew(2, dims, NPY_OBJECT);

    //     // PyObject* data = (PyObject*) PyArray_DATA(pArray);

    //     // for(size_t i=0;i<vec.size();i++){
    //     //     std::vector<std::string> row = vec[i];
    //     //     for(size_t j=0;j<row.size();j++){
    //     //         PyObject* field_value = Py_BuildValue("s", row[j]);
    //     //         memcpy(data, field_value, sizeof(PyObject*) * NumInputs);
    //     //     }
    //     // }


    //     PyObject *out = nullptr;

    //     PyObject* str1 = Py_BuildValue("s", "111");
    //     PyObject* str2 = Py_BuildValue("s", "222");

    //     npy_intp size = {2};

    //     out = PyArray_SimpleNew(1, &size, NPY_OBJECT);
    //     out = (PyObject*) PyArray_GETCONTIGUOUS((PyArrayObject*) out);

    //     cout << "1" << endl;

    //     PyObject** data = (PyObject**) (PyArrayObject**) PyArray_DATA((PyArrayObject*)out);
        

    //     cout << "2" << endl;

    //     PyObject* py_int = Py_BuildValue("s", "111");
    //     PyObject* py_int2 = Py_BuildValue("s", "222");

    //     // void* item_ptr = (char*)PyArray_GETPTR1(out, 0);

    //     // PyArray_SETITEM(out, (char*)item_ptr, py_int);

    //     // cout << "5" << endl;

    //     // PyObject* h = PyArray_GETITEM(out, (char*)item_ptr);

    //     // cout << "6" << endl;

    //     // cout << "REPR: " << PyString_AsString(PyObject_Repr(h)) << endl;

    //     // data +=  sizeof(PyObject*);
    //     *data = py_int;

    //     data += 1;

    //     *data = py_int2;
        

    //     cout << "7" << endl;

    //     return (PyArrayObject*)out;
    // }

    // static PyArrayObject* build_nd_array2(const std::vector<std::vector<std::string>> &vec){

    //     PyObject *out = nullptr;

    //     PyObject* str1 = Py_BuildValue("s", "111");
    //     PyObject* str2 = Py_BuildValue("s", "222");

    //     std::vector<PyObject*> *vector = new std::vector<PyObject*>();
    //     vector->push_back(str1);
    //     vector->push_back(str2);

    //     cout << "|||" << PyString_AsString(str1) << "|||" << endl;

    //     PyObject* temp = (*vector)[0];

    //     cout << "|||" << PyString_AsString(temp) << "|||" << endl;

    //     npy_intp size = {vector->size()};

    //     out = PyArray_SimpleNewFromData(1, &size, NPY_OBJECT, vector->data());



    //     return (PyArrayObject*)out;
    // }

    // static PyArrayObject* build_nd_array(const std::vector<std::vector<std::string>> &vec){

    //     PyArrayObject* pArray;

    //     int num_rows = vec.size();
    //     int num_cols = vec[0].size();

    //     npy_intp dims6[2];
    //     dims6[0] = num_rows;
    //     dims6[1] = num_cols;

    //     pArray = (PyArrayObject*)PyArray_SimpleNew(2, dims6, NPY_OBJECT);

    //     PyObject *str;

    //     for(size_t i=0;i<vec.size();i++){
    //         std::vector<std::string> row = vec[i];
    //         for(size_t j=0;j<row.size();j++){
    //             str = Py_BuildValue("s", row[j].data());
    //             void* item_ptr = PyArray_GETPTR2(pArray, i, j);
    //             PyArray_SETITEM(pArray, (char*)item_ptr, str);
    //         }
    //     }

    //     return pArray;
    // }

    static void encode(Aws::String& data) {
        Aws::String buffer;
        buffer.reserve(data.size());
        for(size_t pos = 0; pos != data.size(); ++pos) {
            switch(data[pos]) {
                case '&':  buffer.append("&amp;");       break;
                case '\"': buffer.append("&quot;");      break;
                case '\'': buffer.append("&apos;");      break;
                case '<':  buffer.append("&lt;");        break;
                case '>':  buffer.append("&gt;");        break;
                default:   buffer.append(&data[pos], 1); break;
            }
        }
        data.swap(buffer);
    }

    PyObject* execute(const char* key, const char* sql, PyObject* on_data_callback) {

        // std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

        try
        {
            // Aws::Utils::Logging::InitializeAWSLogging(
            //     Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
            //         "Scan", Aws::Utils::Logging::LogLevel::Info, "aws_sdk_"));

            Aws::SDKOptions options;
            options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
            Aws::InitAPI(options);
            {

                Aws::Utils::Logging::GetLogSystem()->Log(Aws::Utils::Logging::LogLevel::Trace, "s3filter", "Starting");

                // Do AWS stuff
                static const Aws::String bucket_name = "s3filter";
//                static const char* TEST_OBJ_KEY = "region.csv?select&select-type=2";
                const Aws::String TEST_OBJ_KEY = buildS3ObjectKey(key);
                static const char* ALLOCATION_TAG = "main";
                std::shared_ptr<S3Client> s3_client;
                std::shared_ptr<HttpClient> m_HttpClient;

                std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> Limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, 500000000);

                ClientConfiguration config;
                config.region = Aws::Region::US_EAST_1;
//                config.scheme = Scheme::HTTPS;
                config.verifySSL = false;
                config.scheme = Scheme::HTTP;
                config.connectTimeoutMs = 30000;
                config.requestTimeoutMs = 30000;
//                config.readRateLimiter = Limiter;
//                config.writeRateLimiter = Limiter;
                config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 8);

                m_HttpClient = Aws::Http::CreateHttpClient(config);

                s3_client = Aws::MakeShared<S3Client>(ALLOCATION_TAG,
                                    Aws::MakeShared<DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG), config,
                                        AWSAuthV4Signer::PayloadSigningPolicy::Never /*signPayloads*/, true /*useVirtualAddressing*/);


                // std::chrono::high_resolution_clock::time_point time = std::chrono::high_resolution_clock::now();
                // cout << "Time to client init: " << std::chrono::duration_cast<std::chrono::microseconds>(time - start).count() << endl;

                Aws::String url = buildURI(s3_client, bucket_name, TEST_OBJ_KEY);
                Aws::String body = buildRequestBody(sql);

            //    cout << endl <<
            //            "Sending request:" << endl <<
            //            "URL: " << url << endl <<
            //            "Body: " << endl << body << endl;

                Aws::StringStream content_length;

                content_length << std::to_string(body.length());

                std::shared_ptr<Aws::StringStream> body_ss = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
                *body_ss << body;

                std::shared_ptr<HttpRequest> request = buildRequest(url);
                request->SetContentLength(content_length.str());
                request->AddContentBody(body_ss);

//                cout << "MAKING REQUEST" << endl;

                // std::chrono::high_resolution_clock::time_point time2 = std::chrono::high_resolution_clock::now();
                // cout << "Time to request built: " << std::chrono::duration_cast<std::chrono::microseconds>(time2 - start).count() << endl;

                Aws::Utils::Logging::GetLogSystem()->Log(Aws::Utils::Logging::LogLevel::Trace, "s3filter", "Making");

                aws_allocator* alloc = aws_default_allocator();

                struct test_decoder_data decoder_data = {
                    .latest_prelude = {},
                    .latest_header_name = {},
                    .latest_header_value = {},
                    .alloc = alloc,
                    .latest_error = 0,
                    .last_line = NULL,
                    .buffer_stringstream = Aws::StringStream(),
                    .on_data_callback = on_data_callback,
                    .reader = this
                };

//                cout << "Creating decoder" << endl << std::flush;

                struct aws_event_stream_streaming_decoder decoder;
                aws_event_stream_streaming_decoder_init(&decoder,
                                                        alloc,
                                                        s_decoder_test_on_payload_segment,
                                                        s_decoder_test_on_prelude_received,
                                                        s_decoder_test_header_received,
                                                        s_decoder_test_on_error,
                                                        &decoder_data);

//                cout << "Starting" << endl << std::flush;

                DataReceivedEventHandler f = [&decoder](const HttpRequest *req, const HttpResponse *res, long length) { 

                    // cout << "Data Received Event: " << length << endl;

                    char c_buf[length];
                    res->GetResponseBody().read(c_buf, length);
                    aws_byte_buf buf = aws_byte_buf_from_array((const uint8_t *)c_buf, length);
                    aws_event_stream_streaming_decoder_pump(&decoder, &buf);
                };
                request->SetDataReceivedEventHandler(f);

                Aws::Utils::Logging::GetLogSystem()->Log(Aws::Utils::Logging::LogLevel::Trace, "s3filter", "Made");

                // std::chrono::high_resolution_clock::time_point time3 = std::chrono::high_resolution_clock::now();
                // cout << "Time to event stream decoder init: " << std::chrono::duration_cast<std::chrono::microseconds>(time3 - start).count() << endl;

                std::shared_ptr<HttpResponse> response = m_HttpClient->MakeRequest(request);

                aws_event_stream_streaming_decoder_clean_up(&decoder);

                // std::chrono::high_resolution_clock::time_point time4 = std::chrono::high_resolution_clock::now();
                // cout << "Time to response complete: " << std::chrono::duration_cast<std::chrono::microseconds>(time4 - start).count() << endl;
            }
            Aws::ShutdownAPI(options);
            Aws::Utils::Logging::ShutdownAWSLogging();
        }
        catch(std::exception const& e)
        {
            std::cerr << "Error: " << e.what() << std::endl;
//            return EXIT_FAILURE;
        }

        Py_INCREF(Py_None);
        return Py_None;
    }
};

Reader* reader;

static PyObject *
scan_execute(PyObject *self, PyObject *args)
{
    const char *key;
    const char *sql;
    static PyObject *on_data_callback = NULL;

    if (!PyArg_ParseTuple(args, "ssO", &key, &sql, &on_data_callback)){
        return NULL;
    }

    Py_INCREF(on_data_callback);

//    std::cout << std::endl;
//    std::cout << "Execute | Key: " << key << ", sql: " << sql << std::endl;

    // PyObject* res = reader->execute(key, sql, on_data_callback);
    reader->execute(key, sql, on_data_callback);

    Py_INCREF(Py_None);
    return Py_None;

//    std::cout << res;
}



static PyObject *
scan_get_bytes_scanned(PyObject *self, PyObject *args)
{

//    std::cout << std::endl;
//    std::cout << "get_bytes_scanned" << std::endl;

    PyObject* res = reader->get_bytes_scanned();

//    std::cout << res;

    return res;
}


static PyObject *
scan_get_bytes_processed(PyObject *self, PyObject *args)
{

//    std::cout << std::endl;
//    std::cout << "get_bytes_scanned" << std::endl;

    PyObject* res = reader->get_bytes_processed();

//    std::cout << res;

    return res;
}


static PyObject *
scan_get_bytes_returned(PyObject *self, PyObject *args)
{

//    std::cout << std::endl;
//    std::cout << "get_bytes_scanned" << std::endl;

    PyObject* res = reader->get_bytes_returned();

//    std::cout << res;

    return res;
}

static PyMethodDef ScanMethods[] = {
    {"get_bytes_scanned",  scan_get_bytes_scanned, METH_VARARGS, "Gets Bytes Scanned."},
    {"get_bytes_processed",  scan_get_bytes_processed, METH_VARARGS, "Gets Bytes Processed."},
    {"get_bytes_returned",  scan_get_bytes_returned, METH_VARARGS, "Gets Bytes Returned."},
    {"execute",  scan_execute, METH_VARARGS, "Execute an S3 Select query."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initscan(void)
{
    (void) Py_InitModule("scan", ScanMethods);
    import_array();
    reader = new Reader();
}





//
//
//
//
//
//
//Aws::String buildURI(std::shared_ptr<S3Client> s3_client, Aws::String bucket_name, const char* TEST_OBJ_KEY){
//	Aws::String uri = s3_client->GeneratePresignedUrl(bucket_name, TEST_OBJ_KEY, HttpMethod::HTTP_POST);
//	return uri;
//}
//
//std::shared_ptr<HttpRequest> buildRequest(Aws::String url){
//	std::shared_ptr<HttpRequest> request = Aws::Http::CreateHttpRequest(url, HttpMethod::HTTP_POST, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
//	return request;
//}
//
//Aws::String buildRequestBody(){
//	Aws::StringStream body_ss;
//	body_ss << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
//		"<SelectRequest>"
//			"<Expression>Select * from S3Object</Expression>"
//			"<ExpressionType>SQL</ExpressionType>"
//			"<InputSerialization>"
//				"<CSV>"
//					"<FileHeaderInfo>IGNORE</FileHeaderInfo>"
//				"</CSV>"
//			"</InputSerialization>"
//			"<OutputSerialization>"
//				"<CSV>"
//				"</CSV>"
//		   " </OutputSerialization>"
//		"</SelectRequest> ";
//
//	return body_ss.str();
//}
//
//
//int main(int argc, char** argv) {
//
//    try
//    {
//    	Aws::SDKOptions options;
//    	options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
//    	Aws::InitAPI(options);
//    	{
//    		const Aws::String bucket_name = "s3filter";
//    		static const char* TEST_OBJ_KEY = "lineitem.csv?select&select-type=2";
//    		static const char* ALLOCATION_TAG = "main";
//    		std::shared_ptr<S3Client> s3_client;
//    		std::shared_ptr<HttpClient> m_HttpClient;
//
//    		std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> Limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, 50000000);
//
//    		ClientConfiguration config;
//			config.region = Aws::Region::US_EAST_1;
//			config.scheme = Scheme::HTTPS;
//			config.connectTimeoutMs = 30000;
//			config.requestTimeoutMs = 30000;
//			config.readRateLimiter = Limiter;
//			config.writeRateLimiter = Limiter;
//			config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);
//
//			m_HttpClient = Aws::Http::CreateHttpClient(config);
//
//			s3_client = Aws::MakeShared<S3Client>(ALLOCATION_TAG,
//			                    Aws::MakeShared<DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG), config,
//			                        AWSAuthV4Signer::PayloadSigningPolicy::Never /*signPayloads*/, true /*useVirtualAddressing*/);
//
//			std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
//
//			Aws::String url = buildURI(s3_client, bucket_name, TEST_OBJ_KEY);
//			Aws::String body = buildRequestBody();
//
//			cout << endl <<
//					"Sending request:" << endl <<
//					"URL: " << url << endl <<
//					"Body: " << endl << body << endl;
//
//			Aws::StringStream content_length;
//
//			content_length << std::to_string(body.length());
//
//			std::shared_ptr<Aws::StringStream> body_ss = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
//			*body_ss << body;
//
//			std::shared_ptr<HttpRequest> request = buildRequest(url);
//			request->SetContentLength(content_length.str());
//			request->AddContentBody(body_ss);
//			std::shared_ptr<HttpResponse> response = m_HttpClient->MakeRequest(request);
//
//			HttpResponseCode code = response->GetResponseCode();
//			cout << static_cast<int>(code);
//			Aws::StringStream ss;
//			ss << response->GetResponseBody().rdbuf();
//			ss.seekg(0, ios::end);
//			int size = ss.tellg();
//
//			std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now();
//
//			std::chrono::duration<double, std::milli> time_span = finish - start;
//
//			std::cout << endl <<
//					"Elapsed: " << time_span.count() << " millis." << endl <<
//					"Size: " << size << endl;
//
////			cout << ss.str();
//
//    	}
//    	Aws::ShutdownAPI(options);
//	}
//	catch(std::exception const& e)
//	{
//		std::cerr << "Error: " << e.what() << std::endl;
//		return EXIT_FAILURE;
//	}
//
//    return EXIT_SUCCESS;
//}
