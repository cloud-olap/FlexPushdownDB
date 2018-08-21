//============================================================================
// Name        : cpp-s3filter.cpp
//============================================================================

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

    std::shared_ptr<HttpRequest> buildRequest(Aws::String url, Aws::IOStreamFactory fac){
        std::shared_ptr<HttpRequest> request = Aws::Http::CreateHttpRequest(url, HttpMethod::HTTP_POST, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
        //std::shared_ptr<HttpRequest> request = Aws::Http::CreateHttpRequest(url, HttpMethod::HTTP_POST, fac);
        return request;
    }

    Aws::String buildRequestBody(Aws::String sql){
        Aws::StringStream body_ss;
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
        std::vector<std::vector<std::string>> *data_v;
        AwsEventType latest_event_type;
        int new_event_sequence;
        Aws::String stats_str;
    };

    static void s_decoder_test_on_payload_segment(struct aws_event_stream_streaming_decoder *decoder,
                                              struct aws_byte_buf *data, int8_t final_segment, void *user_data) {
        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;

        // cout << endl << "PAYLOAD SEGMENT START!!!" << endl;
        cout << "Event Type is: |||" << decoder_data->latest_event_type << "|||" << endl;

        Aws::String buffer_string = Aws::String((char*)(data->buffer), data->len);
        Aws::StringStream buffer_stringstream =  Aws::StringStream();

        // cout << "PAYLOAD IS..." << endl;
        // cout << "|||" << buffer_string << "|||" << endl;

        if(decoder_data->last_line != NULL){
            // cout << "Last line: |||" << *(decoder_data->last_line) << "|||" << endl;
            buffer_stringstream << *(decoder_data->last_line);
            decoder_data->last_line = NULL;
        }

        if(decoder_data->latest_event_type == AwsEventType::records){
            if(buffer_string.at(buffer_string.size() - 1) == '\n'){
                // cout << "Complete" << endl;
                buffer_stringstream << buffer_string;
            }
            else{
                // cout << "Incomplete" << endl;
                int last_line_pos = buffer_string.rfind('\n');
                // cout << "Last line pos: " << last_line_pos << endl;
                decoder_data->last_line = new Aws::String(buffer_string.substr(last_line_pos + 1, buffer_string.size()));
                buffer_stringstream << buffer_string.substr(0, last_line_pos + 1);

                // cout << "New Last line" << "|||" << *(decoder_data->last_line) << "|||" << endl;
                // cout << "Stripped payload" << endl <<"|||" <<  buffer_stringstream.str() <<"|||" <<  endl;
            }
        }
        else if(decoder_data->latest_event_type == AwsEventType::stats){
            buffer_stringstream << buffer_string;
        }

        

        if(decoder_data->latest_event_type == AwsEventType::records){

            // cout << "RECORDS PAYLOAD!!!" << buffer_stringstream.str() << endl;

            aria::csv::CsvParser parser = aria::csv::CsvParser(buffer_stringstream)
              .delimiter(',')
              .quote('"')
              .terminator('\n');

            for (auto& row : parser) {
                std::vector<std::string> row_v = std::vector<std::string>();
                for (auto& field : row) {
                    row_v.push_back(field);
                //    std::cout << field << " | ";
                }
                decoder_data->data_v->push_back(row_v);
                // std::cout << std::endl;
            }
        }
        else if(decoder_data->latest_event_type == AwsEventType::stats){
            // cout << "STATS PAYLOAD!!!" << buffer_stringstream.str() << endl;
            decoder_data->stats_str += buffer_stringstream.str();
        }

        // cout << "PAYLOAD SEGMENT END!!!" << endl;
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
//            cout << "New Event Type" << endl;
            if(strcmp(decoder_data->latest_header_value, "Records") == 0){
                decoder_data->latest_event_type = AwsEventType::records;
            }
            else if(strcmp(decoder_data->latest_header_value, "Stats") == 0){
                decoder_data->latest_event_type = AwsEventType::stats;
            }
            else if(strcmp(decoder_data->latest_header_value, "End") == 0){
                decoder_data->latest_event_type = AwsEventType::end;
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

    PyObject* execute(const char* key, const char* sql) {

        std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

        PyArrayObject* pArray;

        try
        {
            Aws::Utils::Logging::InitializeAWSLogging(
                Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
                    "RunUnitTests", Aws::Utils::Logging::LogLevel::Info, "aws_sdk_"));

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


                std::chrono::high_resolution_clock::time_point time = std::chrono::high_resolution_clock::now();
                cout << "TIME_01: " << std::chrono::duration_cast<std::chrono::microseconds>(time - start).count() << endl;


                Aws::String url = buildURI(s3_client, bucket_name, TEST_OBJ_KEY);
                Aws::String body = buildRequestBody(sql);

//                cout << endl <<
//                        "Sending request:" << endl <<
//                        "URL: " << url << endl <<
//                        "Body: " << endl << body << endl;

                Aws::StringStream content_length;

                content_length << std::to_string(body.length());

                std::shared_ptr<Aws::StringStream> body_ss = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
                *body_ss << body;


                // Aws::StringStream *ss = Aws::New<Aws::StringStream>(ALLOCATION_TAG);
                // Aws::IOStreamFactory fac = [ss](){
                //     return ss;
                // };

                std::shared_ptr<HttpRequest> request = buildRequest(url, nullptr);
                request->SetContentLength(content_length.str());
                request->AddContentBody(body_ss);

//                cout << "MAKING REQUEST" << endl;
//                std::this_thread::sleep_for(std::chrono::milliseconds(2000));

                std::chrono::high_resolution_clock::time_point time2 = std::chrono::high_resolution_clock::now();
                cout << "TIME_02: " << std::chrono::duration_cast<std::chrono::microseconds>(time2 - start).count() << endl;

                Aws::Utils::Logging::GetLogSystem()->Log(Aws::Utils::Logging::LogLevel::Trace, "s3filter", "Making");

                // s3_client->MakeRequestWithUnparsedResponse(url, HttpMethod::HTTP_POST, );

                aws_allocator* alloc = aws_default_allocator();

                struct test_decoder_data decoder_data = {
                    .latest_prelude = {},
                    .latest_header_name = {},
                    .latest_header_value = {},
                    .alloc = alloc,
                    .latest_error = 0,
                    .last_line = NULL,
                    .data_v = new std::vector<std::vector<std::string>>()
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

                std::chrono::high_resolution_clock::time_point time5 = std::chrono::high_resolution_clock::now();
                cout << "TIME_05: " << std::chrono::duration_cast<std::chrono::microseconds>(time5 - start).count() << endl;

                DataReceivedEventHandler f = [&decoder](const HttpRequest *req, const HttpResponse *res, long length) { 

                    // cout << "Data Received Event: " << length << endl; 

                    char c_buf[length];
                    res->GetResponseBody().read(c_buf, length);
                    aws_byte_buf buf = aws_byte_buf_from_array((const uint8_t *)c_buf, length);
                    // cout << "Data:" << endl << buf.buffer << endl;
                    aws_event_stream_streaming_decoder_pump(&decoder, &buf);
                };
                request->SetDataReceivedEventHandler(f);

                Aws::Utils::Logging::GetLogSystem()->Log(Aws::Utils::Logging::LogLevel::Trace, "s3filter", "Made");

                std::chrono::high_resolution_clock::time_point time3 = std::chrono::high_resolution_clock::now();
                cout << "TIME_03: " << std::chrono::duration_cast<std::chrono::microseconds>(time3 - start).count() << endl;

                std::shared_ptr<HttpResponse> response = m_HttpClient->MakeRequest(request);

                cout << "MADE REQUEST" << endl;

                std::chrono::high_resolution_clock::time_point time4 = std::chrono::high_resolution_clock::now();
                cout << "TIME_04: " << std::chrono::duration_cast<std::chrono::microseconds>(time4 - start).count() << endl;

                int num_cols = decoder_data.data_v[0][0].size();
                int num_rows = decoder_data.data_v->size();

//                cout << "Rows:" << num_rows << ", Cols:" << num_cols << endl;

//                cout << "TO NUMPY..." << endl;

//                const int ND = 2;
//                npy_intp dims[2]{num_rows, num_cols};


                std::vector<std::vector<std::string>> *vec = decoder_data.data_v;

                for(size_t i=0;i<vec->size();i++){
//
                //    cout << "Row " << i << ": ";
//
                    std::vector<std::string> row = (*vec)[i];
                //    for(int j=0;j<row.size();j++){
                    //    cout << row[j] << " | ";
                //    }
//
                //    cout << endl;
                }

                std::chrono::high_resolution_clock::time_point time7 = std::chrono::high_resolution_clock::now();
                cout << "TIME_07: " << std::chrono::duration_cast<std::chrono::microseconds>(time7 - start).count() << endl;

//                pArray = (PyArrayObject*)(PyArray_SimpleNewFromData(ND, dims, NPY_OBJECT, vec->data()));
//                if (pArray == NULL) {
//                    Py_XDECREF(pArray);
//                    return NULL;
//                }

//                cout << "DONE NUMPY..." << endl;

                npy_intp dims6[2];
                dims6[0] = num_rows;
                dims6[1] = num_cols;
                pArray = (PyArrayObject*)PyArray_New(&PyArray_Type,2, dims6, NPY_OBJECT, nullptr, nullptr, 0, 0, nullptr);

                PyObject *obj3;

                for(size_t i=0;i<vec->size();i++){
                    std::vector<std::string> row = (*vec)[i];
                    for(size_t j=0;j<row.size();j++){
                        obj3 = PyString_FromString(row[j].c_str());
                        PyArray_SETITEM((PyArrayObject*)pArray, (char*)PyArray_GETPTR2((PyArrayObject*)pArray, i, j), obj3);
                    }
                }

                std::chrono::high_resolution_clock::time_point time8 = std::chrono::high_resolution_clock::now();
                cout << "TIME_08: " << std::chrono::duration_cast<std::chrono::microseconds>(time8 - start).count() << endl;

                // STATS - could use an xml parser but its pretty simple so we just hack through it

//                cout << decoder_data.stats_str << endl;

                Aws::String gt = Aws::String(">");
                Aws::String lt = Aws::String("<");

                int scanned_start = find_nth(decoder_data.stats_str, 0, gt, 1);
                int scanned_end = find_nth(decoder_data.stats_str, 0, lt, 2);
                Aws::String bytes_scanned_str = decoder_data.stats_str.substr(scanned_start + 1, scanned_end - scanned_start - 1);
                this->bytes_scanned = std::stoi(bytes_scanned_str.c_str(), nullptr, 10);

                int processed_start = find_nth(decoder_data.stats_str, 0, gt, 3);
                int processed_end = find_nth(decoder_data.stats_str, 0, lt, 4);
                Aws::String bytes_processed_str = decoder_data.stats_str.substr(processed_start + 1, processed_end - processed_start - 1);
                this->bytes_processed = std::stoi(bytes_processed_str.c_str(), nullptr, 10);

                int returned_start = find_nth(decoder_data.stats_str, 0, gt, 5);
                int returned_end = find_nth(decoder_data.stats_str, 0, lt, 6);
                Aws::String bytes_returned_str = decoder_data.stats_str.substr(returned_start + 1, returned_end - returned_start - 1);
                this->bytes_returned = std::stoi(bytes_returned_str.c_str(), nullptr, 10);

                return PyArray_Return(pArray);
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

//        Py_INCREF(pArray);

    }
};

Reader* reader;

static PyObject *
scan_execute(PyObject *self, PyObject *args)
{
    const char *key;
    const char *sql;

    if (!PyArg_ParseTuple(args, "ss", &key, &sql)){
        return NULL;
    }

//    std::cout << std::endl;
//    std::cout << "Execute | Key: " << key << ", sql: " << sql << std::endl;

    PyObject* res = reader->execute(key, sql);

//    std::cout << res;

    return res;
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
