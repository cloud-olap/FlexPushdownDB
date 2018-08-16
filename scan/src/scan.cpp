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
//
//#include <cstdlib>
#include <iostream>
//#include <string>
#include <aws/event-stream/event_stream.h>
#include <aws/common/common.h>
#include "/home/matt/Work/csv-parser/parser.hpp"

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

    const char* key;
    const char* sql;

public:
    Reader(const char* key, const char* sql) {
        this->key = key;
        this->sql = sql;
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

    struct test_decoder_data {
        struct aws_event_stream_message_prelude latest_prelude;
        char latest_header_name[100];
        char latest_header_value[100];
        uint8_t *latest_payload;
        size_t written;
        struct aws_allocator *alloc;
        int latest_error;
        Aws::String *last_line;
        std::vector<std::vector<std::string>> *data_v;
    };

    static void s_decoder_test_on_payload_segment(struct aws_event_stream_streaming_decoder *decoder,
                                              struct aws_byte_buf *data, int8_t final_segment, void *user_data) {
        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;
        memcpy(decoder_data->latest_payload + decoder_data->written, data->buffer, data->len);
        decoder_data->written += data->len;

        cout << "PAYLOAD SEGMENT START!!!" << endl;


        Aws::String buf_s = Aws::String((char*)data->buffer, data->len);

        cout << "PAYLOAD IS..." << endl;
        cout << "|||" << buf_s << "|||" << endl;

        if(decoder_data->last_line != NULL){
            buf_s = *(decoder_data->last_line) + buf_s;
        }

        cout << "APPENDED PAYLOAD IS..." << endl;
        cout << "|||" << buf_s << "|||" << endl;

        Aws::String stripped_payload;
        if(buf_s.at(buf_s.size() - 1) == '\n'){
            // Complete record
            cout << "Complete" << endl;

            stripped_payload = buf_s;
        }
        else{
            // Incomplete, strip the last line
            cout << "Incomplete" << endl;

            int last_line_pos = buf_s.rfind('\n');

            cout << "Last line pos: " << last_line_pos << endl;

            decoder_data->last_line = new Aws::String(buf_s.substr(last_line_pos + 1, buf_s.size()));


            stripped_payload = buf_s.substr(0, last_line_pos + 1);

            cout << "Last line" << "|||" << *(decoder_data->last_line) << "|||" << endl;

            cout << "Stripped payload" << endl <<"|||" <<  stripped_payload <<"|||" <<  endl;

        }

        Aws::StringStream buf_ss = Aws::StringStream(stripped_payload);

//        cout << data->buffer;

        aria::csv::CsvParser parser = aria::csv::CsvParser(buf_ss)
          .delimiter(',')    // delimited by ; instead of ,
          .quote('"')       // quoted fields use ' instead of "
          .terminator('\n'); // terminated by \0 instead of by \r\n, \n, or \r

        cout << "CSV..." << endl;

        for (auto& row : parser) {
            std::vector<std::string> row_v = std::vector<std::string>();
            for (auto& field : row) {
                row_v.push_back(field);
                std::cout << field << " | ";
            }
            decoder_data->data_v->push_back(row_v);
            std::cout << std::endl;
        }

        cout << "PAYLOAD SEGMENT END!!!" << endl;
    }

    static void s_decoder_test_on_prelude_received(struct aws_event_stream_streaming_decoder *decoder,
                                               struct aws_event_stream_message_prelude *prelude, void *user_data) {

        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;
        decoder_data->latest_prelude = *prelude;

        if (decoder_data->latest_payload) {
            aws_mem_release(decoder_data->alloc, decoder_data->latest_payload);
        }

        decoder_data->latest_payload = (uint8_t*)aws_mem_acquire(decoder_data->alloc,
                                                       decoder_data->latest_prelude.total_len - AWS_EVENT_STREAM_PRELUDE_LENGTH - AWS_EVENT_STREAM_TRAILER_LENGTH - decoder_data->latest_prelude.headers_len);
        decoder_data->written = 0;
    }

    static void s_decoder_test_header_received(struct aws_event_stream_streaming_decoder *decoder,
                                           struct aws_event_stream_message_prelude *prelude,
                                           struct aws_event_stream_header_value_pair *header, void *user_data) {
        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;
        memset(decoder_data->latest_header_name, 0, sizeof(decoder_data->latest_header_name));
        memcpy(decoder_data->latest_header_name, header->header_name, (size_t) header->header_name_len);
        memset(decoder_data->latest_header_value, 0, sizeof(decoder_data->latest_header_value));
        memcpy(decoder_data->latest_header_value, header->header_value.variable_len_val, header->header_value_len);
    }

    static void s_decoder_test_on_error(struct aws_event_stream_streaming_decoder *decoder,
                                    struct aws_event_stream_message_prelude *prelude, int error_code,
                                    const char *message,
                                    void *user_data) {

        struct test_decoder_data *decoder_data = (struct test_decoder_data *) user_data;
        decoder_data->latest_error = error_code;
    }

    PyObject* execute() {

        PyArrayObject* pArray;

        try
        {
            Aws::Utils::Logging::InitializeAWSLogging(
                Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
                    "RunUnitTests", Aws::Utils::Logging::LogLevel::Trace, "aws_sdk_"));

            Aws::SDKOptions options;
            options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
            Aws::InitAPI(options);
            {
                // Do AWS stuff
                static const Aws::String bucket_name = "s3filter";
//                static const char* TEST_OBJ_KEY = "region.csv?select&select-type=2";
                const Aws::String TEST_OBJ_KEY = buildS3ObjectKey(this->key);
                static const char* ALLOCATION_TAG = "main";
                std::shared_ptr<S3Client> s3_client;
                std::shared_ptr<HttpClient> m_HttpClient;

                std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> Limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(ALLOCATION_TAG, 50000000);

                ClientConfiguration config;
                config.region = Aws::Region::US_EAST_1;
                config.scheme = Scheme::HTTPS;
                config.connectTimeoutMs = 30000;
                config.requestTimeoutMs = 30000;
                config.readRateLimiter = Limiter;
                config.writeRateLimiter = Limiter;
                config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(ALLOCATION_TAG, 4);

                m_HttpClient = Aws::Http::CreateHttpClient(config);

                s3_client = Aws::MakeShared<S3Client>(ALLOCATION_TAG,
                                    Aws::MakeShared<DefaultAWSCredentialsProviderChain>(ALLOCATION_TAG), config,
                                        AWSAuthV4Signer::PayloadSigningPolicy::Never /*signPayloads*/, true /*useVirtualAddressing*/);

                std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

                Aws::String url = buildURI(s3_client, bucket_name, TEST_OBJ_KEY);
                Aws::String body = buildRequestBody(this->sql);

                cout << endl <<
                        "Sending request:" << endl <<
                        "URL: " << url << endl <<
                        "Body: " << endl << body << endl;

                Aws::StringStream content_length;

                content_length << std::to_string(body.length());

                std::shared_ptr<Aws::StringStream> body_ss = Aws::MakeShared<Aws::StringStream>(ALLOCATION_TAG);
                *body_ss << body;

                std::shared_ptr<HttpRequest> request = buildRequest(url);
                request->SetContentLength(content_length.str());
                request->AddContentBody(body_ss);

                cout << "MAKING REQUEST" << endl;
//                std::this_thread::sleep_for(std::chrono::milliseconds(2000));


                std::shared_ptr<HttpResponse> response = m_HttpClient->MakeRequest(request);

                cout << "MADE REQUEST" << endl;
//                std::this_thread::sleep_for(std::chrono::milliseconds(2000));

                HttpResponseCode code = response->GetResponseCode();
                cout << static_cast<int>(code);

                Aws::IOStream& io_stream = response->GetResponseBody();

                cout << "PARSING" << endl;


                aws_allocator* alloc = aws_default_allocator();

                struct test_decoder_data decoder_data = {
                    .latest_prelude = {},
                    .latest_header_name = {},
                    .latest_header_value = {},
                    .latest_payload = 0,
                    .written = 0,
                    .alloc = alloc,
                    .latest_error = 0,
                    .last_line = NULL,
                    .data_v = new std::vector<std::vector<std::string>>()
                };

                cout << "Creating decoder" << endl << std::flush;

                struct aws_event_stream_streaming_decoder decoder;
                aws_event_stream_streaming_decoder_init(&decoder,
                                                        alloc,
                                                        s_decoder_test_on_payload_segment,
                                                        s_decoder_test_on_prelude_received,
                                                        s_decoder_test_header_received,
                                                        s_decoder_test_on_error,
                                                        &decoder_data);

                cout << "Starting" << endl << std::flush;

                while(!io_stream.eof()){

                    cout << "Reading" << endl << std::flush;

//                    aws_byte_buf* buf[1024 * 1024];
//                      char str[1024 * 1024];
//                    io_stream.read(&str, 1024 * 1024);

                    char bytes[256];
                    io_stream.read(&bytes[0], 256);

                    const aws_byte_buf buf = aws_byte_buf_from_array((const uint8_t*)bytes, sizeof(bytes));

                        int stuff = aws_event_stream_streaming_decoder_pump(&decoder, &buf);
                        cout << "Return val from pump: " << stuff << endl << std::flush;
                }

                cout << "Done" << endl << std::flush;

                int num_cols = decoder_data.data_v[0][0].size();
                int num_rows = decoder_data.data_v->size();

                cout << "Rows:" << num_rows << ", Cols:" << num_cols << endl;

                cout << "TO NUMPY..." << endl;

                const int ND = 2;
                npy_intp dims[2]{num_rows, num_cols};


                std::vector<std::vector<std::string>> *vec = decoder_data.data_v;

                for(int i=0;i<vec->size();i++){

                    cout << "Row " << i << ": ";

                    std::vector<std::string> row = (*vec)[i];
                    for(int j=0;j<row.size();j++){
                        cout << row[j] << " | ";
                    }

                    cout << endl;
                }

                pArray = (PyArrayObject*)(PyArray_SimpleNewFromData(ND, dims, NPY_OBJECT, vec->data()));
                if (pArray == NULL) {
                    Py_XDECREF(pArray);
                    return NULL;
                }

                cout << "DONE NUMPY..." << endl;

                npy_intp dims6[2];
                dims6[0] = num_rows;
                dims6[1] = num_cols;
                pArray = (PyArrayObject*)PyArray_New(&PyArray_Type,2, dims6, NPY_OBJECT, nullptr, nullptr, 0, 0, nullptr);

                PyObject *obj3;

                for(int i=0;i<vec->size();i++){
                    std::vector<std::string> row = (*vec)[i];
                    for(int j=0;j<row.size();j++){
                        obj3 = PyString_FromString(row[j].c_str());
                        PyArray_SETITEM((PyArrayObject*)pArray, (char*)PyArray_GETPTR2((PyArrayObject*)pArray, i, j), obj3);
                    }
                }


//                int elements_count = 10;
//                std::string const &dtype2 = "M8[ms]";
//                int int64_data[7] = {1,2,3,4,5,6,7};
//
//
//                int buffer_size = elements_count*sizeof(int);
//                npy_intp dims2 = elements_count;
//                PyObject *date_type = Py_BuildValue("s", dtype2.c_str());
//                PyArray_Descr *descr;
//                PyArray_DescrConverter(date_type, &descr);
//                Py_XDECREF(date_type);
//                pArray = (PyArrayObject*)PyArray_SimpleNewFromDescr(1, &dims2, descr);
//                memcpy(PyArray_BYTES(pArray), &(int64_data[0]), buffer_size);



//                int size = PyArray_SIZE(pArray);
//                cout << size << endl;
//
//                int ndX = PyArray_NDIM(pArray);
//                cout << ndX << endl;


//                char** dataptr;
//
//                int ndX;
//                npy_intp *shapeX;
//                PyArray_Descr *dtype;
//                NpyIter *iter;
//                NpyIter_IterNextFunc *iternext;
//
//                ndX = PyArray_NDIM(pArray);
//                int size = PyArray_SIZE(pArray);
//                cout << size << endl;
//
//                shapeX = PyArray_SHAPE(pArray);
//                dtype = PyArray_DescrFromType(NPY_OBJECT);
//
//                iter = NpyIter_New(pArray, NPY_ITER_READONLY | NPY_ITER_REFS_OK, NPY_KEEPORDER, NPY_NO_CASTING, dtype);
//                if (iter!=NULL) {
//                    iternext = NpyIter_GetIterNext(iter, NULL);
//                    dataptr = (char **) NpyIter_GetDataPtrArray(iter);
//
//                    do {
//                        char* data = *dataptr;
//                        cout << "[" << data << "]";
//                    } while (iternext(iter));
//
//                    NpyIter_Deallocate(iter);
//                }

                cout << "ITER NUMPY..." << endl;


                // Can tell if response is OK here
//                Aws::String MT(":");
//                int done = 0;
//                Aws::String line;
//                while(!done){
//                    std::getline(io_stream, line);
//                    cout << "Line is" << endl;
//                    cout << line << endl;
//
//                    cout << "[" << line.at(16) << "]" << endl;
//
//
//                    bool ass = starts_with(line, MT);
//
//                    cout << ass << endl;
//
//                    if(!line.compare(0, MT.size(), MT)){
//                        cout << "New message" << endl;
//                    }
//                    else{
//                        cout << "Not a new message" << endl;
//                    }
//
//
//
//
//
//
//                    if(!io_stream)
//                        done = true;
//                }
//
//
//                Aws::StringStream ss;
//                ss << io_stream.rdbuf();
//                ss.seekg(0, ios::end);
//                int size = ss.tellg();

//                std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now();
//
//                std::chrono::duration<double, std::milli> time_span = finish - start;
//
//                std::cout << endl <<
//                        "Elapsed: " << time_span.count() << " millis." << endl <<
//                        "Size: " << size << endl;
//
//                std::cout << "Response:" << endl;
//    			cout << ss.str() << endl;
            }
            Aws::ShutdownAPI(options);
            Aws::Utils::Logging::ShutdownAWSLogging();
        }
        catch(std::exception const& e)
        {
            std::cerr << "Error: " << e.what() << std::endl;
//            return EXIT_FAILURE;
        }

//        Py_INCREF(Py_None);
//        return Py_None;

//        Py_INCREF(pArray);
        return PyArray_Return(pArray);
    }
};


static PyObject *
scan_system(PyObject *self, PyObject *args)
{
    const char *command;
    int sts;

    if (!PyArg_ParseTuple(args, "s", &command))
        return NULL;
    sts = system(command);
    return Py_BuildValue("i", sts);
}

static PyObject *
scan_execute(PyObject *self, PyObject *args)
{
    const char *key;
    const char *sql;

    if (!PyArg_ParseTuple(args, "ss", &key, &sql)){
        return NULL;
    }

    std::cout << std::endl;
    std::cout << "Execute | Key: " << key << ", sql: " << sql << std::endl;

    Reader* reader = new Reader(key, sql);
    PyObject* res = reader->execute();

//    std::cout << res;

    return res;
}

static PyMethodDef ScanMethods[] = {
    {"stuff",  scan_system, METH_VARARGS, "Execute a shell command."},
    {"execute",  scan_execute, METH_VARARGS, "Execute an S3 Select query."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initscan(void)
{
    (void) Py_InitModule("scan", ScanMethods);
    import_array();
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
