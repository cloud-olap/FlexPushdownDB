//============================================================================
// Name        : cpp-s3filter.cpp
//============================================================================

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/platform/Platform.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/UUID.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/crypto/Factories.h>
#include <aws/core/utils/crypto/Cipher.h>
#include <aws/core/utils/HashingUtils.h>

#include <cstdlib>
#include <iostream>
#include <string>

using namespace std;
using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Http;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils;


Aws::String buildURI(std::shared_ptr<S3Client> s3_client, Aws::String bucket_name, const char* TEST_OBJ_KEY){
	Aws::String uri = s3_client->GeneratePresignedUrl(bucket_name, TEST_OBJ_KEY, HttpMethod::HTTP_POST);
	return uri;
}

std::shared_ptr<HttpRequest> buildRequest(Aws::String url){
	std::shared_ptr<HttpRequest> request = Aws::Http::CreateHttpRequest(url, HttpMethod::HTTP_POST, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
	return request;
}

Aws::String buildRequestBody(){
	Aws::StringStream body_ss;
	body_ss << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
		"<SelectRequest>"
			"<Expression>Select * from S3Object</Expression>"
			"<ExpressionType>SQL</ExpressionType>"
			"<InputSerialization>"
				"<CSV>"
					"<FileHeaderInfo>IGNORE</FileHeaderInfo>"
				"</CSV>"
			"</InputSerialization>"
			"<OutputSerialization>"
				"<CSV>"
				"</CSV>"
		   " </OutputSerialization>"
		"</SelectRequest> ";

	return body_ss.str();
}


int main(int argc, char** argv) {

    try
    {
    	Aws::SDKOptions options;
    	options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    	Aws::InitAPI(options);
    	{
    		const Aws::String bucket_name = "s3filter";
    		static const char* TEST_OBJ_KEY = "lineitem.csv?select&select-type=2";
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
			Aws::String body = buildRequestBody();

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
			std::shared_ptr<HttpResponse> response = m_HttpClient->MakeRequest(request);

			HttpResponseCode code = response->GetResponseCode();
			cout << static_cast<int>(code);
			Aws::StringStream ss;
			ss << response->GetResponseBody().rdbuf();
			ss.seekg(0, ios::end);
			int size = ss.tellg();

			std::chrono::high_resolution_clock::time_point finish = std::chrono::high_resolution_clock::now();

			std::chrono::duration<double, std::milli> time_span = finish - start;

			std::cout << endl <<
					"Elapsed: " << time_span.count() << " millis." << endl <<
					"Size: " << size << endl;

//			cout << ss.str();

    	}
    	Aws::ShutdownAPI(options);
	}
	catch(std::exception const& e)
	{
		std::cerr << "Error: " << e.what() << std::endl;
		return EXIT_FAILURE;
	}

    cout << "!!!Hello World!!!" << endl; // prints !!!Hello World!!!

    return EXIT_SUCCESS;
}
