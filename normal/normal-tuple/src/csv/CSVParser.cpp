//
// Created by matt on 19/12/19.
//

#include <normal/tuple/csv/CSVParser.h>

#include <utility>

using namespace normal::tuple::csv;
using namespace normal::tuple;

CSVParser::CSVParser(std::string filePath,
					 int64_t startOffset,
					 std::optional<int64_t> finishOffset,
					 int64_t bufferSize) :
	filePath_(std::move(filePath)), startOffset_(startOffset), finishOffset_(finishOffset), bufferSize_(bufferSize) {
}

CSVParser::CSVParser(const std::string &filePath, int64_t bufferSize) :
	CSVParser(filePath, 0, std::nullopt, bufferSize) {
}

CSVParser::CSVParser(const std::string &filePath) :
	CSVParser(filePath, DefaultBufferSize) {
}

tl::expected<void, std::string> CSVParser::openInputStream() {
  auto result = ::arrow::io::ReadableFile::Open(filePath_);
  if (!result.ok()) {
	return tl::unexpected(result.status().message());
  } else {
	inputStream_ = std::optional(*result);
	return {};
  }
}

tl::expected<std::shared_ptr<::arrow::Buffer>, std::string> CSVParser::advanceToNewLine() {

  ::arrow::Status status;
  ::arrow::BufferBuilder bufferBuilder;

  std::shared_ptr<arrow::Buffer> blockBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);

  bool done = false;
  while (!done) {

	long chunkStartOffset = inputStream_.value()->Tell().ValueOrDie();

	// Read a chunk
	auto maybeChunkBuffer = inputStream_.value()->Read(bufferSize_);
	if (!maybeChunkBuffer.ok())
	  return tl::unexpected(maybeChunkBuffer.status().ToString());
	auto chunkBuffer = *maybeChunkBuffer;

//	SPDLOG_DEBUG("Buffer: {}", chunkBuffer->ToString());

	if (chunkBuffer->size() <= 0) {
	  // Found EOF, return
	  return blockBuffer;
	} else {
	  // See if the chunk has a newline
	  auto chunkString = ::arrow::util::string_view(*chunkBuffer);
	  auto newLinePos = chunkString.find('\n');

	  if (newLinePos == std::string_view::npos) {
		// No newline, append to block buffer and read more
		status = bufferBuilder.Append(chunkBuffer->data(), chunkBuffer->size());
		if (!status.ok()) {
		  return tl::unexpected(status.ToString());
		}
	  } else {
		// Found newline
		// Append up to new line position to block buffer
		status = bufferBuilder.Append(chunkBuffer->data(), newLinePos + 1);
		if (!status.ok()) {
		  return tl::unexpected(status.ToString());
		}

		// Seek to new line position
		status = inputStream_.value()->Seek(chunkStartOffset + newLinePos + 1);
		if (status.ok()) {
		  done = true;
		} else {
		  return tl::unexpected(status.ToString());
		}
	  }
	}
  }

  status = bufferBuilder.Finish(&blockBuffer);
  if (!status.ok()) {
	return tl::unexpected(status.ToString());
  }

  return blockBuffer;
}

tl::expected<std::shared_ptr<TupleSet2>, std::string> CSVParser::parse() {

  ::arrow::Status status;

  // Open input stream
  if (!inputStream_.has_value()) {
	auto expectedInputStream = openInputStream();
	if (!expectedInputStream.has_value())
	  return tl::unexpected(expectedInputStream.error());
  }

  // Parse schema
  auto expectedSchema = parseSchema();
  if (!expectedSchema.has_value())
	return tl::unexpected(expectedSchema.error());
  auto schema = expectedSchema.value();

  std::vector<std::shared_ptr<::arrow::Buffer>> blockBuffers;
  arrow::csv::BlockParser blockParser{arrow::csv::ParseOptions::Defaults(),
									  static_cast<int32_t>(schema->fields().size())};

  bool done = false;

  // The offset where the data starts
  int64_t dataStartOffset = inputStream_.value()->Tell().ValueOrDie();

  // Advance the input stream to (just before) the start offset
  status = inputStream_.value()->Seek((dataStartOffset + startOffset_) - 1);
  if (!status.ok())
	return tl::unexpected(status.ToString());
  advanceToNewLine();

  int64_t currentOffset = inputStream_.value()->Tell().ValueOrDie();

  while (!done) {

	assert(currentOffset >= dataStartOffset + startOffset_);
	assert(finishOffset_.has_value() ? currentOffset <= dataStartOffset + finishOffset_.value() : true);

	// Read a chunk of data, no more than up to finish offset (inclusive)
	long numBytesToRead = finishOffset_.has_value() ?
						  std::min(currentOffset + bufferSize_,
								   ((dataStartOffset + finishOffset_.value()) - currentOffset) + 1) :
						  currentOffset + bufferSize_;
	auto maybeChunkBuffer = inputStream_.value()->Read(numBytesToRead);
	if (!maybeChunkBuffer.ok())
	  return tl::unexpected(maybeChunkBuffer.status().ToString());
	auto chunkBuffer = *maybeChunkBuffer;

	SPDLOG_DEBUG("Chunk: '{}'", chunkBuffer->ToString());

	// Advance to EOR
	auto expectedBlockBuffer = advanceToEOR(chunkBuffer);
	if (!expectedBlockBuffer.has_value())
	  return tl::unexpected(expectedBlockBuffer.error());
	auto blockBuffer = expectedBlockBuffer.value();

	SPDLOG_DEBUG("Block: '{}'", blockBuffer->ToString());

	// Add the buffer to the block vector
	blockBuffers.push_back(blockBuffer);

	currentOffset = currentOffset + chunkBuffer->size();

	if (chunkBuffer->size() <= 0) {
	  // No more bytes to read, done
	  done = true;
	}

	if (finishOffset_.has_value() && currentOffset >= dataStartOffset + finishOffset_.value()) {
	  // Have scanned up to finish offset, done
	  done = true;
	}
  }

  std::vector<::arrow::util::string_view> blockStrings;
  blockStrings.reserve(blockBuffers.size());
  for (const auto &buffer: blockBuffers) {
	blockStrings.push_back(::arrow::util::string_view(*buffer));
  }

  uint32_t numBytesParsed;
  status = blockParser.Parse(blockStrings, &numBytesParsed);
  if (!status.ok())
	return tl::unexpected(status.ToString());

  // Get the parsed arrays
  auto expectedArrays = extractArrays(blockParser);
  if (!expectedArrays.has_value())
	return tl::unexpected(expectedArrays.error());

  return TupleSet2::make(schema->getSchema(), expectedArrays.value());
}

tl::expected<std::shared_ptr<::arrow::Buffer>, std::string>
CSVParser::concatenateBuffers(std::shared_ptr<::arrow::Buffer> buffer1, std::shared_ptr<::arrow::Buffer> buffer2) {

  ::arrow::Status status;
  std::shared_ptr<::arrow::Buffer> buffer;

  auto bufferVector = {std::move(buffer1), std::move(buffer2)};

  status = ::arrow::ConcatenateBuffers(bufferVector, ::arrow::default_memory_pool(), &buffer);
  if (!status.ok())
	return tl::unexpected(status.ToString());

  return buffer;
}

tl::expected<std::shared_ptr<Schema>, std::string> CSVParser::parseSchema() {

  ::arrow::Status status;
  ::arrow::csv::BlockParser blockParser{arrow::csv::ParseOptions::Defaults()};
  unsigned int numBytesParsed;

  if (!inputStream_.has_value()) {
	auto expectedInputStream = openInputStream();
	if (!expectedInputStream.has_value())
	  return tl::unexpected(expectedInputStream.error());
  }

  auto expectedBlockBuffer = advanceToNewLine();
  if (!expectedBlockBuffer.has_value())
	return tl::unexpected(expectedBlockBuffer.error());
  auto blockBuffer = expectedBlockBuffer.value();

  // Parse the buffer
  status = blockParser.Parse(::arrow::util::string_view(*blockBuffer), &numBytesParsed);
  if (!status.ok())
	return tl::unexpected(status.ToString());

  auto expectedSchema = extractSchema(blockParser);

  return expectedSchema;
}

std::shared_ptr<Schema> CSVParser::extractSchema(const arrow::csv::BlockParser &blockParser) {

  assert(blockParser.num_rows() == 1);

  ::arrow::Status status;
  std::vector<std::shared_ptr<::arrow::Field>> fields;

  for (int i = 0; i < blockParser.num_cols(); ++i) {

	status = blockParser.VisitColumn(i, [&](const uint8_t *data, uint32_t size, bool quoted) -> arrow::Status {
	  std::string fieldName{reinterpret_cast<const char *>(data), size};

	  // Remove quotes
	  if (quoted) {
		fieldName.erase(0);
		fieldName.erase(fieldName.length());
	  }

	  // Canonicalize
	  auto canonicalFieldName = ColumnName::canonicalize(fieldName);

	  auto field = std::make_shared<::arrow::Field>(canonicalFieldName, arrow::utf8());
	  fields.push_back(field);

	  return arrow::Status::OK();
	});
  }

  return Schema::make(std::make_shared<::arrow::Schema>(fields));
}

tl::expected<std::vector<std::shared_ptr<::arrow::Array>>, std::string>
CSVParser::extractArrays(const arrow::csv::BlockParser &blockParser) {

  ::arrow::Status status;
  std::vector<std::shared_ptr<::arrow::Array>> arrays;

  for (int i = 0; i < blockParser.num_cols(); ++i) {

	::arrow::StringBuilder arrayBuilder;

	status = blockParser.VisitColumn(i, [&](const uint8_t *data, uint32_t size, bool quoted) -> arrow::Status {
	  std::string cell{reinterpret_cast<const char *>(data), size};

	  // Remove quotes
	  if (quoted) {
		cell.erase(0);
		cell.erase(cell.length());
	  }

	  // Append cell
	  status = arrayBuilder.Append(cell);
	  if (!status.ok())
		return status;
	  return arrow::Status::OK();
	});

	if (!status.ok())
	  return tl::unexpected(status.message());

	std::shared_ptr<arrow::StringArray> array;
	status = arrayBuilder.Finish(&array);
	if (!status.ok())
	  return tl::unexpected(status.message());
	arrays.push_back(array);
  }

  return arrays;
}

tl::expected<std::shared_ptr<::arrow::Buffer>, std::string>
CSVParser::advanceToEOR(std::shared_ptr<::arrow::Buffer> buffer) {

  // See if the chunk ends with a newline
  auto chunkString = ::arrow::util::string_view(*buffer);

  if (!chunkString.ends_with('\n')) {
	// We are not at EOR, advance to newline
	auto expectedRemainingBuffer = advanceToNewLine();
	if (!expectedRemainingBuffer.has_value())
	  return tl::unexpected(expectedRemainingBuffer.error());
	auto remainingBuffer = expectedRemainingBuffer.value();

	auto expectedBuffer = concatenateBuffers(buffer, remainingBuffer);
	if (!expectedBuffer.has_value())
	  return tl::unexpected(expectedBuffer.error());

	buffer = expectedBuffer.value();
  }

  return buffer;
}
