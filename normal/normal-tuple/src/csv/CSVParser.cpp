//
// Created by matt on 19/12/19.
//

#include <normal/tuple/csv/CSVParser.h>

using namespace normal::tuple::csv;
using namespace normal::tuple;

CSVParser::CSVParser(const std::string &filePath) : filePath_(filePath) {
}

CSVParser::CSVParser(const std::string &filePath, long startOffset, long finishOffset) :
	filePath_(filePath), startOffset_(startOffset), finishOffset_(finishOffset) {
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
	auto maybeChunkBuffer = inputStream_.value()->Read(ChunkSize);
	if (!maybeChunkBuffer.ok())
	  return tl::unexpected(maybeChunkBuffer.status().ToString());
	auto chunkBuffer = *maybeChunkBuffer;

	if (chunkBuffer->size() <= 0) {
	  // Found EOF, return
	  return blockBuffer;
	} else {
	  // See if the chunk has a newline
	  auto chunkString = ::arrow::util::string_view(*chunkBuffer);
	  auto newLinePos = chunkString.find('\n');

	  if (newLinePos == (unsigned long)-1) {
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

	status = bufferBuilder.Finish(&blockBuffer);
	if (!status.ok()) {
	  return tl::unexpected(status.ToString());
	}
  }

  return blockBuffer;
}

tl::expected<std::shared_ptr<TupleSet2>, std::string> CSVParser::parse() {

  ::arrow::Status status;

  // Open input stream
  if(!inputStream_.has_value()) {
	auto expectedInputStream = openInputStream();
	if (!expectedInputStream.has_value())
	  return tl::unexpected(expectedInputStream.error());
  }

  // Parse schema
  auto expectedSchema = parseSchema();
  if (!expectedSchema.has_value())
	return tl::unexpected(expectedSchema.error());
  auto schema_ = expectedSchema.value();

  arrow::csv::BlockParser blockParser{arrow::csv::ParseOptions::Defaults(),
									  static_cast<int32_t>(schema_->fields().size())};
  uint32_t numBytesParsed;

  bool done = false;
  ::arrow::BufferBuilder bufferBuilder;

  int64_t dataStartOffset = inputStream_.value()->Tell().ValueOrDie();
  int64_t currentOffset = dataStartOffset;

  // Advance the input stream to the start offset
  status = inputStream_.value()->Seek((dataStartOffset + startOffset_) - 1);
  if (!status.ok())
	return tl::unexpected(status.ToString());
  advanceToNewLine();

  while (!done) {

	assert(currentOffset >= startOffset_);
	assert(currentOffset <= finishOffset_);

	// Read a chunk of data, no more than up to finish offset (inclusive)
	long numBytesToRead = std::min(currentOffset + ChunkSize, (finishOffset_ - currentOffset) + 1);
	auto maybeChunkBuffer = inputStream_.value()->Read(numBytesToRead);
	if (!maybeChunkBuffer.ok())
	  return tl::unexpected(maybeChunkBuffer.status().ToString());
	auto chunkBuffer = *maybeChunkBuffer;

	currentOffset = currentOffset + chunkBuffer->size();

	if (currentOffset - 1 == finishOffset_) {
	  // Have scanned up to finish offset, check if we are already at EOR
	  // See if the chunk contains a newline
	  auto chunkString = ::arrow::util::string_view(*chunkBuffer);
	  auto newLinePos = chunkString.find('\n');

	  if (newLinePos == std::string_view::npos) {
		// We are not at EOR, advance to newline
		auto maybeLastChunkBuffer = advanceToNewLine();
		if (!maybeLastChunkBuffer.has_value())
		  return tl::unexpected(maybeLastChunkBuffer.error());
		auto lastChunkBuffer = maybeLastChunkBuffer.value();

		auto expectedChunkBuffer = concatenateBuffers(chunkBuffer, lastChunkBuffer);
		if (!expectedChunkBuffer.has_value())
		  return tl::unexpected(expectedChunkBuffer.error());

		chunkBuffer = expectedChunkBuffer.value();
	  }

	  done = true;
	}

	if (chunkBuffer->size() <= 0) {
	  // No more bytes to read, done
	  done = true;
	} else {
	  // Parse the buffer
	  status = blockParser.Parse(::arrow::util::string_view(*chunkBuffer), &numBytesParsed);
	  if (!status.ok())
		return tl::unexpected(status.ToString());
	}
  }

  // Get the parsed arrays
  auto expectedArrays = extractArrays(blockParser);
  if (!expectedArrays.has_value())
	return tl::unexpected(expectedArrays.error());

  return TupleSet2::make(schema_->getSchema(), expectedArrays.value());
}

tl::expected<std::shared_ptr<::arrow::Buffer>, std::string>
CSVParser::concatenateBuffers(std::shared_ptr<::arrow::Buffer> buffer1, std::shared_ptr<::arrow::Buffer> buffer2) {

  ::arrow::Status status;
  std::shared_ptr<::arrow::Buffer> buffer;

  auto bufferVector = {buffer1, buffer2};

  status = ::arrow::ConcatenateBuffers(bufferVector, ::arrow::default_memory_pool(), &buffer);
  if (!status.ok())
	return tl::unexpected(status.ToString());

  return buffer;
}

tl::expected<std::shared_ptr<Schema>, std::string> CSVParser::parseSchema() {

  ::arrow::Status status;
  ::arrow::csv::BlockParser blockParser{arrow::csv::ParseOptions::Defaults()};
  unsigned int numBytesParsed;

  if(!inputStream_.has_value()) {
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
