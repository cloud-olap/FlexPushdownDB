//
// Created by matt on 19/12/19.
//

#include <fpdb/tuple/csv/CSVParser.h>

#include <utility>

using namespace fpdb::tuple::csv;
using namespace fpdb::tuple;

CSVParser::CSVParser(std::shared_ptr<::arrow::io::RandomAccessFile> inputStream,
                     std::shared_ptr<::arrow::Schema> schema,
                     std::optional<std::vector<std::string>> columnNames,
                     int64_t startPos,
                     std::optional<int64_t> finishPos,
                     int64_t bufferSize) :
	inputStream_(std::move(inputStream)),
  schema_(std::move(schema)),
	columnNames_(std::move(columnNames)),
	startPos_(startPos),
	finishPos_(finishPos),
  bufferSize_(bufferSize) {
}

tl::expected<std::shared_ptr<::arrow::Buffer>, std::string> CSVParser::advanceToNewLine() {

  ::arrow::Status status;
  ::arrow::BufferBuilder bufferBuilder;

  std::shared_ptr<arrow::Buffer> blockBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);

  bool done = false;
  while (!done) {

	long chunkStartOffset = inputStream_->Tell().ValueOrDie();

	// Read a chunk
	auto maybeChunkBuffer = inputStream_->Read(bufferSize_);
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
		status = inputStream_->Seek(chunkStartOffset + newLinePos + 1);
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

tl::expected<std::shared_ptr<TupleSet>, std::string> CSVParser::parse() {

  ::arrow::Status status;

  // Skip the first row which are column names
  advanceToNewLine();

  std::vector<std::shared_ptr<::arrow::Buffer>> blockBuffers;

  /*
   * FIXME: max_num_rows is set to max of an int. Feels hacky.
   */
  arrow::csv::BlockParser blockParser{arrow::csv::ParseOptions::Defaults(),
									  static_cast<int32_t>(schema_->fields().size()),
									  std::numeric_limits<int>::max()};

  bool done = false;

  // The offset where the data starts
  int64_t dataStartPos = inputStream_->Tell().ValueOrDie();

  // Advance the input stream to (just before) the start pos
  status = inputStream_->Seek((dataStartPos + startPos_) - 1);
  if (!status.ok())
	return tl::unexpected(status.ToString());
  advanceToNewLine();

  int64_t currentPos = inputStream_->Tell().ValueOrDie();

  while (!done) {

	assert(currentPos >= dataStartPos + startPos_);
	assert(finishPos_.has_value() ? currentPos <= dataStartPos + finishPos_.value() : true);

	// Read a chunk of data, no more than up to finish offset (inclusive)
	long numBytesToRead = finishPos_.has_value() ?
						  std::min(currentPos + bufferSize_,
								   ((dataStartPos + finishPos_.value()) - currentPos)) :
						  currentPos + bufferSize_;

	assert(finishPos_.has_value() ? currentPos + numBytesToRead <= dataStartPos + finishPos_.value() : true);

	auto maybeChunkBuffer = inputStream_->Read(numBytesToRead);
	if (!maybeChunkBuffer.ok())
	  return tl::unexpected(maybeChunkBuffer.status().ToString());
	auto chunkBuffer = *maybeChunkBuffer;
	auto chunkSize = chunkBuffer->size();
	currentPos += chunkSize; // Get the number of bytes read from the size of the chunk

//	SPDLOG_DEBUG("Chunk: [{}]", chunkBuffer->ToString());

	// Advance to EOR
	auto expectedBlockBuffer = advanceToEOR(chunkBuffer);
	if (!expectedBlockBuffer.has_value())
	  return tl::unexpected(expectedBlockBuffer.error());
	auto blockBuffer = expectedBlockBuffer.value();
	auto blockSize = blockBuffer->size();
	currentPos += blockSize - chunkSize; // Get the number of bytes read from the difference between the chunk and block buffer sizes

//	SPDLOG_DEBUG("Block: [{}]", blockBuffer->ToString());

	// Add the buffer to the block vector
	blockBuffers.push_back(blockBuffer);

	if (chunkBuffer->size() <= 0) {
	  // No more bytes to read, done
	  done = true;
	}

	if (finishPos_.has_value() && currentPos >= dataStartPos + finishPos_.value()) {
	  // Have scanned up to finish offset, done
	  done = true;
	}
  }

  int bufferCount = 0;

  std::vector<::arrow::util::string_view> blockStrings;
  blockStrings.reserve(blockBuffers.size());
  for (const auto &buffer: blockBuffers) {
//    SPDLOG_DEBUG("Buffer {}  | start: {}...", bufferCount, buffer->ToString().substr(0, std::min(100L, buffer->size())));
//	SPDLOG_DEBUG("Buffer {}  | end: ...{}", bufferCount, buffer->ToString().substr(std::max(0L, buffer->size() - 100), buffer->size()));
	blockStrings.push_back(::arrow::util::string_view(*buffer));
	++bufferCount;
  }

  uint32_t numBytesParsed;
  status = blockParser.Parse(blockStrings, &numBytesParsed);
  if (!status.ok())
	return tl::unexpected(status.ToString());

  // Make output schema
  auto expOutputSchema = makeOutputSchema();
  if (!expOutputSchema.has_value()) {
    return tl::make_unexpected(expOutputSchema.error());
  }
  auto outputSchema = *expOutputSchema;

  // Get the parsed arrays
  auto expectedArrays = extractArrays(blockParser, schema_, columnNames_);
  if (!expectedArrays.has_value())
	return tl::unexpected(expectedArrays.error());

  auto tupleSet = TupleSet::make(outputSchema, expectedArrays.value());

  if(!status.ok())
	return tl::make_unexpected(status.message());

  return tupleSet;
}

tl::expected<std::shared_ptr<::arrow::Buffer>, std::string>
CSVParser::concatenateBuffers(std::shared_ptr<::arrow::Buffer> buffer1, std::shared_ptr<::arrow::Buffer> buffer2) {

  ::arrow::Status status;
  std::shared_ptr<::arrow::Buffer> buffer;

  auto bufferVector = {std::move(buffer1), std::move(buffer2)};

  auto expectedBuffer = ::arrow::ConcatenateBuffers(bufferVector, ::arrow::default_memory_pool());
  if (expectedBuffer.ok())
    buffer = *expectedBuffer;
  else
	return tl::unexpected(expectedBuffer.status().message());

  return buffer;
}

tl::expected<std::shared_ptr<::arrow::Schema>, std::string> CSVParser::makeOutputSchema() {
  if (!columnNames_.has_value()) {
    return schema_;
  }

  ::arrow::FieldVector outputFields;
  for (const auto &columnName: *columnNames_) {
    auto field = schema_->GetFieldByName(columnName);
    if (!field) {
      return tl::make_unexpected(fmt::format("Read CSV Error: column {} not found", columnName));
    }
    outputFields.emplace_back(field);
  }
  return ::arrow::schema(outputFields);
}

tl::expected<std::vector<std::shared_ptr<::arrow::Array>>, std::string>
CSVParser::extractArrays(const arrow::csv::BlockParser &blockParser,
						 const std::shared_ptr<::arrow::Schema>& csvFileSchema,
						 const std::optional<std::vector<std::string>> &columnNamesToRead) {

  ::arrow::Status status;
  std::vector<std::shared_ptr<::arrow::Array>> arrays;

  for (int i = 0; i < blockParser.num_cols(); ++i) {

    bool readColumn = false;
    auto csvFileField = csvFileSchema->field(i);
    if(!columnNamesToRead.has_value()) {
	  readColumn = true;
	}
    else{
      auto it = std::find(columnNamesToRead->begin(), columnNamesToRead->end(), csvFileField->name());
	  readColumn = it != columnNamesToRead->end();
    }

    if(readColumn) {
	  ::arrow::StringBuilder arrayBuilder;

	  status = blockParser.VisitColumn(i, [&](const uint8_t *data, uint32_t size, bool /*quoted*/) -> arrow::Status {
		std::string cell{reinterpret_cast<const char *>(data), size};

		// TODO: Seems quotes are already removed, need to figure out why the quoted flag is passed in?
		// Remove quotes
		//	  if (quoted) {
		//		cell.erase(0, 1);
		//		cell.erase(cell.length() - 1, cell.length());
		//	  }

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
