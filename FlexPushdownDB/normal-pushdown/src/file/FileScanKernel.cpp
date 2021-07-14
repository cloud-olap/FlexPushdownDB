//
// Created by matt on 12/8/20.
//

#include "normal/pushdown/file/FileScanKernel.h"

using namespace normal::pushdown::file;

FileScanKernel::FileScanKernel(std::string path,
							   FileType fileType,
							   std::shared_ptr<FileReader> reader,
							   unsigned long startPos,
							   unsigned long finishPos) :
	path_(std::move(path)),
	fileType_(fileType),
	reader_(std::move(reader)),
	startPos_(startPos),
	finishPos_(finishPos) {}

std::unique_ptr<FileScanKernel> FileScanKernel::make(const std::string &path,
													 FileType fileType,
													 unsigned long startPos,
													 unsigned long finishPos) {

  auto reader = FileReaderBuilder::make(path, fileType);

  return std::make_unique<FileScanKernel>(path,
										  fileType,
										  std::move(reader),
										  startPos,
										  finishPos);
}

tl::expected<std::shared_ptr<TupleSet2>, std::string>
FileScanKernel::scan(const std::vector<std::string> &columnNames) {
  return reader_->read(columnNames, startPos_, finishPos_);
}

const std::string &FileScanKernel::getPath() const {
  return path_;
}

const std::optional<FileType> &FileScanKernel::getFileType() const {
  return fileType_;
}

unsigned long FileScanKernel::getStartPos() const {
  return startPos_;
}

unsigned long FileScanKernel::getFinishPos() const {
  return finishPos_;
}
