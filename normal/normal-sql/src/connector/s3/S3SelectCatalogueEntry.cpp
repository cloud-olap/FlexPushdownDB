//
// Created by matt on 27/3/20.
//

#include "connector/s3/S3SelectCatalogueEntry.h"

#include <utility>
S3SelectCatalogueEntry::S3SelectCatalogueEntry(const std::string& Alias,
                                               std::string S3Bucket,
                                               std::string S3Object)
    : CatalogueEntry(Alias), s3Bucket_(std::move(S3Bucket)), s3Object_(std::move(S3Object)) {}
