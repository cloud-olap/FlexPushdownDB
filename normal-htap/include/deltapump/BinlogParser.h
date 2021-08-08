//
// Created by ZhangOscar on 8/8/21.
//

#ifndef NORMAL_BINLOGPARSER_H
#define NORMAL_BINLOGPARSER_H

#include <jni.h>
#include <cassert>
#include <iostream>
#include <vector>
#include <string>
#include <cstdio>
#include <ctime>

/*
 * Convert java array to c++ vector
 */
std::vector<std::vector<std::string> > from_java(JNIEnv *env, jobjectArray arr);

/*
 * function to call functions in java and receive 2d array returned from java side
 * row data in `string format | timestamp | type`
 */
std::vector<std::vector<std::string>> parse(const char *filePath);

#endif //NORMAL_BINLOGPARSER_H
