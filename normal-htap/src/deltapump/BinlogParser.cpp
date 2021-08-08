//
// Created by ZhangOscar on 8/8/21.
//

include "BinlogParser.h"

std::vector<std::vector<std::string>> from_java(JNIEnv *env, jobjectArray arr) {
    // always on the lookout for null pointers. Everything we get from Java can be null.
    jsize OuterDim = arr ? env->GetArrayLength(arr) : 0;
    std::vector<std::vector<std::string> > result(OuterDim);

    for(jsize i = 0; i < OuterDim; ++i) {
        jobjectArray inner = static_cast<jobjectArray>(env->GetObjectArrayElement(arr, i));

        // null pointer check
        if(inner) {
            // Get the inner array length here.
            jsize InnerDim = env->GetArrayLength(inner);

            for (int j=0; j<InnerDim; j++) {
                jstring cell = (jstring) (env->GetObjectArrayElement(inner, j));

                const char *rawString = env->GetStringUTFChars(cell, 0);
                std::string str = rawString;

                result[i].insert(result[i].end(), str);

                //`ReleaseStringUTFChars` when done.
                env->ReleaseStringUTFChars(cell, rawString);
                env->DeleteLocalRef(cell);
            }
        }
    }

    return result;
}

std::vector<std::vector<std::string>> parse(const char *filePath) {
    // code to call parser functions in java
    const int kNumOptions = 4;
    JavaVMOption options[kNumOptions] = {
            {const_cast<char *>("-Xmx512m"),                                                          NULL},
            {const_cast<char *>("-verbose:gc"),                                                       NULL},
            {const_cast<char *>("-XX:G1ReservePercent=5"),                                            NULL},
            {const_cast<char *>("-Djava.class.path=./Parser.jar:./mysql-binlog-connector-java-0.25.1.jar"), NULL}
    };

    JavaVMInitArgs vm_args;
    vm_args.version = JNI_VERSION_1_6;
    vm_args.options = options;
    vm_args.nOptions = sizeof(options) / sizeof(JavaVMOption);
    assert(vm_args.nOptions == kNumOptions);

    JNIEnv *env = NULL;
    JavaVM *jvm = NULL;
    int res = JNI_CreateJavaVM(&jvm, reinterpret_cast<void **>(&env), &vm_args);
    if (res != JNI_OK) {
        std::cerr << "FAILED: JNI_CreateJavaVM " << res << std::endl;
        exit(-1);
    }

    const char *kClassName = "binlog_parser/Parser";
    jclass cls = env->FindClass(kClassName);
    if (cls == NULL) {
        std::cerr << "FAILED: FindClass" << std::endl;
        exit(-1);
    }

    const char *kMethodName = "parseBinlogFile";
    jmethodID mid = env->GetStaticMethodID(cls, kMethodName, "(Ljava/lang/String;)[[Ljava/lang/String;");
    if (mid == NULL) {
        std::cerr << "FAILED: GetStaticMethodID" << std::endl;
        exit(-1);
    }

    const jsize kNumArgs = 1;
    jclass string_cls = env->FindClass("java/lang/String");
    jobject initial_element = NULL;
    jobjectArray method_args = env->NewObjectArray(kNumArgs, string_cls, initial_element);

    jstring method_args_0 = env->NewStringUTF(filePath);
    env->SetObjectArrayElement(method_args, 0, method_args_0);

    // receive string 2d array from java
    jobjectArray java_cells = static_cast<jobjectArray>(env->CallStaticObjectMethod(cls, mid, method_args_0));


    // convert java 2d array to c++ vector
    std::vector <std::vector<std::string>> cpp_cells = from_java(env, java_cells);


    jvm->DestroyJavaVM();

    return cpp_cells;
}