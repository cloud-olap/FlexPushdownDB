#include "BinlogParser.h"

avro::ValidSchema loadSchema(const char* filename)
{
    std::ifstream ifs(filename);
    avro::ValidSchema result;
    avro::compileJsonSchema(ifs, result);
    return result;
}


void parse(const char *filePath,  const char *rangeFilePath, std::unordered_map<int, std::set<struct lineorder_record>> **lineorder_record_ptr ){

    //get table_name, offset and range(fixed) of partitions for each table
    std::unordered_map<std::string, std::tuple<int, int>> range_result;
    std::ifstream inFile(rangeFilePath);
    if(!inFile.is_open()) throw std::runtime_error("Could not open range file");

    std::string lineStr, table_name;
    int offset, fixed_range;
    while(std::getline(inFile, lineStr)){
        std::stringstream ss(lineStr);
        std::string str;
        std::vector<std::string> lineArray;
        while(std::getline(ss, str, ',')){
            lineArray.push_back(str);
        }
        table_name = lineArray.at(0);
        offset = std::stoi(lineArray.at(1));
        fixed_range = std::stoi(lineArray.at(2));
        std::tuple<int, int> line_tuple;
        line_tuple = std::make_tuple(offset, fixed_range);
        range_result.insert(std::make_pair(table_name, line_tuple));
    }
    inFile.close();

    // code to call parser functions in java
    const int kNumOptions = 3;
    JavaVMOption options[kNumOptions] = {
            {const_cast<char *>("-Xmx512m"),                                                          NULL},
            {const_cast<char *>("-verbose:gc"),                                                       NULL},
            {const_cast<char *>("-Djava.class.path=./Parser.jar:./lib/mysql-binlog-connector-java-0.25.1.jar:./lib/avro-1.10.2.jar:./lib/avro-tools-1.10.2.jar"), NULL}
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
    jmethodID mid =
            env->GetStaticMethodID(cls, kMethodName, "(Ljava/lang/String;)[[B");
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

    auto byte_arr_list = static_cast<jobjectArray>(env->CallStaticObjectMethod(cls, mid, method_args_0));

    // extract serialized byte array for each table
    auto jlineorder = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 0);
    jsize lineorder_dim = jlineorder ? env->GetArrayLength(jlineorder) : 0;

    auto jcustomer = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 1);
    jsize customer_dim = jcustomer ? env->GetArrayLength(jcustomer) : 0;

    auto jsupplier = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 2);
    jsize supplier_dim = jsupplier ? env->GetArrayLength(jsupplier) : 0;

    auto jpart = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 3);
    jsize part_dim = jpart ? env->GetArrayLength(jpart) : 0;

    auto jdate = (jbyteArray) env->GetObjectArrayElement(byte_arr_list, 4);
    jsize date_dim = jdate ? env->GetArrayLength(jdate) : 0;

    // cast ti cpp byte array type
    jbyte *lineorder = env->GetByteArrayElements(jlineorder, 0);
    const auto* input_lineorder = (const uint8_t*) lineorder;

    jbyte *customer = env->GetByteArrayElements(jcustomer, 0);
    const auto* input_customer = (const uint8_t*) customer;

    jbyte *supplier = env->GetByteArrayElements(jsupplier, 0);
    const auto* input_supplier = (const uint8_t*) supplier;

    jbyte *part = env->GetByteArrayElements(jpart, 0);
    const auto* input_part = (const uint8_t*) part;

    jbyte *date = env->GetByteArrayElements(jdate, 0);
    const auto* input_date = (const uint8_t*) date;


    // put data into input streams
    std::unique_ptr<avro::InputStream> in_lineorder = avro::memoryInputStream(input_lineorder, (int) lineorder_dim);
    std::unique_ptr<avro::InputStream> in_customer = avro::memoryInputStream(input_customer, (int) customer_dim);
    std::unique_ptr<avro::InputStream> in_supplier = avro::memoryInputStream(input_supplier, (int) supplier_dim);
    std::unique_ptr<avro::InputStream> in_part = avro::memoryInputStream(input_part, (int) part_dim);
    std::unique_ptr<avro::InputStream> in_date = avro::memoryInputStream(input_date, (int) date_dim);

    // load schemas
    avro::ValidSchema lineorderSchema = loadSchema("./schemas/delta/lineorder_d.json");
    avro::ValidSchema customerSchema = loadSchema("./schemas/delta/customer_d.json");
    avro::ValidSchema supplierSchema = loadSchema("./schemas/delta/supplier_d.json");
    avro::ValidSchema partSchema = loadSchema("./schemas/delta/part_d.json");
    avro::ValidSchema dateSchema = loadSchema("./schemas/delta/date_d.json");


    //partition the lineorder table based on the range info
    auto lineorder_it = range_result.find("LINEORDER");
    int lineorder_offset = std::get<0>(lineorder_it->second);
    int lineorder_range = std::get<1>(lineorder_it->second);
    std::unordered_map<int, std::set<struct lineorder_record>> li_partitions;

    auto *lineorder_record_map = new std::unordered_map<int, std::set<struct lineorder_record>>;

    // read the data input stream with the given valid schema
    avro::DataFileReader <i::lineorder> lineorderReader(move(in_lineorder), lineorderSchema);
    i::lineorder l1;
    while (lineorderReader.read(l1)) {
        //lineorder
//        std::cout << '(' << l1.lo_orderkey << "," << l1.lo_linenumber << ","  << l1.lo_custkey << ","  << l1.lo_partkey << ","  << l1.lo_suppkey << ","  << l1.lo_orderdate << "," <<
//                  l1.lo_orderpriority  << "," << l1.lo_shippriority << ","  << l1.lo_quantity << ","  << l1.lo_extendedprice << ","  << l1.lo_discount << ","  << l1.lo_revenue << ","  <<
//                  l1.lo_supplycost << ","  << l1.lo_tax << ","  << l1.lo_commitdate << ","  << l1.lo_shipmode << ","  << l1.type << ","  << l1.timestamp << ')' << std::endl;

        //calculate the key for the record
        int key;
        if (l1.lo_orderkey <= lineorder_offset) {
            key = 1;
        }else{
            key = (int)((floor)((float)(l1.lo_orderkey - lineorder_offset) / (float)lineorder_range)) + 2;
        }

        //insert record into a partition (create a new partition if not exist)
        lineorder_record r = {l1.lo_orderkey, l1.lo_linenumber, MakeTuple::makeLineorderDeltaTuple(l1)} ;
        auto it = (*lineorder_record_map).find(key);
        if(it == (*lineorder_record_map).end()){
            std::set<struct lineorder_record> new_set;
            new_set.insert(r);
            (*lineorder_record_map).insert(std::make_pair(key, new_set));
//            std::cout << "New partition: " << key << " is created;" <<
//                      "Record inserted: " << r.orderkey <<" " << r.linenumber << std::endl;
        }
        else{
            (it->second).insert(r);
//            std::cout << "Find partition: " << key << "; " << "Record inserted: "
//                      << r.orderkey <<" " << r.linenumber << std::endl;
        }

    }

//    avro::DataFileReader <i::customer> customerReader(move(in_customer), customerSchema);
//    i::customer *c1 = new i::customer();
//    while (customerReader.read(*c1)) {
//        //customer
////        std::cout << '(' << c1.c_custkey << "," << c1.c_name << ","  << c1.c_address << ","  << c1.c_city << ","  << c1.c_nation << ","  << c1.c_region << "," <<
////                  c1.c_phone  << "," << c1.c_mktsegment << ","  << c1.c_payment << ","  << c1.type << ","  << c1.timestamp << ')' << std::endl;
//    }

//    avro::DataFileReader <i::supplier> supplierReader(move(in_supplier), supplierSchema);
//    i::supplier *s1 = new i::supplier();
//    while (supplierReader.read(*s1)) {
//        //supplier
////        std::cout << '(' << s1.s_suppkey << "," << s1.s_name << ","  << s1.s_address << ","  << s1.s_city << ","  << s1.s_nation << ","  << s1.s_region << "," <<
////                  s1.s_phone  << ","  << s1.s_ytd << ","  << s1.type << ","  << s1.timestamp << ')' << std::endl;
//    }

    //Assign pointers
    *lineorder_record_ptr = lineorder_record_map;


    jvm->DestroyJavaVM();

}