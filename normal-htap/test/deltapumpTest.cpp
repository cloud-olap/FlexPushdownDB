#include "BinlogParser.h"
#include "makeTuple.h"

using namespace normal::avro_tuple::make;

/*
 * demo for BinlogParer
 */
int main() {
    // clock to measure duration
    std::clock_t start;
    double duration;
    start = std::clock();

    // vector to stored parsed delta
    std::unordered_map<int, std::set<struct lineorder_record>> *lineorder_record_ptr = nullptr;

    const char* path = "./bin.000002"; //binlog file path
    const char* path_range = "./partitions/ranges.csv"; //range file path

    parse(path, path_range, &lineorder_record_ptr);

    for(auto lineorder_pair : (*lineorder_record_ptr)){
        std::cout<<"lineorder_pair table number: "<< lineorder_pair.first <<'\n';
        std::set<struct lineorder_record> lineorder_partition = lineorder_pair.second;
        for(auto lineorder_record : lineorder_partition){
            LineorderDelta_t lineorder_delta = lineorder_record.lineorder_delta;
            std::cout << std::get<0>(lineorder_delta) << ", " << std::get<1>(lineorder_delta) << ", " << std::get<2>(lineorder_delta) << ", " <<
                    std::get<3>(lineorder_delta) << ", " << std::get<4>(lineorder_delta) << ", " << std::get<5>(lineorder_delta) << ", " <<
                            std::get<6>(lineorder_delta) << ", " <<std::get<7>(lineorder_delta) << ", " <<std::get<8>(lineorder_delta) << ", " <<
                                    std::get<9>(lineorder_delta) << ", " <<std::get<10>(lineorder_delta) << ", " <<std::get<11>(lineorder_delta) << ", " <<
                                            std::get<12>(lineorder_delta) << ", " <<std::get<13>(lineorder_delta) << ", " << std::get<14>(lineorder_delta) << ", " <<
                                                    std::get<15>(lineorder_delta) << ", "<< std::get<16>(lineorder_delta) << ", "<< std::get<17>(lineorder_delta) << ", "<<
                                                            std::get<18>(lineorder_delta) << std::endl;
        }

    }

    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    std::cout<<"main end: "<< duration <<'\n';

    return 0;
}
