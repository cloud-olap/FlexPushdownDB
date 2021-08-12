//
// Created by ZhangOscar on 8/10/21.
//

#include "cpx.hh"

using namespace avro;

 int main() {
    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    c::cpx c1;
    c1.re = 1.0;
    c1.im = 2.13;
    avro::encode(*e, c1);

    std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(*out);
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in);

    c::cpx c2;
    avro::decode(*d, c2);
    std::cout << '(' << c2.re << ", " << c2.im << ')' << std::endl;
    return 0;
 }

