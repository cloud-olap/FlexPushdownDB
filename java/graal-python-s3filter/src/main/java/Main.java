import org.graalvm.polyglot.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        File file = new File("/home/matt/Work/s3filter/s3filter/benchmark/s3_select/s3_select.py");
        FileReader fr = new FileReader(file);

        Context polyglot = Context.newBuilder().allowAllAccess(true).build();




        Value array = polyglot.eval(Source.newBuilder("python", file).build());




        int result = array.getArrayElement(2).asInt();
        System.out.println(result);
    }


}
