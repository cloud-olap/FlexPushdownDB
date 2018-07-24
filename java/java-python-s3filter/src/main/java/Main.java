import org.python.core.Py;
import org.python.core.PyString;
import org.python.core.PySystemState;


import javax.script.*;
import java.io.*;
import java.util.List;

public class Main {

    public static void main(String[] args) throws ScriptException, FileNotFoundException {

        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> engines = scriptEngineManager.getEngineFactories();
        for (ScriptEngineFactory engine : engines) {
            System.out.println("Engine name: " + engine.getEngineName());
            System.out.println("\tVersion: " + engine.getEngineVersion());
            System.out.println("\tLanguage: " + engine.getLanguageName());
            List<String> extensions = engine.getExtensions();
            if (extensions.size() > 0) {
                System.out.println("\tEngine supports the following extensions:");
                for (String e : extensions) {
                    System.out.println("\t\t" + e);
                }
            }
            List<String> shortNames = engine.getNames();
            if (shortNames.size() > 0) {
                System.out.println("\tEngine has the following short names:");
                for (String n : engine.getNames()) {
                    System.out.println("\t\t" + n);
                }
            }
            System.out.println("=========================");
        }


        FileReader fr = new FileReader(new File("/home/matt/Work/s3filter/s3filter/benchmark/tpch/tpch_q14_filtered_join.py"));

//        StringWriter sw = new StringWriter();

        ScriptEngine pythonEngine = scriptEngineManager.getEngineByName("python");

        CompiledScript x = ((Compilable) pythonEngine).compile(fr);

        PySystemState sys = Py.getSystemState();
        sys.path.append(new PyString("/usr/local/lib/python2.7/dist-packages"));
        sys.path.append(new PyString("/home/matt/Work/s3filter"));

        long start = System.currentTimeMillis();

        x.eval();

        long finish = System.currentTimeMillis();

        System.out.println(finish - start);

        start = System.currentTimeMillis();
        x.eval();
        finish = System.currentTimeMillis();
        System.out.println(finish - start);
//        System.out.println(sw);

//        start = System.currentTimeMillis();
//        x.eval();
//        finish = System.currentTimeMillis();
//        System.out.println(finish - start);
//        start = System.currentTimeMillis();
//        x.eval();
//        finish = System.currentTimeMillis();
//        System.out.println(finish - start);

        System.out.println("=========================");

    }
}
