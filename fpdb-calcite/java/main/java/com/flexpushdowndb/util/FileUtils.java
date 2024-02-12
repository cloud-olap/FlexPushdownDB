package com.flexpushdowndb.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class FileUtils {
  public static List<String> readFileByLine(Path filePath) throws Exception {
    File file = filePath.toFile();
    BufferedReader br = new BufferedReader(new FileReader(file));
    List<String> lines = new ArrayList<>();
    String line;
    while ((line = br.readLine()) != null) {
      lines.add(line);
    }
    return lines;
  }

  public static String readFile(Path filePath) throws Exception {
    return new String(Files.readAllBytes(filePath));
  }
}
