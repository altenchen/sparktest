package enn.enndigit.udfaggregation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @Author:chenchen
 * @Description:
 * @Date:2018/9/3
 * @Project:sparktest
 * @Package:enn.enndigit.udfaggregation
 */
public class GenerateBigData {
    public static void main(String[] args) {
        String path = "/Users/chenchen/Desktop/testdata/generatedData/DataFile.txt";
        File file = new File(path);
        try {
            FileWriter fileWriter = new FileWriter(file);
            Random random = new Random();
            for (int i = 0; i < 100000; i++) {
                fileWriter.write(i + " " + (random.nextInt(100) + 1));
                fileWriter.write(System.getProperty("line.separator"));
            }
            fileWriter.flush();
            fileWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


