import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by sank on 10/6/16.
 */
public class GenerateGraph {
    public static void main(String[] args) throws IOException {
        generateTop5000();
    }

    public static void generateTop5000() throws IOException {
        Map<String, char[]> top500Map = convertFileToMap("/Users/sank/Desktop/CloudComputing/sample5000UserToCommunityMap", 10);
        Map<String, char[]> remainingMap = convertFileToMap("/Users/sank/Desktop/CloudComputing/sampleToPredictUserToCommunityMap", 10);
        String filePath = "/Users/sank/Desktop/CloudComputing/sampleOutputFile";
        writeIntpFile(top500Map, remainingMap, filePath);

    }

    public static Map<String, char[]> convertFileToMap(String filePath, int arrayLength) throws IOException {
        char[] charString = new char[arrayLength];
        for (int i = 0; i < charString.length; i++) {
            charString[i] = '0';
        }
        Map<String, char[]> map = new HashMap<String, char[]>();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line = null;
        while ((line = br.readLine()) != null) {
            char[] temp = new char[arrayLength];
            for (int i = 0; i < temp.length; i++) {
                temp[i] = '0';
            }
            String[] currLine = line.split("\\s+");
            if (map.containsKey(currLine[0])) {
                temp = map.get(currLine[0]);
            }
            for (int i = 1; i < currLine.length; i++) {
                temp[Integer.parseInt(currLine[i]) - 1] = '1';
            }
            map.put(currLine[0], temp);
        }
        br.close();
        return map;
    }

    public static void writeIntpFile(Map<String, char[]> top5000Map, Map<String, char[]> remainingMap, String filePath) throws IOException {

        File file = new File(filePath);
        FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);


        System.out.println("user_id   " + "top5000   " + "remaining");
        bufferedWriter.write("user_id   " + "top5000   " + "remaining");
        Iterator<String> iter = remainingMap.keySet().iterator();
        while (iter.hasNext()) {
            String userId = iter.next();
            char[] rem = remainingMap.get(userId);
            char[] charString = new char[10];
            for (int i = 0; i < charString.length; i++) {
                charString[i] = '0';
            }

            if (top5000Map.keySet().contains(userId)) {
                charString = top5000Map.get(userId);
            }

            System.out.println(userId + "   " + Arrays.toString(charString) + "   " + Arrays.toString(rem));
            bufferedWriter.write(userId + "   " + Arrays.toString(charString) + "   " + Arrays.toString(rem));
            top5000Map.remove(userId);
        }

        if (top5000Map.keySet().size() != 0) {
            iter = top5000Map.keySet().iterator();
            while (iter.hasNext()) {
                String userId = iter.next();
                char[] charString = top5000Map.get(userId);
                char[] rem = new char[10];
                for (int i = 0; i < rem.length; i++) {
                    rem[i] = '0';
                }
                if (remainingMap.keySet().contains(userId)) {
                    rem = remainingMap.get(userId);
                }
                System.out.println(userId + "   " + Arrays.toString(charString) + "   " + Arrays.toString(rem));
                bufferedWriter.write(userId + "   " + Arrays.toString(charString) + "   " + Arrays.toString(rem));
            }
        }
        bufferedWriter.close();
    }
}
