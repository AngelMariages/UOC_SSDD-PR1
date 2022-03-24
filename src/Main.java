import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<String> test = Arrays.asList("ah", "bh", "ch", "dh");

        TimestampMatrix tm1 = new TimestampMatrix(test);

        TimestampMatrix tm2 = tm1.clone();

        System.out.println(tm2.toString().equals(tm1.toString()));

        System.out.println(tm2);

        tm2.update("ch", new TimestampVector(Arrays.asList("what", "whot")));

        System.out.println(tm2.toString().equals(tm1.toString()));
        System.out.println(tm2);
    }
}
