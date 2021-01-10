package giraph.lri.hnguyen_jcandela;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class getClasses {

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        String file = args[0];
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    file));

            FileWriter myWriter = new FileWriter("CSV_RESULT.csv");

            String line = reader.readLine();
            String csv_line = "";
            String csv_header = "FILE,ALGO,TAU,SIGMA,BETA,NUMPARTITIONS,Time,Score,Migrations,Total LP\n";
            myWriter.write(csv_header);
            int counter = 0;

            String BETA = "";
            String TAU = "";
            String SIGMA = "";
            String GRAPH = "";
            String SAMPLING_TYPE = "";
            String numberOfPartitions = "";
            String Time = "";
            String Score = "";
            String Migrations = "";
            String TotalLP = "";


            while (line != null) {
                if (line.contains("sudo /usr/local/hadoop/bin/hadoop jar")){

                    if (counter > 0){


                        csv_line = GRAPH + "," + SAMPLING_TYPE + "," +  TAU + "," + SIGMA + "," +
                                BETA + "," + numberOfPartitions +"," + Time +
                                "," + Score +"," + Migrations +"," + TotalLP +"\n";

                        myWriter.write(csv_line);

                        BETA = "";
                        TAU = "";
                        SIGMA = "";
                        GRAPH = "";
                        SAMPLING_TYPE = "";
                        numberOfPartitions = "";
                        Time = "";
                        Score = "";
                        Migrations = "";
                        TotalLP = "";

                    }
                    counter++;





                    String[] args_line = line.split(" -ca ");
                    for (String arg_unit : args_line) {
                        String[] arg_name = arg_unit.split("=");
                        if (arg_name[0].contains("BETA")){
                            BETA = arg_name[1].replace("'","");
                        }
                        if (arg_name[0].contains("TAU")){
                            TAU = arg_name[1].replace("'","");
                        }

                        if (arg_name[0].contains("SIGMA")){
                            SIGMA = arg_name[1].replace("'","");
                        }

                        if (arg_name[0].contains("GRAPH")){
                            GRAPH = arg_name[1].replace("'","");
                        }

                        if (arg_name[0].contains("SAMPLING_TYPE")){
                            SAMPLING_TYPE = arg_name[1].replace("'","");
                        }

                        if (arg_name[0].contains("numberOfPartitions")){
                            numberOfPartitions = arg_name[1].replace("'","");
                        }


                    }
                    System.out.println(line);


                }

                if (line.contains("Score (x1000)")){
                    String[] args_line = line.split("=");
                    Score = args_line[1];
                }

                if (line.contains("Migrations")){
                    String[] args_line = line.split("=");
                    Migrations = args_line[1];
                }

                if (line.contains("Total LP (ms)")){
                    String[] args_line = line.split("=");
                    TotalLP = args_line[1];
                }

                if (line.contains("Total (ms)")){
                    String[] args_line = line.split("=");
                    Time = args_line[1];
                }

                // read next line
                line = reader.readLine();
            }

            csv_line = GRAPH + "," + SAMPLING_TYPE + "," +  TAU + "," + SIGMA + "," +
                    BETA + "," + numberOfPartitions +"," + Time +
                    "," + Score +"," + Migrations +"," + TotalLP +"\n";

            myWriter.write(csv_line);

            reader.close();
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(file);


        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(elapsedTime/1000);
    }

}
