package giraph.lri.nguyen_candela;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class getClasses {

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        String file = args[0];
        String output = args[1];
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    file));

            FileWriter myWriter = new FileWriter(output + ".csv");

            String line = reader.readLine();
            String csv_line = "";
            String csv_header = "FILE,ALGO,TAU,SIGMA,BETA,NUMPARTITIONS,Time,Score,Migrations,Total LP,R MaxNorm unbalance (x1000),V MaxNorm unbalance (x1000),#real EC,#real EC (%%),#virtrual EC,Vertex Balance JSD,Sampling Time\n";
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

            String r_MNU = "";
            String v_MNU = "";
            String r_EC = "";
            String r_EC_per = "";
            String v_EC = "";
            String JSD = "";
            String ST = "";


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

                if (line.contains("R MaxNorm unbalance (x1000)")){
                    String[] args_line = line.split("=");
                    r_MNU = args_line[1];
                }

                if (line.contains("V MaxNorm unbalance (x1000)")){
                    String[] args_line = line.split("=");
                    v_MNU = args_line[1];
                }

                if (line.contains("#real EC")){
                    String[] args_line = line.split("=");
                    r_EC = args_line[1];
                }

                if (line.contains("#real EC (%%)")){
                    String[] args_line = line.split("=");
                    r_EC_per = args_line[1];
                }

                if (line.contains("#virtrual EC")){
                    String[] args_line = line.split("=");
                    v_EC = args_line[1];
                }

                if (line.contains("Vertex Balance JSD")){
                    String[] args_line = line.split("=");
                    JSD = args_line[1];
                }

                if (line.contains("Sampling Time")){
                    String[] args_line = line.split("=");
                    ST = args_line[1];
                }


                // read next line
                line = reader.readLine();
            }

            csv_line = GRAPH + "," + SAMPLING_TYPE + "," +  TAU + "," + SIGMA + "," +
                    BETA + "," + numberOfPartitions +"," + Time +
                    "," + Score +"," + Migrations +"," + TotalLP +
                    "," + r_MNU +"," + v_MNU +"," + r_EC +
                    "," + r_EC_per +"," + v_EC +"," + JSD +
                    "," + ST +"\n";

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
