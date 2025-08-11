package src.HAUSP_PG;

import java.io.IOException;

/**
 * This is an implementation of the HAUSP-PG algorithm as presented in this paper: <br/><br/>
 * 
 *    "HAUSP-PG"
 *
 * 
 * @author Kai Cao & Yucong Duan, HNU & Wensheng Gan, HITsz, China.
 * 
 */
public class HAUSP_PG_Test {
    public static void main(String[] args) throws IOException {
        double minUtilityRatio = 0.012;
//        int nbtopk = 9;
        String dataset = "SIGN";
        String input = "./exp/dataFile/" + dataset + ".txt";
//        String output = "hauspminer.txt";
        String output = "./exp/outputFile/" + dataset + "_" + minUtilityRatio +"_hausps.txt";

        // run the algorithm
        HAUSP_PG_Algo hauspMiner = new HAUSP_PG_Algo(input, minUtilityRatio, output);
//        HAUSP_PG_Algo hauspMiner = new HAUSP_PG_Algo(input, nbtopk, output);
        hauspMiner.runAlgo();

        // print statistics
        hauspMiner.printStatistics();
    }
}
