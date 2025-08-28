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
        String dataset = "SIGN";
        String input = "./exp/dataFile/" + dataset + ".txt";
        String output = "./exp/outputFile/" + dataset + "_" + minUtilityRatio +"_hausps_v21.txt";

        // run the algorithm
        HAUSP_PG_Algo hauspMiner = new HAUSP_PG_Algo(input, minUtilityRatio, output);
        hauspMiner.runAlgo();

        // print statistics
        hauspMiner.printStatistics();
    }
}
