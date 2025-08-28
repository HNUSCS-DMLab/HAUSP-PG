package  src.HAUSP_PG;

import java.io.*;
import java.util.*;

//import HAUSP_PG.HAUSP_PG_Algo.LastId;


public class HAUSP_PG_Algo {
    // % of threshold * databaseUtility
    protected double threshold;
    // Average Utility
    protected int minAU;
    
    // test dataset
    protected String pathname;

    protected long hauspNum;
    protected long candidateNum;
//    protected long deletedTimes;

    protected boolean isDebug;
    protected ArrayList<String> patterns;
    protected boolean isWriteToFile;
    protected String output;

    protected long currentTime;

    protected boolean[] isRemove;
    boolean DBUpdated;
    protected boolean[] isRemoveUnrise;
    protected int testCount1 = 0;

    protected BufferedWriter writer;
//    ArrayList<ULinkList> uLinkListDB = new ArrayList<ULinkList>();
    ULinkList[] uLinkListDB;
    ArrayList<Integer> prefix;
    protected Boolean firstPEU;

    /**
     * Set the WSU and LastID
     *
     * @author wsgan
     */

    protected class LastId {
        public int swu;
        public ULinkList uLinkList;
        public int itemUtil;
        public int localLen;

        public LastId(int swu, ULinkList uLinkList, int itemUtil, int localLen) {
            this.swu = swu;
            this.uLinkList = uLinkList;
            this.itemUtil = itemUtil;
            this.localLen = localLen;
        }
    }
    

    /**
     * HUSP-Miner
     *
     * @param pathname
     * @param threshold
     * @param output
     */
    public HAUSP_PG_Algo(String pathname, double threshold, String output) {
        this.pathname = pathname;
        this.threshold = threshold;

        hauspNum = 0;
        candidateNum = 0;

        isDebug = false;
        isWriteToFile = true;
        this.output = output;
    }

    /**
     * Run HUSP-Miner
     */
    public void runAlgo() throws IOException {
        if (isWriteToFile) patterns = new ArrayList<String>();
        currentTime = System.currentTimeMillis();

        // reset maximum memory
        MemoryLogger.getInstance().reset();
//        HashMap<Integer, Integer> mapItemSWU = new HashMap<>();
//        getDBFromTxt(pathname, mapItemSWU);
//        long l = System.currentTimeMillis();
        getDBFromTxt_Reduce(pathname);
//        System.out.println(System.currentTimeMillis() - l);
        MemoryLogger.getInstance().checkMemory();
        // call the mining function
        firstUSpan();

        MemoryLogger.getInstance().checkMemory();
        //System.out.println("Max memory: " + MemoryLogger.getInstance().getMaxMemory() + " MB");
        if (isWriteToFile) writeToFile();
    }

    void getDBFromTxt_Reduce(String fileName) throws IOException {
        Map<Integer, Integer> mapItemToSWU = new HashMap<>();
        ArrayList<UItem[]> rawDB = new ArrayList<>();

        Long totalUtil = 0L;
        BufferedReader myInput = null;
        String thisLine;
        try {
            // prepare the object for reading the file
            myInput = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileName))));
            // for each line (transaction) until the end of file
            while ((thisLine = myInput.readLine()) != null) {
                // if the line is a comment, is  empty or is a kind of metadata, skip it
                if (thisLine.isEmpty() == true || thisLine.charAt(0) == '#' || thisLine.charAt(0) == '%' || thisLine.charAt(0) == '@') {
                    continue;
                }

                HashSet<Integer> consideredItems = new HashSet<>();
                // split the transaction according to the " " separator
                String tokens[] = thisLine.split(" ");
                int seqLen;
                if (tokens[tokens.length - 3].equals("-2")) {
                    seqLen = tokens.length - 4;
                }
                else seqLen = tokens.length - 3;
                UItem[] uItems = new UItem[seqLen];

                String sequenceUtilityString = tokens[tokens.length - 1];
                int positionColons = sequenceUtilityString.indexOf(':');
                int sequenceUtility = Integer.parseInt(sequenceUtilityString.substring(positionColons + 1));
                totalUtil += sequenceUtility;

                // Then read each token from this sequence (except the last three tokens
                // which are -1 -2 and the sequence utility)
                for (int i = 0; i <= seqLen - 1; i++) {
                    String currentToken = tokens[i];
                    // if the current token is not -1
                    if (currentToken.length() == 0)
                        continue;
                    if (currentToken.charAt(0) != '-') {
                        // find the left brack
                        int positionLeftBracketString = currentToken.indexOf('[');
                        // get the item
                        String itemString = currentToken.substring(0, positionLeftBracketString);
                        int item = Integer.parseInt(itemString);

                        String itemUtility = currentToken.substring(positionLeftBracketString + 1, currentToken.length() - 1);
                        int utility = Integer.parseInt(itemUtility);

                        uItems[i] = new UItem(item, utility);

                        if (!consideredItems.contains(item)) {
                            consideredItems.add(item);
                            Integer swu = mapItemToSWU.get(item);

                            // add the utility of sequence utility to the swu of this item
                            swu = (swu == null) ? sequenceUtility : swu + sequenceUtility;
                            mapItemToSWU.put(item, swu);
                        }
                    } else {
                        uItems[i] = new UItem(-1, -1);
                    }
                }
                rawDB.add(uItems);
            }
        } catch (Exception e) {
            // catches exception if error while reading the input file
            e.printStackTrace();
        } finally {
            if (myInput != null) {
                myInput.close();
            }
        }

    	minAU = (int) (totalUtil * threshold);    	
		System.out.println("minAU:" + minAU);
		
        MemoryLogger.getInstance().checkMemory();


        
        
        /***********trans rawDB to designed seq data structure**********/
        ArrayList<ULinkList> uLinkListDBs = new ArrayList<ULinkList>();
        int maxItemName = 0;
        int maxSequenceLength = 0;
        Iterator<UItem[]> listIterator = rawDB.iterator();
        while (listIterator.hasNext()) {
            UItem[] uItems = listIterator.next();
            ArrayList<UItem> newItems = new ArrayList<>();
            int seqIndex = 0;
            HashMap<Integer, ArrayList<Integer>> tempHeader = new HashMap<>();
            BitSet tempItemSetIndices = new BitSet(uItems.length);
            for (UItem uItem : uItems) {
                if(uItem == null)
                    continue;
                int item = uItem.itemName();
                if (item != -1) {
                    if (mapItemToSWU.get(item) >= minAU) {

                        newItems.add(uItem);
                        if (item > maxItemName)
                            maxItemName = item;
                        if (tempHeader.containsKey(item))
                            tempHeader.get(item).add(seqIndex++);
                        else {
                            ArrayList<Integer> list = new ArrayList<>();
                            list.add(seqIndex++);
                            tempHeader.put(item, list);
                        }
                    }
                }
                else
                    tempItemSetIndices.set(seqIndex);//从0开始记录
            }
            int size = newItems.size();
            if (size > 0) {
                if (size > maxSequenceLength)
                    maxSequenceLength = size;
                ULinkList uLinkList = new ULinkList();
                uLinkList.seq = newItems.toArray(new UItem[size]);
                uLinkList.remainingUtility = new int[size];
                uLinkList.itemSetIndex = tempItemSetIndices;
//                uLinkList.remainingLen = new int[size];


//                uLinkList.headTable = new itemAndIndices[tempHeader.size()];
                uLinkList.header = new int[tempHeader.size()];
                uLinkList.headerIndices = new Integer[tempHeader.size()][];
                int hIndex = 0;
                for (Map.Entry<Integer, ArrayList<Integer>> entry : tempHeader.entrySet()) {
                    uLinkList.header[hIndex++] = entry.getKey();
//                    uLinkList.headTable.put(entry.getKey(), entry.getValue().toArray(new Integer[entry.getValue().size()]));
                }
                Arrays.sort(uLinkList.header);
                for (int i = 0; i < uLinkList.header.length; i++) {
                    int cItem = uLinkList.header[i];
                    ArrayList<Integer> indices = tempHeader.get(cItem);
                    uLinkList.headerIndices[i] = indices.toArray(new Integer[indices.size()]);
                }

                uLinkListDBs.add(uLinkList);
            }
//            listIterator.remove();
        }
        prefix = new ArrayList<Integer>(maxSequenceLength);
        //Kevin added:
        isRemoveUnrise = new boolean[maxItemName + 1];
        
        isRemove = new boolean[maxItemName + 1];
        uLinkListDB =  uLinkListDBs.toArray(new ULinkList[uLinkListDBs.size()]);
        MemoryLogger.getInstance().checkMemory();
    }

    /**
     * Write to file
     */
    /**
     * Write to file
     */
    protected void writeToFile() {
        Collections.sort(patterns);
        //WriteFile.WriteFileByCharBuffer(this.output, patterns);
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
            for (String pattern : patterns) {
                writer.write(pattern);
                writer.newLine();
            }
            
            StringBuilder stats = new StringBuilder();
            stats.append("=============  HUSPull_PLUS ALGORITHM - STATS ============\n");
            //stats.append(" Total utility of DB: " + totalUtil + " \n");
    		stats.append(" threshold: " + String.format("%.5f", threshold) + " \n");
    		stats.append("minAU: " + minAU + " \n");
    		stats.append("time: " + (System.currentTimeMillis() - currentTime)/1000.0 + " s" + " \n");
    		stats.append("Max memory: " + MemoryLogger.getInstance().getMaxMemory() + "  MB" + " \n");
    		stats.append("HAUSPs: " + hauspNum + " \n");
    		stats.append("Candidates: " + candidateNum);
            writer.write(stats.toString());
            writer.newLine();
            // close output file
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * First USpan
     */
     void firstUSpan() throws IOException {
        // remove the invalid item
        firstRemoveItem();
        HashMap<Integer, Integer> mapItemSwu1 = getMapItemSwu();
//        System.out.println(System.currentTimeMillis() - currentTime);
        MemoryLogger.getInstance().checkMemory();
//        System.out.println(MemoryLogger.getInstance().getMaxMemory());
        for (Map.Entry<Integer, Integer> entry : mapItemSwu1.entrySet()) {
            if (entry.getValue() < minAU) isRemove[entry.getKey()] = true;
        }
//        firstRemoveItem();
        mapItemSwu1.clear();
        // call the function of firstConcatenation
        firstConcatenation();

        // check the memory usage
        MemoryLogger.getInstance().checkMemory();
    }

    protected HashMap<Integer, Integer> getMapItemSwu() {
        HashMap<Integer, Integer> mapItemSwu = new HashMap<Integer, Integer>();
        for (ULinkList uLinkList : uLinkListDB) {
            int seqUtility = uLinkList.utility(0) + uLinkList.remainUtility(0);
            for (int item : uLinkList.header) {
                int twu = mapItemSwu.getOrDefault(item, 0);
                mapItemSwu.put(item, seqUtility + twu);
            }
        }

        return mapItemSwu;
    }

    protected HashMap<Integer, Integer> getMapItemTRSU() {
        HashMap<Integer, Integer> mapItemSwu = new HashMap<Integer, Integer>();
        for (ULinkList uLinkList : uLinkListDB) {
            for (int item : uLinkList.header) {
                Integer itemIndex = uLinkList.getItemIndices(item)[0];
                int trsu = uLinkList.utility(itemIndex) + uLinkList.remainUtility(itemIndex);
                int twu = mapItemSwu.getOrDefault(item, 0);
                mapItemSwu.put(item, trsu + twu);
            }
        }

        return mapItemSwu;
    }
/**
    // 此方法用于获取不同header对应的最大item utility
    protected HashMap<Integer, Integer> getMaxItemUtilityPerHeader() {
        // 创建一个HashMap用于存储每个header对应的最大item utility
        HashMap<Integer, Integer> mapHeaderToMaxUtility = new HashMap<>();
        // 遍历ULinkListDB中的每个ULinkList
        for (ULinkList uLinkList : uLinkListDB) {
            // 遍历ULinkList的header数组
            for (int i = 0; i < uLinkList.header.length; i++) {
                int headerItem = uLinkList.header[i];
                // 获取当前header对应的所有索引
                Integer[] indices = uLinkList.headerIndices[i];
                int maxUtility = 0;
                // 遍历这些索引，找出最大的item utility
                for (int index : indices) {
                    int utility = uLinkList.utility(index);
                    maxUtility = Math.max(maxUtility, utility);
                }
                // 如果map中已经有该header的记录，更新最大utility
                if (mapHeaderToMaxUtility.containsKey(headerItem)) {
                    int currentMax = mapHeaderToMaxUtility.get(headerItem);
                    mapHeaderToMaxUtility.put(headerItem, Math.max(currentMax, maxUtility));
                } else {
                    // 若没有记录，直接存入最大utility
                    mapHeaderToMaxUtility.put(headerItem, maxUtility);
                }
            }
        }
        return mapHeaderToMaxUtility;
    }
*/
    /**
     * First concatenation
     *
     */
     void firstConcatenation() throws IOException {
         HashMap<Integer, Integer> mapItemTRSU = getMapItemTRSU();
         for (Map.Entry<Integer, Integer> entry : mapItemTRSU.entrySet()) {
             if (entry.getValue() >= minAU) {

                candidateNum += 1;
                int addItem = entry.getKey();
                int sumUtility = 0;
                int upperBound = 0;

                int newprefixLength = 0;
                
                ArrayList<ProjectULinkList> newProjectULinkListDB = new ArrayList<ProjectULinkList>();

                for (ULinkList uLinkList : uLinkListDB) {
                    Integer[] itemIndices = uLinkList.getItemIndices(addItem);
                    if (itemIndices == null)
                        continue;
                    // addItem should be in the transaction
                    int utilityInTXN = 0;
                    int ubInTXN = 0;
                    ArrayList<UPosition> newUPositions = new ArrayList<UPosition>();

                    for (int index : itemIndices) {
                        int curUtility = uLinkList.utility(index);
                        utilityInTXN = Math.max(utilityInTXN, curUtility);
                        ubInTXN = Math.max(ubInTXN, getUpperBound(uLinkList, index, curUtility));
                        newUPositions.add(new UPosition(index, curUtility));
                    }

                    // update the sumUtility and upper-bound
                    if (newUPositions.size() > 0) {
                        newprefixLength = 1;//is 1, is not 2
                        newProjectULinkListDB.add(
                                new ProjectULinkList(uLinkList, newUPositions, utilityInTXN, newprefixLength));

                        sumUtility += utilityInTXN;
                        upperBound += ubInTXN;

                    }

                }

                if (sumUtility >= minAU * newprefixLength) {
                    hauspNum += 1;
                    //prefix.add(addItem);
                    //recordPattern(prefix, sumUtility); // 新增：记录模式
                    //prefix.remove(prefix.size() - 1);
                }

                if (upperBound >= minAU * (newprefixLength + 1)) {
                    prefix.add(addItem);
                    // call the function
                    runHUSPspan(newProjectULinkListDB);
                    prefix.remove(prefix.size() - 1);
                }
            }
        }
    }

    /**
     * Run USpan algorithm
     *
     * @param projectULinkListDB
     */
    protected void runHUSPspan(ArrayList<ProjectULinkList> projectULinkListDB) throws IOException {
        HashMap<Integer, LastId> mapItemExtensionUtility = getMapItemExtensionUtility(projectULinkListDB);

        testCount1++;
        // remove the item has low RSU
        for (Map.Entry<Integer, LastId> entry : mapItemExtensionUtility.entrySet()) {
            int item = entry.getKey();
            int swu = entry.getValue().swu;
            if (swu < minAU * entry.getValue().localLen) {
                isRemove[item] = true;
                DBUpdated = true;
            }

            int itemUtil = entry.getValue().itemUtil;
            if (itemUtil < minAU) {
            	isRemoveUnrise[item] = true;
            	DBUpdated = true;
            }
        }
        

//        removeItem(projectULinkListDB);
        removeUnriseItem(projectULinkListDB);


        // call the iConcatenation function
        HashMap<Integer, LastId> mapItemIConcatenationSwu = getMapItemIConcatenationSwu(projectULinkListDB);
        iConcatenation(projectULinkListDB, mapItemIConcatenationSwu);

        // check the memory usage
        MemoryLogger.getInstance().checkMemory();

        // call the sConcatenation function
        HashMap<Integer, LastId> mapItemSConcatenationSwu = getMapItemSConcatenationSwu(projectULinkListDB);
        sConcatenation(projectULinkListDB, mapItemSConcatenationSwu);


        // check the memory usage
        MemoryLogger.getInstance().checkMemory();

        for (Map.Entry<Integer, LastId> entry : mapItemExtensionUtility.entrySet()) {
            int item = entry.getKey();
            int swu = entry.getValue().swu;
            if (swu < minAU * entry.getValue().localLen) {
                isRemove[item] = false;
                DBUpdated = true;
            }
            
            int itemUtil = entry.getValue().itemUtil;
            if (itemUtil < minAU) {
            	isRemoveUnrise[item] = false;
            	DBUpdated = true;
            }
        }

//        removeItem(projectULinkListDB);
        removeUnriseItem(projectULinkListDB);

    }

    /**
     * items appear after prefix in the same itemset in difference sequences;
     * SWU = the sum these sequence utilities for each item as their upper bounds under prefix
     * should not add sequence utility of same sequence more than once
     *
     * @param projectedDB: database
     * @return upper-bound
     */
    protected HashMap<Integer, LastId> getMapItemIConcatenationSwu(ArrayList<ProjectULinkList> projectedDB) {
        //Kevin added:
    	HashMap<Integer, LastId> mapItemIConcatenationSwu = new HashMap<Integer, LastId>();
        HashMap<Integer, Integer> currentitemUtil = new HashMap<>(); // 当前uLinkList中各item的最大utility
        HashMap<Integer, Integer> maxRsIU = new HashMap<>();
        ////        HashMap<Integer, LastId> mapItemIConcatenationSwu0 = new HashMap<Integer, LastId>();
        for (ProjectULinkList projectULinkList : projectedDB) {
            ULinkList uLinkList = projectULinkList.getULinkList();
            ArrayList<UPosition> uPositions = projectULinkList.getUPositions();
            int localSwu = getItemUpperBound(projectULinkList);
            int prefixLen = projectULinkList.prefixLength();
            currentitemUtil.clear();

            for (UPosition uPosition : uPositions) {
                int nextItemsetPos = uLinkList.nextItemsetPos(uPosition.index());
                if (nextItemsetPos == -1)
                    nextItemsetPos = uLinkList.length();
                for (int index = uPosition.index() + 1; index < nextItemsetPos; ++index) {
                	
                    int item = uLinkList.itemName(index);

                    // 场景2：index < nextItemsetPos（执行原有处理逻辑）
                    int localitemUtil = uLinkList.utility(index);

                    if (!isRemove[item]) {
                        // only find items in the same itemset, else break
                         LastId lastId = mapItemIConcatenationSwu.get(item);
                        if (lastId == null) {
                        	maxRsIU.clear();
                        	for (int i = uLinkList.length() - 1; i >= index; --i) {
                        	    int rsitem = uLinkList.itemName(i);
                        	    if (!isRemoveUnrise[rsitem]) {
                        	        maxRsIU.merge(rsitem, uLinkList.utility(i), Math::max);
                        	    }
                        	}
                        	int localremainLen = 0;
                        	for (int tempRsIU : maxRsIU.values()) {
                        	    if (tempRsIU >= minAU) localremainLen++;
                        	}

                        	int trsu = get_TRSU(projectULinkList, index);
                            mapItemIConcatenationSwu.put(item, new LastId(localSwu - trsu, uLinkList, localitemUtil, localremainLen + prefixLen));
                            //标记前一个状态
                            currentitemUtil.put(item, localitemUtil);

                        } else {
                            // should not add sequence utility of same sequence more than once
                            // since many UPosition may have same item, [a b] [a b]
                            if (lastId.uLinkList != uLinkList) {
                            	maxRsIU.clear();
                            	for (int i = uLinkList.length() - 1; i >= index; --i) {
                            	    int rsitem = uLinkList.itemName(i);
                            	    if (!isRemoveUnrise[rsitem]) {
                            	        maxRsIU.merge(rsitem, uLinkList.utility(i), Math::max);
                            	    }
                            	}
                            	int localremainLen = 0;
                            	for (int tempRsIU : maxRsIU.values()) {
                            	    if (tempRsIU >= minAU) localremainLen++;
                            	}
                            	lastId.localLen = Math.min(localremainLen + prefixLen, lastId.localLen);

                            	int trsu = get_TRSU(projectULinkList, index);
                                lastId.swu += localSwu - trsu;

                                lastId.itemUtil = maxRsIU.getOrDefault(item, 0);
                                lastId.localLen = localremainLen + prefixLen;
                          	
                                lastId.uLinkList = uLinkList;//记录uLL仅为了便于在这部分的判断
                                //标记前一个状态
                                currentitemUtil.put(item, localitemUtil);
                            }else {
                            }
                        }
                    }

                }
            }
        }

        return mapItemIConcatenationSwu;
    }

    //cansel trsu
    private int get_TRSU(ProjectULinkList projectULinkList, int index) {
        if (!firstPEU) {
            return 0;
        }
        ArrayList<UPosition> uPositions = projectULinkList.getUPositions();
        ULinkList uLinkList = projectULinkList.getULinkList();
        int trsu = 0;
        int pIndex = 0;
        for (UPosition position : uPositions) {
            if (position.index() < index) {
                pIndex = position.index();
            } else {
                break;
            }
        }
        trsu = uLinkList.remainUtility(pIndex) - uLinkList.remainUtility(index - 1);
        return trsu >= 0 ? trsu : 0;
    }
    
    
    /**
     * items appear from the next itemset after prefix in difference sequences;
     * SWU = sum these sequence utilities for each item as their upper bounds under prefix
     * should not add sequence utility of same sequence more than once
     *
     * @param projectedDB: database
     * @return upper-bound
     */
    protected HashMap<Integer, LastId> getMapItemSConcatenationSwu(ArrayList<ProjectULinkList> projectedDB) {
        HashMap<Integer, LastId> mapItemSConcatenationSwu = new HashMap<Integer, LastId>();
        HashMap<Integer, Integer> currentitemUtil = new HashMap<>(); // 当前uLinkList中各item的最大utility
        HashMap<Integer, Integer> maxRsIU = new HashMap<>();
        for (ProjectULinkList projectULinkList : projectedDB) {
            ULinkList uLinkList = projectULinkList.getULinkList();
            ArrayList<UPosition> uPositions = projectULinkList.getUPositions();
            int localSwu = getItemUpperBound(projectULinkList);
            // ....
            int addItemPos = uLinkList.nextItemsetPos(uPositions.get(0).index());
            int prefixLen = projectULinkList.prefixLength();
            currentitemUtil.clear();

            // two methods to calc swu of s-concatenation
            // first one is to traverse the last positions of items in header,
            // which will not repeat adding swu of item in the same transaction.
//            if (addItemPos != -1 && uLinkList.length() - addItemPos + 1 > uLinkList.headerLength()) {
//                for (int i = 0; i < uLinkList.headerLength(); ++i) {
//                    if (uLinkList.lastPosOfItemByInd(i) >= addItemPos) {
//                        LastId lastId = mapItemSConcatenationSwu.get(uLinkList.header(i));
//                        if (lastId == null) {
//                            mapItemSConcatenationSwu.put(uLinkList.header(i), new LastId(localSwu, uLinkList));
//                        } else {
//                        	// update the WSU of lastID
//                            lastId.swu += localSwu;
//                        }
//                    }
//                }
//            } else {
            // the second one is to traverse from the position of next itemset of addItem to
            // the end of transaction, which may repeat adding swu of item in the same transaction.
            for (int index = addItemPos; index < uLinkList.length() && index != -1; ++index) {

                int item = uLinkList.itemName(index);

                int localitemUtil = uLinkList.utility(index);

                if (!isRemove[item]) {

                    LastId lastId = mapItemSConcatenationSwu.get(item);
                    if (lastId == null) {
                    	maxRsIU.clear();
                    	for (int i = uLinkList.length() - 1; i >= index; --i) {
                    	    int rsitem = uLinkList.itemName(i);
                    	    if (!isRemoveUnrise[rsitem]) {
                    	        maxRsIU.merge(rsitem, uLinkList.utility(i), Math::max);
                    	    }
                    	}
                    	int localremainLen = 0;
                    	for (int tempRsIU : maxRsIU.values()) {
                    	    if (tempRsIU >= minAU) localremainLen++;
                    	}
                    	
                        int trsu = get_TRSU(projectULinkList, index);
                        mapItemSConcatenationSwu.put(item, new LastId(localSwu - trsu, uLinkList, localitemUtil, localremainLen + prefixLen));
                        currentitemUtil.put(item, localitemUtil);

                    } else {
                        // should not add sequence utility of same sequence more than once
                        if (lastId.uLinkList != uLinkList) {
                        	maxRsIU.clear();
                        	for (int i = uLinkList.length() - 1; i >= index; --i) {
                        	    int rsitem = uLinkList.itemName(i);
                        	    if (!isRemoveUnrise[rsitem]) {
                        	        maxRsIU.merge(rsitem, uLinkList.utility(i), Math::max);
                        	    }
                        	}
                        	int localremainLen = 0;
                        	for (int tempRsIU : maxRsIU.values()) {
                        	    if (tempRsIU >= minAU) localremainLen++;
                        	}
                        	lastId.localLen = Math.min(localremainLen + prefixLen, lastId.localLen);

                        	int trsu = get_TRSU(projectULinkList, index);
                            lastId.swu += localSwu - trsu;

                            lastId.itemUtil = maxRsIU.getOrDefault(item, 0);
                        	lastId.localLen = localremainLen + prefixLen;
                        	
                            lastId.uLinkList = uLinkList;
                            currentitemUtil.put(item, localitemUtil);
                        }
                    }
                }
            }
        }
        return mapItemSConcatenationSwu;
    }

    /**
     * I-concatenation
     * <p>
     * current item should be larger than last item in prefix
     * avoiding repetition: e.g. <[a a]>, <[a b]> and <[b a]>
     * candidate sequences are evaluated by (prefix utility + remaining utility) (PU)
     *
     * @param projectedDB:              database
     * @param mapItemIConcatenationSwu: upper-bound of addItem
     */
    protected void iConcatenation(ArrayList<ProjectULinkList> projectedDB,
                                  HashMap<Integer, LastId> mapItemIConcatenationSwu) throws IOException {
   
        for (Map.Entry<Integer, LastId> entry : mapItemIConcatenationSwu.entrySet()) {

        	if (entry.getValue().swu >= (minAU * entry.getValue().localLen)) {

                candidateNum += 1;
                int addItem = entry.getKey();
                int sumUtility = 0;
                int upperBound = 0;
                int newprefixLength = 0;

                int newConLength = 0;
//                if (!isRemoveUnrise[addItem]) {
                if (entry.getValue().itemUtil >= minAU) {
                	newConLength = entry.getValue().localLen;
                } else {
                	newConLength = entry.getValue().localLen + 1;
                }

                ArrayList<ProjectULinkList> newProjectULinkListDB = new ArrayList<ProjectULinkList>();

                for (ProjectULinkList projectULinkList : projectedDB) {
                    ULinkList uLinkList = projectULinkList.getULinkList();
                    ArrayList<UPosition> uPositions = projectULinkList.getUPositions();

                    Integer[] itemIndices = uLinkList.getItemIndices(addItem);

                    /*
                     * 1. addItem should in the header of transaction
                     * 2. addItem should be larger than UPosition (last item in prefix), avoiding
                     * repetition.  e.g. <[a a]>, <[a b]> and <[b a]>
                     */
                    if (itemIndices == null)
                        continue;
                    int utilityInTXN = 0;
                    int ubInTXN = 0;
                    ArrayList<UPosition> newUPositions = new ArrayList<UPosition>();

                    //Kevin added:
                    //int maxUtilityAddItemInd = -1; // 记录utilityInTXN最大时的addItemInd
                    //int maxUbAddItemInd = -1;      // 记录ubInTXN最大时的addItemInd

                    /*
                     * i: index of UPosition (prefix), UPosition contains position and prefix
                     * utility.
                     * addItemInd: position of addItem in transaction, can get item utility.
                     *
                     * addItem should in the same itemset with UPosition (last item of prefix),
                     * which indicates that prefixItemsetIndex == addItemItemsetIndex
                     */
                    int addItemInd;
                    for (int i = 0, j = 0; i < uPositions.size() && j < itemIndices.length; ) {
                        addItemInd = itemIndices[j];
                        UPosition uPosition = uPositions.get(i);
                        int uPositionItemsetIndex = uLinkList.whichItemset(uPosition.index());
                        int addItemItemsetIndex = uLinkList.whichItemset(addItemInd);

                        if (uPositionItemsetIndex == addItemItemsetIndex) {
                            int curUtility = uLinkList.utility(addItemInd) + uPosition.utility();
                            utilityInTXN = Math.max(utilityInTXN, curUtility);
                            ubInTXN = Math.max(ubInTXN, getUpperBound(uLinkList, addItemInd, curUtility));

                            newUPositions.add(new UPosition(addItemInd, curUtility));


                            i++;
                            j++;
                        } else if (uPositionItemsetIndex > addItemItemsetIndex) {
                            j++;
                        } else if (uPositionItemsetIndex < addItemItemsetIndex) {
                            i++;
                        }
                    }

                    // if exist new positions, update the sumUtility and upper-bound
                    if (newUPositions.size() > 0) {
//                        newProjectULinkListDB.add(new ProjectULinkList(uLinkList, newUPositions, utilityInTXN));
                        newprefixLength = projectULinkList.prefixLength() + 1;
                        newProjectULinkListDB.add(new ProjectULinkList(uLinkList, newUPositions, utilityInTXN, newprefixLength));

                        sumUtility += utilityInTXN;
                        upperBound += ubInTXN;
                    }
                }
                
                if (sumUtility >= minAU * newprefixLength) {
                    hauspNum += 1;
                    //prefix.add(addItem);
                    //recordPattern(prefix, sumUtility); // 新增：记录模式
                    //prefix.remove(prefix.size() - 1);

                }

                if (upperBound >= minAU * newConLength) {
                    prefix.add(addItem);
                    // call the function
                    runHUSPspan(newProjectULinkListDB);
                    prefix.remove(prefix.size() - 1);
                }
            }
        }
    }

    /**
     * S-concatenation
     * <p>
     * each addItem (candidate item) has multiple index in the sequence
     * each index can be s-concatenation with multiple UPositions before this index
     * but these UPositions s-concatenation with the same index are regarded as one sequence
     * so for each index, choose the UPosition with maximal utility
     * <p>
     * candidate sequences are evaluated by (prefix utility + remaining utility) (PU)
     *
     * @param projectedDB:              database
     * @param mapItemSConcatenationSwu: upper-bound of addItem
     */
    protected void sConcatenation(ArrayList<ProjectULinkList> projectedDB,
                                  HashMap<Integer, LastId> mapItemSConcatenationSwu) throws IOException {
       
        for (Map.Entry<Integer, LastId> entry : mapItemSConcatenationSwu.entrySet()) {

        	if (entry.getValue().swu >= (minAU * entry.getValue().localLen)) {

            	candidateNum += 1;
                int addItem = entry.getKey();
                int sumUtility = 0;
                int upperBound = 0;
                int newprefixLength = 0;

                int newConLength = 0;
//                if (!isRemoveUnrise[addItem]) {
                if (entry.getValue().itemUtil >= minAU) {
                	newConLength = entry.getValue().localLen;
                } else {
                	newConLength = entry.getValue().localLen + 1;
                }

                ArrayList<ProjectULinkList> newProjectULinkListDB = new ArrayList<ProjectULinkList>();

                for (ProjectULinkList projectULinkList : projectedDB) {
                    ULinkList uLinkList = projectULinkList.getULinkList();
                    ArrayList<UPosition> uPositions = projectULinkList.getUPositions();

                    Integer[] itemIndices = uLinkList.getItemIndices(addItem);
                    if (itemIndices == null)  // addItem should be in the transaction
                        continue;
                    int utilityInTXN = 0;
                    int ubInTXN = 0;
                    ArrayList<UPosition> newUPositions = new ArrayList<UPosition>();

                    /*
                     * each addItem has multiple index (will become new UPosition) in the
                     * sequence, each index (will become new UPosition) can be s-concatenation
                     * with multiple UPositions (contain position of last item in prefix)
                     * before this index, but multiple UPositions s-concatenation with the same
                     * index are regarded as one new UPosition, so for each index, choose the
                     * maximal utility of UPositions before this index as prefix utility for
                     * this index.
                     */
                    int maxPositionUtility = 0;  // choose the maximal utility of UPositions
                    int uPositionNextItemsetPos = -1;

                    int addItemInd;
                    for (int i = 0, j = 0; j < itemIndices.length; j++) {
                        addItemInd = itemIndices[j];
                        for (; i < uPositions.size(); i++) {
                            uPositionNextItemsetPos = uLinkList.nextItemsetPos(uPositions.get(i).index());

                            // 1. next itemset should be in transaction
                            // 2. addItem should be after or equal to the next itemset of UPosition
                            if (uPositionNextItemsetPos != -1 && uPositionNextItemsetPos <= addItemInd) {
                                if (maxPositionUtility < uPositions.get(i).utility())
                                    maxPositionUtility = uPositions.get(i).utility();
                            } else {
                                break;
                            }
                        }

                        // maxPositionUtility is initialized outside the loop,
                        // will be the same or larger than before
                        if (maxPositionUtility != 0) {
                            int curUtility = uLinkList.utility(addItemInd) + maxPositionUtility;
                            newUPositions.add(new UPosition(addItemInd, curUtility));
                            utilityInTXN = Math.max(utilityInTXN, curUtility);
                            ubInTXN = Math.max(ubInTXN, getUpperBound(uLinkList, addItemInd, curUtility));
                        }
                    }

                    // if exist new positions, update the sumUtility and upper-bound
                    if (newUPositions.size() > 0) {

                        newprefixLength = projectULinkList.prefixLength() + 1;
                        newProjectULinkListDB.add(
                                	new ProjectULinkList(uLinkList, newUPositions, utilityInTXN, newprefixLength));

                        sumUtility += utilityInTXN;
                        upperBound += ubInTXN;
                    }
                }

                if (sumUtility >= minAU * newprefixLength) {
                    hauspNum += 1;
                	//prefix.add(-1);
                    //prefix.add(addItem);
                    //recordPattern(prefix, sumUtility); // 新增：记录模式
                    //prefix.remove(prefix.size() - 1);
                    //prefix.remove(prefix.size() - 1);
                }

                if (upperBound >= minAU * newConLength) {
                    prefix.add(-1);
                    prefix.add(addItem);
                    // call the function
                    runHUSPspan(newProjectULinkListDB);
                    prefix.remove(prefix.size() - 1);
                    prefix.remove(prefix.size() - 1);
                }
            }
        }
    }

    /**
     *
     * Example for check of S-Concatenation
     * <[(3:25)], [(1:32) (2:18) (4:10) (5:8)], [(2:12) (3:40) (5:1)]> 146
     * Pattern: 3 -1 2
     * UPositions: (3:25), (3:40)
     * For
     * addItemInd = firstPosOfItemByName = (2:18)
     *   UPosition = (3:25)
     *   uPositionNextItemsetPos = [(1:32) (2:18) (4:10) (5:8)]
     *   maxPositionUtility = 25
     *   UPosition = (3:40)
     *   uPositionNextItemsetPos = -1 -> break
     * newUPosition = 25 + 18
     * addItemInd = (2:12)
     *   UPosition = (3:40)
     *   uPositionNextItemsetPos = -1 -> break
     * newUPosition = 25 + 12
     * End
     */

    /**
     * PEU
     *
     * @param uLinkList
     * @param index
     * @param curUtility
     * @return
     */
    protected int getUpperBound(ULinkList uLinkList, int index, int curUtility) {
        return curUtility + uLinkList.remainUtility(index);//基于新的扩展位置的上界计算，这里先计算，随后在调用之后判断max
    }

    /**
     * PEU
     *
     * @param projectULinkList
     * @return
     */
    protected int getItemUpperBound(ProjectULinkList projectULinkList) {
        ULinkList uLinkList = projectULinkList.getULinkList();
        ArrayList<UPosition> uPositions = projectULinkList.getUPositions();
        int upperBound = 0;
        byte count = 0;
        for (UPosition uPosition : uPositions) {
            count++;
            int localPeu = uPosition.utility() + uLinkList.remainUtility(uPosition.index());
            if (localPeu > upperBound) {
                upperBound = localPeu;//更新，直到取最大的上界估计值
                firstPEU = true;//为了判定是否计算TRSU
                if (count != 1) {
                    firstPEU = false;
                    break;
                }
            }
        }

        // return upper-bound
        return upperBound;
    }


    /**
     * reset remaining utility for not removed item.
     */
    void firstRemoveItem() {
        for (ULinkList uLinkList : uLinkListDB) {
            int remainingUtility = 0;
            int remainingLength = 0;
            for (int i = uLinkList.length() - 1; i >= 0; --i) {
                int item = uLinkList.itemName(i);
                
                if (!isRemove[item]) {
                    uLinkList.setRemainUtility(i, remainingUtility);
                    remainingUtility += uLinkList.utility(i);
                }
            }
        }
        DBUpdated = false;
    }

    /**
     * items appear after prefix (including I-Concatenation and S-Concatenation item) in difference
     * sequences;
     * sum these MEU for each item as their upper bounds under prefix
     * PEU = max{position.utility + position.remaining utility}
     * should not add sequence utility of same sequence more than once
     * used for removing items to reduce remaining utility
     */

    /**
     * Get MapItemExtension utility
     *
     * @param projectULinkListDB
     * @return
     */
    protected HashMap<Integer, LastId> getMapItemExtensionUtility(
            ArrayList<ProjectULinkList> projectULinkListDB) {
        HashMap<Integer, LastId> mapItemExtensionUtility = new HashMap<Integer, LastId>();
        HashMap<Integer, Integer> currentMaxItemUtil = new HashMap<>(); // 当前uLinkList中各item的最大utility
        for (ProjectULinkList projectULinkList : projectULinkListDB) {
            ULinkList uLinkList = projectULinkList.getULinkList();
            ArrayList<UPosition> uPositions = projectULinkList.getUPositions();
            int swuInT = getItemUpperBound(projectULinkList);
            int prefixLen = projectULinkList.prefixLength();
            currentMaxItemUtil.clear();

            UPosition uPosition = uPositions.get(0);
            for (int i = uPosition.index() + 1; i < uLinkList.length(); ++i) {
            	
                int item = uLinkList.itemName(i);

                if (!isRemove[item]) {
                    LastId lastId = mapItemExtensionUtility.get(item);
                    
                    int itemUtilInT = uLinkList.utility(i);
                    
                    if (lastId == null) {

                        mapItemExtensionUtility.put(item, new LastId(swuInT, uLinkList, itemUtilInT, prefixLen));
                        //For next loop
                        currentMaxItemUtil.put(item, itemUtilInT);
                    } else {
                        if (lastId.uLinkList != uLinkList) {
                            lastId.swu += swuInT;
                            lastId.uLinkList = uLinkList;
                            lastId.itemUtil += itemUtilInT;
                            //For next loop
                            currentMaxItemUtil.put(item, itemUtilInT);
                        }else {
                        	//Only update itemUtil
                        	if (itemUtilInT > currentMaxItemUtil.get(item)) {
                        		int varMIU = itemUtilInT - currentMaxItemUtil.get(item);
                            	lastId.itemUtil += varMIU;
                            	//For next loop
                            	currentMaxItemUtil.put(item, itemUtilInT);
                            }
                            	
                        }
                    }
                }
            }
        }

        return mapItemExtensionUtility;
    }


    /**
     * Funtion of removeUnriseItem, using the position of remaining utility
     * used for variant of upper bound
     *
     * @param projectULinkListDB
     */
    protected void removeUnriseItem(ArrayList<ProjectULinkList> projectULinkListDB) {
        if(!DBUpdated)
            return;
        for (ProjectULinkList projectULinkList : projectULinkListDB) {
            ULinkList uLinkList = projectULinkList.getULinkList();
            ArrayList<UPosition> uPositions = projectULinkList.getUPositions();
            int positionIndex = uPositions.get(0).index();
            int remainingUtility = 0;
            //int remainingLength = 0;
            //has appear
//            HashSet<Integer> distinctItems = new HashSet<>();

            for (int i = uLinkList.length() - 1; i >= positionIndex; --i) {
                int item = uLinkList.itemName(i);

                if (!isRemove[item]) {
                    uLinkList.setRemainUtility(i, remainingUtility);
                    //uLinkList.setRemainLen(i, remainingLength);
                	
                	if (!isRemoveUnrise[item]) {
/**
                        // 获取item在序列中的所有出现位置
                        Integer[] allIndices = uLinkList.getItemIndices(item);
                        // 判断当前位置i是否是positionIndex之后的首次出现
                        boolean isFirst = false;
                        if (allIndices != null && allIndices.length > 0) {
                            for (int index : allIndices) {
                                if (index > positionIndex) {
                                    isFirst = (index == i);
                                    break; // 找到第一个>=positionIndex的位置后立即退出
                                }
                            }
                        }
                        
                        if (isFirst) {
                            remainingLength++; // 仅在首次出现时增加计数
                        }
*/
                        
/**                        // 仅记录rrs中的distinct items的计数
                        if (!distinctItems.contains(item)) {
                        	distinctItems.add(item);
                        	//for next loop
                            remainingLength ++;
                        }
*/
                        //for next loop
                        remainingUtility += uLinkList.utility(i);
                	}
//                    //for next loop
//                    remainingUtility += uLinkList.utility(i);
                } else {  // ??? can be delete 
                    // no, someone >= minUtility should reset remaining utility and remaining length
                    uLinkList.setRemainUtility(i, remainingUtility);
                    //uLinkList.setRemainLen(i, remainingLength);
                }
            }
        }
        DBUpdated = false;
    }
/**
    //记录发现的高频序列
    private void recordPattern(ArrayList<Integer> prefix, int utility) {
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < prefix.size(); i++) {
            pattern.append(prefix.get(i));
            if (i < prefix.size() - 1) {
                pattern.append(" ");
            }
        }
        pattern.append(" -1 -2 Utility: ").append(utility);
        patterns.add(pattern.toString());
    }
*/
    @Override
    public String toString() {
        return "HuspMiner{" +
                "threshold= " + threshold +
                ", DB= '" + pathname.split("/")[pathname.split("/").length - 1] +
                ", minAU= " + minAU +
                ", hauspNum= " + hauspNum +
                ", candidateNum= " + candidateNum +
                '}';
    }


    public ArrayList<String> getResults() {
        ArrayList<String> ret = new ArrayList<String>();
        ret.add("HuspMiner");
        ret.add("" + threshold);
        ret.add("" + hauspNum);
        ret.add("" + candidateNum);
        return ret;
    }

    /**
     * Print statistics about the algorithm execution
     */
    public void printStatistics()  {
        System.out.println("=============  HAUSP_PG ALGORITHM - STATS ============");
//		System.out.println(" Total utility of DB: " + databaseUtility);
//        System.out.println("minAU: " + String.format("%.5f", threshold));
        System.out.println("minAU: " + minAU);
        System.out.println("time: " + (System.currentTimeMillis() - currentTime)/1000.0 + " s");
        System.out.println("Max memory: " + MemoryLogger.getInstance().getMaxMemory() + "  MB");
        System.out.println("HAUSPs: " + hauspNum);
        System.out.println("Candidates: " + candidateNum);
    }


}