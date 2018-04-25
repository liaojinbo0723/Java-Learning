package com.neo.datamanager;

import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;

import java.io.IOException;
import java.util.*;

public class HiveLineageInfo implements NodeProcessor {

//    private static final Logger logger = LoggerFactory.getLogger(HiveLineageInfo.class);

    /**
     * Stores input tables in sql.
     */
    TreeSet inputTableList = new TreeSet();
    /**
     * Stores output tables in sql.
     */
    TreeSet OutputTableList = new TreeSet();

    /**
     * @return java.util.TreeSet
     */
    public TreeSet getInputTableList() {
        return inputTableList;
    }

    /**
     * @return java.util.TreeSet
     */
    public TreeSet getOutputTableList() {
        return OutputTableList;
    }

    /**
     * Implements the process method for the NodeProcessor interface.
     */
    public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;

        switch (pt.getToken().getType()) {

            case HiveParser.TOK_CREATETABLE:
                OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
                break;
            case HiveParser.TOK_TAB:
                OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
                break;

            case HiveParser.TOK_TABREF:
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String table_name = (tabTree.getChildCount() == 1) ?
                        BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) :
                        BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                inputTableList.add(table_name);
                break;
        }
        return null;
    }

    /**
     * parses given query and gets the lineage info.
     *
     * @param query
     * @throws ParseException
     */
    public void getLineageInfo(String query) throws ParseException,
            SemanticException {

    /*
     * Get the AST tree
     */
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }

    /*
     * initialize Event Processor and dispatcher.
     */
        inputTableList.clear();
        OutputTableList.clear();

        // create a walker which walks the tree in a DFS manner while maintaining
        // the operator stack. The dispatcher
        // generates the plan from the operator tree
        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        // The dispatcher fires the processor corresponding to the closest matching
        // rule and passes the context along
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        // Create a list of topop nodes
        ArrayList topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }

    public static void main(String[] args) throws IOException, ParseException, SemanticException {
        String query =
                "INSERT overwrite table dma_zxopr_data.Rpt_Channel_Daily_Natural\n" +
                "select Static_Date,--统计日期\n" +
                "       Channel_Group,--渠道所属组\n" +
                "       Channel_Name,--渠道名\n" +
                "       Channel_Platform,--渠道所属平台\n" +
                "       Word,--关键词\n" +
                "       sum(Register_Count) as Register_Count,--注册人数\n" +
                "       SUM(Real_Name_Count) AS Real_Name_Count,--实名人数\n" +
                "       SUM(Bind_Card_Count) AS Bind_Card_Count,--绑卡人数\n" +
                "       SUM(Recharge_Count) AS Recharge_Count,--充值人数\n" +
                "       SUM(Invest_Count) AS Invest_Count,--投资人数\n" +
                "       SUM(Invest_Amount) AS Invest_Amount,--投资金额\n" +
                "       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS Process_Time\n" +
                "  from (select Static_Date,\n" +
                "               Channel_Group,\n" +
                "               Channel_Name,\n" +
                "               Channel_Platform,\n" +
                "               Word,\n" +
                "               Register_Count,\n" +
                "               Real_Name_Count,\n" +
                "               Bind_Card_Count,\n" +
                "               Recharge_Count,\n" +
                "               Invest_Count,\n" +
                "               Invest_Amount\n" +
                "          from tmp_datamanagement_od.TMP_Rpt_Channel_Daily_Natural_1\n" +
                "        union all\n" +
                "        SELECT Static_Date,\n" +
                "               Channel_Group,\n" +
                "               Channel_Name,\n" +
                "               Channel_Platform,\n" +
                "               Word,\n" +
                "               Register_Count,\n" +
                "               Real_Name_Count,\n" +
                "               Bind_Card_Count,\n" +
                "               Recharge_Count,\n" +
                "               Invest_Count,\n" +
                "               Invest_Amount\n" +
                "          FROM tmp_datamanagement_od.TMP_Rpt_Channel_Daily_Natural_2\n" +
                "        UNION ALL\n" +
                "        SELECT Static_Date,\n" +
                "               Channel_Group,\n" +
                "               Channel_Name,\n" +
                "               Channel_Platform,\n" +
                "               Word,\n" +
                "               Register_Count,\n" +
                "               Real_Name_Count,\n" +
                "               Bind_Card_Count,\n" +
                "               Recharge_Count,\n" +
                "               Invest_Count,\n" +
                "               Invest_Amount\n" +
                "          FROM tmp_datamanagement_od.TMP_Rpt_Channel_Daily_Natural_3\n" +
                "        UNION ALL\n" +
                "        SELECT Static_Date,\n" +
                "               Channel_Group,\n" +
                "               Channel_Name,\n" +
                "               Channel_Platform,\n" +
                "               Word,\n" +
                "               Register_Count,\n" +
                "               Real_Name_Count,\n" +
                "               Bind_Card_Count,\n" +
                "               Recharge_Count,\n" +
                "               Invest_Count,\n" +
                "               Invest_Amount\n" +
                "          FROM tmp_datamanagement_od.TMP_Rpt_Channel_Daily_Natural_4) a\n" +
                " group by Static_Date, Channel_Group, Channel_Name, Channel_Platform, Word\n";
        HiveLineageInfo lep = new HiveLineageInfo();
        lep.getLineageInfo(query);
        System.out.println("Input tables = " + lep.getInputTableList());
        System.out.println("Output tables = " + lep.getOutputTableList());
    }
}
