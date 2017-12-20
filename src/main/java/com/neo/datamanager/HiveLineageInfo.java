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
        String query = "insert into table aa  select * from bb union all select * from cc";
        HiveLineageInfo lep = new HiveLineageInfo();
        lep.getLineageInfo(query);
        System.out.println("Input tables = " + lep.getInputTableList());
        System.out.println("Output tables = " + lep.getOutputTableList());
    }
}
