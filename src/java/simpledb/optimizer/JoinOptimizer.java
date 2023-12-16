package simpledb.optimizer;

import simpledb.ParsingException;
import simpledb.common.Database;
import simpledb.execution.Join;
import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.execution.PlanCache;
import simpledb.execution.Predicate;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.WindowConstants;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    final LogicalPlan p;
    final List<LogicalJoinNode> joins;

    /**
     * Constructor
     *
     * @param p     the logical plan being optimized
     * @param joins the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, List<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best iterator for computing a given logical join, given the
     * specified statistics, and the provided left and right sub plans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because OpIterators don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1
     *
     * @param lj    The join being considered
     * @param plan1 The left join node's child
     * @param plan2 The right join node's child
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj,
                                             OpIterator plan1, OpIterator plan2) throws ParsingException {

        int t1id, t2id;
        OpIterator j;

        try {
            t1id = plan1.getTupleDesc().indexForFieldName(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().indexForFieldName(
                        lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field "
                        + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);

        j = new Join(p, plan1, plan2);

        return j;

    }

    /**
     * Estimate the cost of a join.
     * <p>
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU operations performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     *
     * @param j     A LogicalJoinNode representing the join operation being performed.
     * @param card1 Estimated cardinality of the left-hand side of the query
     * @param card2 Estimated cardinality of the right-hand side of the query
     * @param cost1 Estimated cost of one full scan of the table on the left-hand side of the query
     * @param cost2 Estimated cost of one full scan of the table on the right-hand side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A Logical Sub plan JoinNode represents a sub query.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            return cost1 + card1 * cost2 + card1 * card2;
        }
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     *
     * @param j      A LogicalJoinNode representing the join operation being
     *               performed.
     * @param card1  Cardinality of the left-hand table in the join
     * @param card2  Cardinality of the right-hand table in the join
     * @param t1pkey Is the left-hand table a primary-key table?
     * @param t2pkey Is the right-hand table a primary-key table?
     * @param stats  The table stats, referenced by table names, not alias
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
                                       boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A Logical Sub plan JoinNode represents a sub query.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias,
                    j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey,
                    stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
                                                   String table1Alias, String table2Alias, String field1PureName,
                                                   String field2PureName, int card1, int card2, boolean t1pkey,
                                                   boolean t2pkey, Map<String, TableStats> stats,
                                                   Map<String, Integer> tableAliasToId) {
        int card = 1;

        if (Objects.equals(joinOp, Predicate.Op.EQUALS)) {
            if (t1pkey && t2pkey) {
                card = Math.max(card1, card2);
            } else if (t1pkey) {
                card = card2;
            } else if (t2pkey) {
                card = card1;
            } else {
                card = Math.max(card1, card2);
            }
        } else {
            card = (int) (0.3 * card1 * card2);
        }

        return card <= 0 ? 1 : card;
    }

    /**
     * Helper method to enumerate all the subsets of a given size of a specified vector.
     *
     * @param v    The vector whose subsets are desired
     * @param size The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */
    public static <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> subsets = new HashSet<>();
        backtrack(v, size, 0, new HashSet<>(), subsets);
        return subsets;
    }

    private static <T> void backtrack(List<T> v, int size, int start, Set<T> currentSubset, Set<Set<T>> subsets) {
        if (currentSubset.size() == size) {
            subsets.add(new HashSet<>(currentSubset));
            return;
        }

        for (int i = start; i < v.size(); i++) {
            T element = v.get(i);
            if (!currentSubset.contains(element)) {
                currentSubset.add(element);
                backtrack(v, size, i + 1, currentSubset, subsets);
                currentSubset.remove(element);
            }
        }
    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * the Lab 3 description for hints on how this should be implemented.
     *
     * @param stats             Statistics for each table involved in the join, referenced by base table names,
     *                          not alias
     * @param filterSelectivity Selectivity of the filter predicates on each table in the join, referenced by
     *                          table alias (if no alias, the base table name)
     * @param explain           Indicates whether your code should explain its query plan or simply execute it
     * @return A List<LogicalJoinNode> that stores joins in the left-deep order in which they should be executed.
     * @throws ParsingException when stats or filter selectivity is missing a table in the join, or when another
     *                          internal error occurs
     */
    public List<LogicalJoinNode> orderJoins(Map<String, TableStats> stats,
                                            Map<String, Double> filterSelectivity,
                                            boolean explain) throws ParsingException {
        PlanCache planCache = new PlanCache();
        int numJoinNodes = this.joins.size();
        for (int i = 1; i <= numJoinNodes; i++) {
            Set<Set<LogicalJoinNode>> subSetOfJoins = enumerateSubsets(this.joins, i);
            for (Set<LogicalJoinNode> subSetOfJoin : subSetOfJoins) {
                double bestCost = Double.MAX_VALUE;
                int bestCard = 0;
                List<LogicalJoinNode> bestPlan = null;
                for (LogicalJoinNode joinToRemove : subSetOfJoin) {
                    CostCard costCard = this.computeCostAndCardOfSubplan(stats, filterSelectivity,
                            joinToRemove, subSetOfJoin, bestCost, planCache);
                    if (Objects.nonNull(costCard)) {
                        bestCost = costCard.cost;
                        bestPlan = costCard.plan;
                        bestCard = costCard.card;
                    }
                }
                if (Objects.nonNull(bestPlan)) {
                    planCache.addPlan(subSetOfJoin, bestCost, bestCard, bestPlan);
                }
            }
        }

        List<LogicalJoinNode> order = planCache.getOrder(new HashSet<>(this.joins));
        if (explain) {
            this.printJoins(order, planCache, stats, filterSelectivity);
        }
        return order;
    }

    // ===================== Private Methods =================================

    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats             table stats for all the tables, referenced by table names rather than alias
     *                          (see {@link #orderJoins})
     * @param filterSelectivity the selectivity of the filters over each of the tables (where tables are
     *                          identified by their alias or name if no alias is given)
     * @param joinToRemove      the join to remove from joinSet
     * @param joinSet           the set of joins being considered
     * @param bestCostSoFar     the best way to join joinSet so far (minimum of previous invocations of
     *                          computeCostAndCardOfSubplan for this joinSet, from returned CostCard)
     * @param pc                the PlanCache for this join; should have subplans for all plans of size
     *                          joinSet.size()-1
     * @return A {@link CostCard} objects desribing the cost, cardinality, optimal subplan
     * @throws ParsingException when stats, filterSelectivity, or pc object is missing tables involved in join
     */
    @SuppressWarnings("unchecked")
    private CostCard computeCostAndCardOfSubplan(Map<String, TableStats> stats,
                                                 Map<String, Double> filterSelectivity,
                                                 LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet,
                                                 double bestCostSoFar, PlanCache pc) throws ParsingException {

        LogicalJoinNode j = joinToRemove;

        List<LogicalJoinNode> prevBest;

        if (this.p.getTableId(j.t1Alias) == null) {
            throw new ParsingException("Unknown table " + j.t1Alias);
        }
        if (this.p.getTableId(j.t2Alias) == null) {
            throw new ParsingException("Unknown table " + j.t2Alias);
        }

        String table1Name = Database.getCatalog().getTableName(this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias;
        String table2Alias = j.t2Alias;

        Set<LogicalJoinNode> news = new HashSet<>(joinSet);
        news.remove(j);

        double t1cost, t2cost;
        int t1card, t2card;
        boolean leftPkey, rightPkey;

        if (news.isEmpty()) { // base case -- both are base relations
            prevBest = new ArrayList<>();
            t1cost = stats.get(table1Name).estimateScanCost();
            t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivity.get(j.t1Alias));
            leftPkey = isPkey(j.t1Alias, j.f1PureName);

            t2cost = table2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
            t2card = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateTableCardinality(filterSelectivity.get(j.t2Alias));
            rightPkey = table2Alias != null && isPkey(table2Alias, j.f2PureName);
        } else {
            // news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news);

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest == null) {
                return null;
            }

            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            // estimate cost of right subtree
            if (doesJoin(prevBest, table1Alias)) { // j.t1 is in prevBest
                t1cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name).estimateTableCardinality(
                        filterSelectivity.get(j.t2Alias));
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName);
            } else if (doesJoin(prevBest, j.t2Alias)) { // j.t2 is in prevbest
                // (both
                // shouldn't be)
                t2cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);
                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivity.get(j.t1Alias));
                leftPkey = isPkey(j.t1Alias, j.f1PureName);

            } else {
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return null;
            }
        }

        // case where prevbest is left
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return null;

        CostCard cc = new CostCard();

        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey, rightPkey, stats);
        cc.cost = cost1;
        cc.plan = new ArrayList<>(prevBest);
        cc.plan.add(j); // prev best is left -- add new join to end
        return cc;
    }

    /**
     * Return true if the specified table is in the list of joins, false otherwise
     */
    private boolean doesJoin(List<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table) || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false otherwise
     *
     * @param tableAlias The alias of the table in the query
     * @param field      The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in joinlist
     */
    private boolean hasPkey(List<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName) || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     *
     * @param js            the join plan to visualize
     * @param pc            the PlanCache accumulated whild building the optimal plan
     * @param stats         table statistics for base tables
     * @param selectivities the selectivities of the filters over each of the tables
     *                      (where tables are indentified by their alias or name if no
     *                      alias is given)
     */
    private void printJoins(List<LogicalJoinNode> js, PlanCache pc,
                            Map<String, TableStats> stats,
                            Map<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        Map<String, DefaultMutableTreeNode> m = new HashMap<>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(this.p.getTableId(j.t2Alias));

            // Double c = pc.getCost(pathSoFar);
            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost =" + pc.getCost(pathSoFar) +
                    ", card = " + pc.getCard(pathSoFar) + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                        selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                + " (Cost = "
                                + stats.get(table2Name)
                                .estimateScanCost()
                                + ", card = "
                                + stats.get(table2Name)
                                .estimateTableCardinality(
                                        selectivities
                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.isEmpty()) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }

}
