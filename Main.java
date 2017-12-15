package dubstep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

class Main2 extends Main implements Comparator<List<Integer>> {

	@Override
	public int compare(List<Integer> x, List<Integer> y) {
		// TODO Auto-generated method stubString firstString = tableData.get(sortOnTable).get(x.get(x.size()-1))[sortIndex];
		String firstString = tableData.get(sortOnTable).get(x.get(x.size()-1))[sortIndex];
		String secondString = tableData.get(sortOnTable).get(y.get(y.size()-1))[sortIndex];
		if ((firstString).compareTo((secondString)) > 0)
			return 1;
		else
			return -1;
	}
	
}

class Main3 extends Main implements Comparator<String[]>{
	
	public int compare(String[] x, String[] y) {
		int index = order.get(o);
		boolean isAsc = asc.get(o).equals("ASC");
		String format = orderDataTypes.get(o);
		switch (format) {
		case "string":
		case "STRING":
		case "varchar":
		case "VARCHAR":
		case "char":
		case "CHAR": {
			format = "String";
			break;
		}
		case "int":
		case "INT":
		case "decimal":
		case "DECIMAL": {
			format = "Double";
			break;
		}
		case "date":
		case "DATE": {
			format = "Date";
			break;
		}
		}

		double first, second;
		String firstString, secondString;

		if (format.equals("Double")) {
			first = Double.parseDouble(x[index]);
			second = Double.parseDouble(y[index]);
			if (first == second) {
				if (order.size() - 1 > o) {
					o++;
					int comparisonResult = compare(x, y);
					if (comparisonResult != 0)
						o = 0;
					return comparisonResult;
				} else {
					o = 0;
					return 0;
				}
			} else {
				if (isAsc) {
					if (first < second)
						return -1;
					else
						return 1;
				} else {
					if (first < second)
						return 1;
					else
						return -1;

				}
			}
		} else if (format.equals("String")) {
			firstString = x[index];
			secondString = y[index];

			if ((firstString).compareTo((secondString)) == 0) {
				if (order.size() - 1 > o) {
					o++;
					int comparisonResult = compare(x, y);
					if (comparisonResult != 0)
						o = 0;
					return comparisonResult;
				} else {
					o = 0;
					return 0;
				}
			} else {
				if (isAsc) {
					if ((firstString).compareTo((secondString)) < 0)
						return -1;
					else
						return 1;
				} else {
					if ((firstString).compareTo((secondString)) < 0)
						return 1;
					else
						return -1;

				}
			}

		} else if (format.equals("Date")) {
			Date date1 = null, date2 = null;
			try {
				date1 = sdf.parse(x[index]);
				date2 = sdf.parse(y[index]);
			} catch (Exception e) {
				System.out.println("Exception");
			}

			if (date1.compareTo(date2) == 0) {
				if (order.size() - 1 > o) {
					o++;
					int comparisonResult = compare(x, y);
					if (comparisonResult != 0)
						o = 0;
					return comparisonResult;
				} else {
					o = 0;
					return 0;
				}
			} else {
				if (isAsc) {
					if (date1.compareTo(date2) > 0)
						return 1;
					else
						return -1;
				} else {
					if (date1.compareTo(date2) > 0)
						return -1;
					else
						return 1;
				}
			}

		}
		return 0;
	}
	
}

public class Main extends Eval {

	public static HashMap<String, HashMap<String, Integer>> tables = null;
	static Map<String, Map> colNames=new HashMap<>();
	static Map<String, ArrayList<String>> colDataTypes=new HashMap<>();
	
	static Map<String, TreeMap<String, Integer>> indexValues;
	static List<List<Integer>> records;
	static ArrayList<String[]> data;
	static Map<String, ArrayList<String[]>> tableData=new HashMap();
	
	static Iterator<String[]> recordIter;
	static Map<String, String[]> jointRows;
	static String sortOnTable, sortOnColumn, sortFormat;
	static int sortIndex;
	static String[] rows;
	static int columnPos;
	static Long limit;
	static List<OrderByElement> orderBy;
	static List<String> orderDataTypes;
	static List<ColumnDefinition> columns;
	static CreateTable create;
	static String tableName;
	static ArrayList<String> printResults = new ArrayList<String>();
	static Double[] func = null;
	static boolean previousWhereClause = false;
	static boolean isInMem = false;
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	static boolean evalPart2 = false, evalPart3 = false;
	static String fileName = "";
	static File scanFile;
	static File tempFile;
	static File resultsFile = new File("results.txt");
	static PrintWriter tempWrite;
	static PrintWriter resultsWrite;
	static Scanner tempScan;
	static Scanner resultsScan;
	static Scanner scan;
	static File printResultsFile = new File("printResults.txt");
	static Scanner printResultsScanner;
	static PrintWriter printResultsWriter;
	static TreeMap<String,TreeMap<String, TreeMap<String,ArrayList<Integer>>>> indexes=new TreeMap<>();
	static ArrayList<Integer> order;
	static ArrayList<String> asc;
	static int o = 0;
	static Comparator<String[]> sorting = new Main3();
	static Comparator<List<Integer>> joinSort = new Main2();

	static List<String[]> results = new ArrayList<String[]>();
	// static ArrayList<String[]> subresults=new ArrayList<>();
	static String resultRow = "";
	static Runtime runtime = Runtime.getRuntime();
	static Main eval = new Main();

	public static void main(String[] args) throws Exception {
		System.out.print("$>");

		if (args[1].contains("in-mem"))
			isInMem = true;

		Scanner scanner = new Scanner((System.in));
		String ip = "";
		while (scanner.hasNextLine()) {
			ip = ip + " " + scanner.nextLine();
			if (!ip.contains(";"))
				continue;
			StringReader input = new StringReader(ip);
			ip = "";
			CCJSqlParser parser = new CCJSqlParser(input);
			Statement query;

			while ((query = parser.Statement()) != null) {

				if (query instanceof CreateTable) {

					//records = new ArrayList<String[]>();

					create = (CreateTable) query;
					tableName = create.getTable().getName();
					data=new ArrayList<>();
					columns = new ArrayList<ColumnDefinition>();
					columns = create.getColumnDefinitions();
					Map<String, Integer> colPosition = new HashMap<String, Integer>();
					ArrayList<String> colDataType = new ArrayList<String>();
					colDataType.clear();
					//scanFile = new File("U:\\eclipse_workspace\\Junaid\\Checkpoint3\\src\\dubstep\\data\\"+ tableName + ".csv");
					//scanFile = new File("C:\\Users\\tsankhe\\Desktop\\Checkpoint3\\"+ tableName + ".csv");
					//scanFile= new File("/Users/mitalibhiwande/Desktop/data/data1/"+tableName+".csv");
					scanFile = new File("data/" + tableName + ".csv");
					scan = new Scanner(scanFile);
					scan.useDelimiter("|");

					for (int i = 0; i < columns.size(); i++) {
						colPosition.put(columns.get(i).getColumnName(), i);
						colDataType.add(columns.get(i).getColDataType().getDataType());
					}

					colNames.put(tableName, colPosition);
					colDataTypes.put(tableName, colDataType);

					if (isInMem) {
						// System.out.println("aagaya!!");
						String row = "";
						while (scan.hasNextLine() && ((row = scan.nextLine()) != null))
							data.add(row.split("\\|"));
						scan.close();
						inMemIndexing();
						//System.out.println(indexes);
						
					} else {
						if (create.getIndexes() != null)
							eval.indexing();
					}

				} else if (query instanceof Select) {
					Expression e = null;
					SelectBody body = ((Select) query).getSelectBody();
					PlainSelect plain = (PlainSelect) body;
					tableName = ((Table) plain.getFromItem()).getName();
					//System.out.println(tableName);
					scan = new Scanner(scanFile);
					printResults.clear();
					func = null;
					limit = plain.getLimit() != null ? plain.getLimit().getRowCount() : Long.MAX_VALUE;
					orderBy = plain.getOrderByElements();
					previousWhereClause = false;
					if (isInMem) {
						//System.out.println(tableData.get(tableName));
						recordIter = tableData.get(tableName).iterator();
						executeSel(plain);
						List<OrderByElement> orderBy = plain.getOrderByElements();
						limit = plain.getLimit() != null ? plain.getLimit().getRowCount() : Long.MAX_VALUE;
						executeprint(plain);
					} else {
						printResultsWriter = new PrintWriter(printResultsFile, "UTF-8");
						printResultsWriter.print("");
						printResultsWriter.close();
						
						executeOnDisk(plain);
						executeprintOnDisk(plain);
					}

				}
			}
			System.out.print("$>");
			results.clear();

		}
		scanner.close();
	}

	public static void inMemIndexing(){


		ArrayList<Integer> intermediateInt;	
		String intermediateString;
		int pos=0;
		List<Index> indexes1 = create.getIndexes();
		List<String> indexedColumns = new ArrayList<String>();
		if(indexes1!=null)
		for (Index i : indexes1) {
			if (i.getType().toString().equalsIgnoreCase("PRIMARY KEY") ) {
				for (String l : i.getColumnsNames()) {
					indexedColumns.add(l);
					//mapArray.add(new TreeMap<String, Integer>());
				}
			}
		}
		indexes1 = null;
		
		for(ColumnDefinition c: columns)
			if(c.getColumnSpecStrings()!=null)
				indexedColumns.add(c.getColumnName());
		
		//System.out.println("Indexwdcolumns "+ indexedColumns);
		TreeMap<String,TreeMap<String,ArrayList<Integer>>> schema=new TreeMap<>();
		for(String c: indexedColumns)
		{
			
			TreeMap<String, ArrayList<Integer>> temp=new TreeMap<>();
			//String colName=c.getColumnName();
			recordIter = data.iterator();
			int rowNum = 0;	
			while(recordIter.hasNext())
			{
				rows = recordIter.next();
				//columnPos = (int) colNames.get(tableName).get(c);
				intermediateString = rows[pos];

				if(temp.containsKey(intermediateString))
				{
					ArrayList<Integer> rowsList=temp.get(intermediateString);
					rowsList.add(rowNum);	
				}
				else
				{
					intermediateInt = new ArrayList<Integer>();
					intermediateInt.add(rowNum);
					temp.put(intermediateString.toUpperCase(), intermediateInt);
				}	

				rowNum++;
			}
			schema.put(c.toUpperCase(), temp);

			temp=null;
			pos++;
		}
		indexes.put(tableName, schema);
		tableData.put(tableName,data);
		//System.out.println("tableData: "+tableData);
		schema=null;
	}


		public static void executeSel(PlainSelect select) throws Exception {
		FromItem from = select.getFromItem();
		Expression whereClause = null;
		boolean whereResult = false;
		whereClause = select.getWhere();
		records = new ArrayList<>();
		
		if (select.getJoins() != null) {
			List<Join> joins = select.getJoins();
			List<FromItem> fromItemList = new ArrayList<FromItem>();
			ArrayList<Expression> joinClauses = new ArrayList<Expression>();
			ArrayList<Expression> joinLefts = new ArrayList<Expression>();
			ArrayList<Expression> joinRights = new ArrayList<Expression>();
			fromItemList.add(from);
			for (Join join : joins) {
				fromItemList.add(join.getRightItem());
				joinClauses.add(null);
				joinLefts.add(null);
				joinRights.add(null);
			}

			Expression a = whereClause;
			while (a != null) {
				Expression toCheck;
				if (a instanceof AndExpression)
					toCheck = ((AndExpression) a).getRightExpression();
				else
					toCheck = a;
				for (int i = 0; i < fromItemList.size() - 1; i++) {

					if (toCheck.toString().contains(fromItemList.get(i).toString())
							&& (toCheck.toString().contains(fromItemList.get(i + 1).toString()))) {

						joinClauses.set(i, (toCheck));
					}
				}
				for (int i = 0; i < fromItemList.size() - 1; i++) {
					if (toCheck.toString().contains(fromItemList.get(i).toString()) && !joinClauses.contains(toCheck)
							&& !joinLefts.contains(toCheck) && !joinRights.contains(toCheck)) {
						if (joinLefts.get(i) != null)
							joinLefts.set(i, new AndExpression(((AndExpression) a).getRightExpression(), joinLefts.get(i)));
						else
							joinLefts.set(i, ((AndExpression) a).getRightExpression());
					} else if (toCheck.toString().contains(fromItemList.get(i + 1).toString())
							&& !joinClauses.contains(toCheck) && !joinLefts.contains(toCheck) && !joinRights.contains(toCheck)) {
						if (joinRights.get(i) != null)
							joinRights.set(i, new AndExpression(((AndExpression) a).getRightExpression(), joinRights.get(i)));
						else
							joinRights.set(i, ((AndExpression) a).getRightExpression());
					}
				}
				if (a instanceof AndExpression)
					a = ((AndExpression) a).getLeftExpression();
				else
					a = null;
			}
			for (int i = 0; i < fromItemList.size() - 1; i++) {
				//System.out.println("Executing join on: " + fromItemList.get(i) + " and: " + fromItemList.get(i + 1)+ " joinClause: " + joinClauses.get(i) + " left: " + joinLefts.get(i) + " joinRights: "+ joinRights.get(i));
				executeJoin(i ,fromItemList.get(i), fromItemList.get(i + 1), joinClauses.get(i), joinLefts.get(i),joinRights.get(i));
			}
		}
		//Can do a parallel sort-merge join only if the joining condition is on the same column from each table
	
	
		if (from instanceof SubSelect) {
			SelectBody subbody = ((SubSelect) from).getSelectBody();
			PlainSelect subsel = (PlainSelect) subbody;
			executeSel(subsel);
		}
	
	
		List<Function> functions = new ArrayList<Function>();
		if (!(select.getSelectItems().get(0) instanceof AllColumns)) {
			for (int i = 0; i < select.getSelectItems().size(); i++) {
				if ((Expression) ((SelectExpressionItem) select.getSelectItems().get(i))
						.getExpression() instanceof Function) {
					functions.add((Function) ((SelectExpressionItem) select.getSelectItems().get(i)).getExpression());
	
				}
			}
		}
	
		if (whereClause != null && select.getJoins() == null ) {
	
			previousWhereClause = true;
			ArrayList<String[]> temp = new ArrayList<>();
	
			while (recordIter.hasNext() && (orderBy != null ? true : temp.size() < limit)) {
				rows = recordIter.next();
				whereResult = eval.eval(whereClause).toBool();
				if (whereResult)
					temp.add(rows);
			}
	
			rows = null;
			results.clear();
			results.addAll(temp);
			temp = null;
		} else {
			if (!previousWhereClause) {
				results.clear();
				//results.addAll(records);
			}
		}
	
		recordIter = results.iterator();
	
		if (select.getGroupByColumnReferences() != null && results.size() > 0) {
			executeGroupBy(select);
		} else {
			if (functions.size() > 0 && results.size() > 0) {
				executeAggregate(select, functions);
			}
		}
	
		if (select.getOrderByElements() != null) {
			executeOrder(select, recordIter);
	
		}
	
	}

	private static void executeJoin(int joinNum, FromItem fromItem1, FromItem fromItem2, Expression joinClause, Expression joinLeft,
			Expression joinRight) throws Exception {
		String useSortedLeft = "";
		String useSortedRight = "";
		String sortLeftOn = "";
		ArrayList<ArrayList<Integer>> toAdd = new ArrayList<ArrayList<Integer>>();
		jointRows = new HashMap<String, String[]>();
		if (records.size() == 0) {
			for (Object o : colNames.get(fromItem1.toString()).keySet()) {
				if (joinClause.toString().contains(fromItem1.toString() + "." + o.toString())) {
					useSortedLeft = o.toString();
					break;
				}
			}
		}
		else{
			for (Object o : colNames.get(fromItem1.toString()).keySet()) {
				if (joinClause.toString().contains(fromItem1.toString() + "." + o.toString())) {
					sortLeftOn = o.toString();
					useSortedLeft = o.toString();
					break;
				}
			}
		}
		for (Object o : colNames.get(fromItem2.toString()).keySet()) {
			if (joinClause.toString().contains(fromItem2.toString() + "." + o.toString())) {
				useSortedRight = o.toString();
				
				break;
			}
		}
		if(!useSortedLeft.equals(""))
		if(indexes.get(fromItem1.toString().toUpperCase()).equals(""))
		{
		TreeMap<String,TreeMap<String,ArrayList<Integer>>> schema=new TreeMap<>();
		
			TreeMap<String, ArrayList<Integer>> temp=new TreeMap<>();
			//String colName=c.getColumnName();
			recordIter = tableData.get(fromItem1.toString().toUpperCase()).iterator();
			int rowNum = 0;
			int pos = (int) colNames.get(fromItem1.toString().toUpperCase()).get(useSortedLeft.toUpperCase());
			while(recordIter.hasNext())
			{
				rows = recordIter.next();
				//columnPos = (int) colNames.get(tableName).get(c);
				String intermediateString = rows[pos];

				if(temp.containsKey(intermediateString))
				{
					ArrayList<Integer> rowsList=temp.get(intermediateString);
					rowsList.add(rowNum);	
				}
				else
				{
					ArrayList<Integer> intermediateInt = new ArrayList<Integer>();
					intermediateInt.add(rowNum);
					temp.put(intermediateString.toUpperCase(), intermediateInt);
				}	

				rowNum++;
			}
			schema.put(useSortedLeft.toUpperCase(), temp);

			temp=null;
			pos++;
		
		indexes.put(fromItem1.toString().toUpperCase(), schema);
		}
		
		if(!useSortedRight.equals(""))
		if(indexes.get(fromItem2.toString().toUpperCase()).get(useSortedRight).keySet()==null)
		{
		TreeMap<String,TreeMap<String,ArrayList<Integer>>> schema=new TreeMap<>();
		
			TreeMap<String, ArrayList<Integer>> temp=new TreeMap<>();
			//String colName=c.getColumnName();
			recordIter = tableData.get(fromItem2.toString().toUpperCase()).iterator();
			int rowNum = 0;
			int pos = (int) colNames.get(fromItem2.toString().toUpperCase()).get(useSortedRight.toUpperCase());
			while(recordIter.hasNext())
			{
				rows = recordIter.next();
				//columnPos = (int) colNames.get(tableName).get(c);
				String intermediateString = rows[pos];

				if(temp.containsKey(intermediateString))
				{
					ArrayList<Integer> rowsList=temp.get(intermediateString);
					rowsList.add(rowNum);	
				}
				else
				{
					ArrayList<Integer> intermediateInt = new ArrayList<Integer>();
					intermediateInt.add(rowNum);
					temp.put(intermediateString.toUpperCase(), intermediateInt);
				}	

				rowNum++;
			}
			schema.put(useSortedRight.toUpperCase(), temp);

			temp=null;
			pos++;
		
		indexes.put(fromItem2.toString().toUpperCase(), schema);
		
		}
		
		
		if (joinNum==0) {
			//toAdd.add(new ArrayList<Integer>());
			
			records.add(new ArrayList<>());
			
			/*System.out.println("fromItem1: "+fromItem1.toString().toUpperCase());
			System.out.println("useSL: "+useSortedLeft);
			System.out.println("joinLeft: "+joinLeft);
			System.out.println(
					"Values to check: " + indexes.get(fromItem1.toString().toUpperCase()).get(useSortedLeft).keySet());
			*/
			if(joinLeft!=null){
			//System.out.println("from " + fromItem1.toString().toUpperCase());	
			//System.out.println("sortleft "+ useSortedLeft);
			//System.out.println("index on "+ indexes.get(fromItem1.toString().toUpperCase()).keySet());
			for (String s : indexes.get(fromItem1.toString().toUpperCase()).get(useSortedLeft).keySet()) {
				for(int x: indexes.get(fromItem1.toString().toUpperCase()).get(useSortedLeft).get(s)){
					rows = tableData.get(fromItem1.toString().toUpperCase()).get(x);
				if (eval.eval(joinLeft).toBool()) {
						records.get(records.size() - 1).add(x);
				}
				}
			}
			}
			else
			{	//System.out.println("first:"+ indexes.get(fromItem1.toString().toUpperCase()));
				for (String s : indexes.get(fromItem1.toString().toUpperCase()).get(useSortedLeft).keySet()) {
					for(int x: indexes.get(fromItem1.toString().toUpperCase()).get(useSortedLeft).get(s))
							records.get(records.size() - 1).add(x);
			}
			//System.out.println("Records now contains: " + records);
		}
		}
		else
		{
			sortOnTable = fromItem1.toString().toUpperCase();
			sortOnColumn = sortLeftOn;
			sortIndex = (int) colNames.get(sortOnTable).get(sortOnColumn);
			//System.out.println("sortOnTable: "+sortOnTable+" sortOnColumn: "+sortOnColumn+" sortIndex: "+sortIndex+" sortFormat: "+sortFormat);
			Collections.sort(records, joinSort);
			//System.out.println("records after sorting: "+records);
		}

		//toAdd.add(new ArrayList<Integer>());
		records.add(new ArrayList<>());
	/*	System.out.println("FromItem2: " + fromItem2.toString().toUpperCase());
		System.out.println("UseSR: " + useSortedRight);
		System.out.println("JoinRight: " + joinRight);
		System.out.println(
				"Values to check: " + indexes.get(fromItem2.toString().toUpperCase()).get(useSortedRight).keySet());
		*/
		if(joinRight!=null){
			
			//System.out.println("from " + fromItem2.toString().toUpperCase());	
			//System.out.println("sortleft "+ useSortedRight);
			//System.out.println("index on "+ indexes.get(fromItem2.toString().toUpperCase()).keySet());
			
		for (String s : indexes.get(fromItem2.toString().toUpperCase()).get(useSortedRight).keySet()) {
			for (int x : indexes.get(fromItem2.toString().toUpperCase()).get(useSortedRight).get(s)) {
				rows = tableData.get(fromItem2.toString().toUpperCase()).get(x);
				if (eval.eval(joinRight).toBool())
					records.get(records.size() - 1).add(x);
			}
		}
		}
		else{
			for (String s : indexes.get(fromItem2.toString().toUpperCase()).get(useSortedRight).keySet()) 
				for (int x : indexes.get(fromItem2.toString().toUpperCase()).get(useSortedRight).get(s)) 
					records.get(records.size() - 1).add(x);
		}
		//System.out.println("Records now contains: " + records);
		int columnPos1 = (int) colNames.get(fromItem1.toString().toUpperCase()).get(useSortedLeft.toUpperCase());
		int columnPos2 = (int) colNames.get(fromItem2.toString().toUpperCase()).get(useSortedRight.toUpperCase());
		int a = 0;
		int b = 0;
		if(joinNum==0){
			while (a < records.get(records.size()-2).size() && b < records.get(records.size() - 1).size()) {
				jointRows.put(fromItem1.toString(),
						tableData.get(fromItem1.toString()).get(records.get(records.size() - 2).get(a)));
				jointRows.put(fromItem2.toString(),
						tableData.get(fromItem2.toString()).get(records.get(records.size() - 1).get(b)));
				evalPart3 = true;
				if (eval.eval(joinClause).toBool()) {
					ArrayList<Integer> temp=new ArrayList<>();

					for (int i = 0; i < records.size(); i++) {

						if (i == records.size() - 1)
							temp.add(records.get(i).get(b));
						else
							temp.add(records.get(i).get(a));
					}
					toAdd.add(temp);
					b++;
				} 
				else {
						if (jointRows.get(fromItem1.toString().toUpperCase())[columnPos1]
								.compareTo(jointRows.get(fromItem2.toString().toUpperCase())[columnPos2]) > 0)
							b++;
						else
							a++;
					}
			}
		}
		else{
			//System.out.println("joinClause: "+joinClause);
		while (a < records.size()-1 && b < records.get(records.size() - 1).size()) {
			/*System.out.println("JointRows1: ");
			for(int i = 0; i<tableData.get(fromItem1.toString()).get(records.get(a).get(records.get(a).size()-1)).length; i++)
				System.out.print(tableData.get(fromItem1.toString()).get(records.get(a).get(records.get(a).size()-1))[i]);
			*/
			jointRows.put(fromItem1.toString(),
					tableData.get(fromItem1.toString()).get(records.get(a).get(records.get(a).size()-1)));
//			System.out.println("JointRows2: ");
//			for(int i = 0; i<tableData.get(fromItem2.toString()).get(records.get(records.size() - 1).get(b)).length; i++)
//				System.out.print(tableData.get(fromItem2.toString()).get(records.get(records.size() - 1).get(b))[i]);
//		
			jointRows.put(fromItem2.toString(),
					tableData.get(fromItem2.toString()).get(records.get(records.size() - 1).get(b)));
			evalPart3 = true;
			//System.out.println("jointRows: "+jointRows.);
			if (eval.eval(joinClause).toBool()) {
				//System.out.println("found match");
				ArrayList<Integer> temp=new ArrayList<>();
				
						temp.addAll(records.get(a));
						temp.add(records.get(records.size()-1).get(b));
						//System.out.println("left: "+records.get(a));
						//System.out.println("right: "+records.get(records.size()-1).get(b));
				toAdd.add(temp);
				a++;
			} 
			else {
					if (jointRows.get(fromItem1.toString().toUpperCase())[columnPos1]
							.compareTo(jointRows.get(fromItem2.toString().toUpperCase())[columnPos2]) > 0)
						b++;
					else
						a++;
				}
		}
		}
		evalPart3 = false;
		records.clear();
		records.addAll(toAdd);
		//System.out.println("post join records now contains: "+records.toString());
	}

		private static void executeAggregate(PlainSelect select, List<Function> functions)
				throws InvalidPrimitive, SQLException {
			// TODO Auto-generated method stub
			if(select.getJoins() == null){
			func = new Double[functions.size()];
			for (int i = 0; i < functions.size(); i++) {
				String funname = functions.get(i).getName();
				switch (funname) {
				case "SUM":
					func[i] = 0.0;
					break;
				case "MAX":
					func[i] = Double.MIN_VALUE;
					break;
				case "MIN":
					func[i] = Double.MAX_VALUE;
					break;
				case "COUNT":
					func[i] = 0.0;
				case "AVG":
					func[i] = 0.0;
		
				}
			}
			int count = 0;
			while (recordIter.hasNext()) {
				rows = recordIter.next();
				count++;
				for (int i = 0; i < functions.size(); i++) {
					String funname = functions.get(i).getName();
					ExpressionList params = functions.get(i).getParameters();
					Double temp = null;
					if (!(funname.equalsIgnoreCase("COUNT"))) {
						temp = eval.eval(params.getExpressions().get(0)).toDouble();
		
						switch (funname) {
						case "SUM": {
							func[i] += temp;
							break;
						}
						case "MIN": {
							if (temp < func[i])
								func[i] = temp;
							break;
						}
						case "MAX": {
							if (temp > func[i])
								func[i] = temp;
							break;
						}
						case "AVG": {
							func[i] += temp;
							break;
						}
						}
					}
				}
			}
		
			for (int i = 0; i < functions.size(); i++) {
				String funname = functions.get(i).getName();
				if (funname.equalsIgnoreCase("COUNT"))
					func[i] = (double) count;
				else if (funname.equalsIgnoreCase("AVG"))
					func[i] /= count;
			}
			}
		}

		private static void executeGroupBy(PlainSelect select) throws SQLException {
			if(select.getJoins() == null){
			// TODO Auto-generated method stub
			List<SelectItem> selist = select.getSelectItems();
			String key;
			List<String[]> temp;
			List<Column> groupList = select.getGroupByColumnReferences();
			Map<String, List<String[]>> groupMap = new TreeMap<String, List<String[]>>();
			List<Function> functions = new ArrayList<Function>();
			for (SelectItem si : selist) {
				if ((Expression) ((SelectExpressionItem) si).getExpression() instanceof Function)
					functions.add((Function) ((SelectExpressionItem) si).getExpression());
		
			}
			while (recordIter.hasNext()) {
				rows = recordIter.next();
				key = "";
				for (Column l : groupList)
					key = key + eval.eval(l);
				if (!groupMap.containsKey(key)) {
					temp = new ArrayList<String[]>();
					temp.add(rows);
					groupMap.put(key, temp);
				} else
					groupMap.get(key).add(rows);
		
			}
		
			for (String s : groupMap.keySet()) {
				results.clear();
				results.addAll(groupMap.get(s));
				recordIter = results.iterator();
				executeAggregate(select, functions);
				StringBuilder resultRow = new StringBuilder("");
				int counter = 0;
				for (SelectItem si : selist) {
					if ((Expression) ((SelectExpressionItem) si).getExpression() instanceof Function)
						resultRow.append(func[counter++]).append("|");
					else
						resultRow.append((eval.eval(((SelectExpressionItem) si).getExpression())).toRawString())
						.append("|");
				}
				resultRow.setLength(resultRow.length() - 1);
				printResults.add(resultRow.toString());
				func = null;
		
			}
			groupMap = null;
			functions = null;
			groupList = null;
			selist = null;
		}
		}

		private static void executeOrder(PlainSelect select, Iterator<String[]> recordIter2) {
			// TODO Auto-generated method stub
			if(select.getJoins() == null){
			orderBy = select.getOrderByElements();
			orderDataTypes = new ArrayList<String>();
			order = new ArrayList<Integer>();
			asc = new ArrayList<String>();
			if (orderBy != null) {
				for (int i = 0; i < orderBy.size(); i++) {
					for (int j = 0; j < columns.size(); j++) {
						if ((columns.get(j).getColumnName()).equals(orderBy.get(i).getExpression().toString()
								.substring(orderBy.get(i).getExpression().toString().indexOf('.') + 1))) {
							order.add(j);
							orderDataTypes.add(colDataTypes.get(tableName).get(j));
							asc.add(orderBy.get(i).isAsc() == true ? "ASC" : "DESC");
							break;
						}
					}
				}
			}
			if (orderBy != null) {
				Collections.sort(results, sorting);
		
				order = null;
				asc = null;
				orderBy = null;
			}
			recordIter = results.iterator();
		}
	}

		public static void executeprint(PlainSelect plain) throws SQLException {
		if(plain.getJoins() == null){
			int noOfSelItems = plain.getSelectItems().size();
			if (results.size() == 0 && printResults.size() == 0 && plain.getGroupByColumnReferences() == null) {
				for (int i = 0; i < noOfSelItems; i++) {
					if (plain.getSelectItems().get(i) instanceof AllColumns) {
						for (int j = 0; j < columns.size(); j++)
							System.out.print("|");
					} else {
						Expression e = (Expression) ((SelectExpressionItem) plain.getSelectItems().get(i)).getExpression();
						if (e instanceof Function) {
							if (((Function) e).getName().equalsIgnoreCase("COUNT")) {
								if (i < noOfSelItems - 1)
									System.out.print("0|");
								else
									System.out.print("0");
							} else
								System.out.print("|");
						} else {
							System.out.print("|");
						}
					}
				}
				System.out.println();

			}

			if (plain.getGroupByColumnReferences() == null && func == null) {
				while (recordIter.hasNext() && printResults.size() < limit) {
					rows = recordIter.next();
					if (plain.getSelectItems().get(0) instanceof AllColumns) {
						for (int i = 0; i < rows.length; i++) {
							if (i == rows.length - 1)
								resultRow = resultRow + rows[i];
							else
								resultRow = resultRow + rows[i] + "|";
						}
						printResults.add(resultRow);
						resultRow = "";
					} else {
						for (int i = 0; i < noOfSelItems; i++) {
							Expression e = ((SelectExpressionItem) plain.getSelectItems().get(i)).getExpression();
							resultRow = resultRow + (eval.eval(e).toRawString());
							if (i == noOfSelItems - 1) {
								printResults.add(resultRow);
								resultRow = "";
							} else
								resultRow = resultRow + "|";
						}
					}
				}

				for (int i = 0; i < printResults.size(); i++)
					System.out.println(printResults.get(i));

				printResults.clear();
			} else if (func != null && plain.getGroupByColumnReferences() == null) {
				for (int i = 0; i < func.length; i++) {
					if (i != func.length - 1)
						System.out.print(func[i] + "|");
					else
						System.out.println(func[i]);
				}
				func = null;
			} else if (func == null && plain.getGroupByColumnReferences() != null) {
				for (String s : printResults)
					System.out.println(s);

				printResults.clear();
			}
		}
	}

		private static void executeOnDisk(PlainSelect select) throws Exception {
			// System.out.println("totalBEFORE:" +runtime.totalMemory()/1024/1024);
			// System.out.println("Limit: "+limit);
			FromItem from = select.getFromItem();
			
			Expression whereClause = null;
			boolean whereResult = false;
			String indexColumnToUse = "";
			ArrayList<String> filesToCheck = new ArrayList<String>();
			if (from instanceof SubSelect) {
				SelectBody subbody = ((SubSelect) from).getSelectBody();
				PlainSelect subsel = (PlainSelect) subbody;
				executeSel(subsel);
			}
		
			whereClause = select.getWhere();
		
			if(select.getGroupByColumnReferences()==null && select.getOrderByElements()!=null)
			{
				Column c = (Column) select.getOrderByElements().get(0).getExpression();
				List<Column> list = new ArrayList<Column>();
				list.add(c);
				select.setGroupByColumnReferences(list);
			}
		
			Expression a = null;
			if (whereClause instanceof AndExpression) {
				a = (AndExpression) whereClause;
				while (((AndExpression) a).getLeftExpression() instanceof AndExpression) {
					a = (AndExpression) ((AndExpression) a).getLeftExpression();
				}
				for (String x : indexValues.keySet()) {
					if (a.toString().split(((AndExpression) a).getStringExpression())[0].contains(x.toUpperCase())
							&& a.toString().split(((AndExpression) a).getStringExpression())[1].contains(x.toUpperCase())) {
						indexColumnToUse = x;
						break;
					} else if (a.toString().split(((AndExpression) a).getStringExpression())[0].contains(x.toUpperCase())) {
						a = ((AndExpression) a).getLeftExpression();
						indexColumnToUse = x;
						break;
					} else if (a.toString().split(((AndExpression) a).getStringExpression())[1].contains(x.toUpperCase())) {
						// System.out.println("right expression contain index
						// column");
						a = ((AndExpression) a).getRightExpression();
						indexColumnToUse = x;
						break;
					}
				}
		
			} else if (whereClause instanceof BinaryExpression) {
				for (String x : indexValues.keySet()) {
					if (whereClause.toString().contains(x.toUpperCase())) {
						a = (BinaryExpression) whereClause;
						indexColumnToUse = x;
						break;
					}
				}
			} else
				a = null;
		
			if (a != null) {
				evalPart2 = true;
				for (String s : indexValues.get(indexColumnToUse).keySet()) {
					fileName = s;
					if (eval.eval(a).toBool())
						filesToCheck.add(indexColumnToUse + s);
				}
				evalPart2 = false;
			}
		
			List<Function> functions = new ArrayList<Function>();
			if (!(select.getSelectItems().get(0) instanceof AllColumns)) {
				for (int i = 0; i < select.getSelectItems().size(); i++) {
					if ((Expression) ((SelectExpressionItem) select.getSelectItems().get(i))
							.getExpression() instanceof Function) {
						functions.add((Function) ((SelectExpressionItem) select.getSelectItems().get(i)).getExpression());
		
					}
				}
			}
		
			String row = "";
			if (whereClause != null) {
				if (a != null) {
					int temp = 0;
					previousWhereClause = true;
					tempFile = new File("temp.txt");
					tempWrite = new PrintWriter(tempFile, "UTF-8");
					// System.out.println("Before reading file"+scan.hasNextLine());
					for (String s : filesToCheck) {
						scan = new Scanner(new File(s));
						scan.useDelimiter("|");
						while (scan.hasNextLine() && (row = scan.nextLine()) != null
								&& (orderBy != null ? true : temp < limit)) {
							rows = row.split("\\|");
							whereResult = eval.eval(whereClause).toBool();
							if (whereResult) {
								temp++;
								tempWrite.println(row);
							}
						}
						scan.close();
					}
					filesToCheck = null;
					temp = 0;
					tempWrite.close();
					scan.close();
					rows = null;
					resultsWrite = new PrintWriter(resultsFile, "UTF-8");
					tempScan = new Scanner(tempFile);
					while (tempScan.hasNextLine() && (row = tempScan.nextLine()) != null
							&& (orderBy != null ? true : temp < limit)) {
						resultsWrite.println(row);
					}
					resultsWrite.close();
					tempScan.close();
				} else {
					int temp = 0;
					previousWhereClause = true;
					tempFile = new File("temp.txt");
		
					scan.useDelimiter("|");
					tempWrite = new PrintWriter(tempFile, "UTF-8");
					// System.out.println("Before reading file"+scan.hasNextLine());
					while (scan.hasNextLine() && (row = scan.nextLine()) != null
							&& (orderBy != null ? true : temp < limit)) {
						rows = row.split("\\|");
						// System.out.println("Read: "+row+" from file");
						whereResult = eval.eval(whereClause).toBool();
						// System.out.println("where: "+whereResult);
						if (whereResult) {
							temp++;
							// System.out.println("Writing: "+row+" to temp");
							tempWrite.println(row);
						}
					}
					temp = 0;
					tempWrite.close();
					scan.close();
					rows = null;
					resultsWrite = new PrintWriter(resultsFile, "UTF-8");
					tempScan = new Scanner(tempFile);
					while (tempScan.hasNextLine() && (row = tempScan.nextLine()) != null
							&& (orderBy != null ? true : temp < limit)) {
						resultsWrite.println(row);
					}
					resultsWrite.close();
					tempScan.close();
				}
			} else {
				if (!previousWhereClause) {
					int temp = 0;
					resultsWrite = new PrintWriter(resultsFile, "UTF-8");
					scan = new Scanner(scanFile);
					// System.out.println("Before read: "+scan.hasNextLine());
					while (scan.hasNextLine() && (row = scan.nextLine()) != null
							&& (orderBy != null ? true : temp < limit)) {
						temp++;
						resultsWrite.println(row);
					}
					temp = 0;
					resultsWrite.close();
				}
			}
		
			boolean hasResults = false;
			scan = new Scanner(resultsFile);
			if (scan.hasNextLine() && (row = scan.nextLine()) != null) { // System.out.println(row);
				hasResults = true;
				scan = new Scanner(resultsFile);
		
			}
		
			if (select.getGroupByColumnReferences() != null && hasResults) {
				executeGroupByOnDisk(select);
			} else {
				if (functions.size() > 0 && hasResults) {
					executeAggregateOnDisk(select, functions);
				}
			}
		
			System.gc();
			// System.out.println("total:" +runtime.totalMemory()/1024/1024);
			// System.out.println("USED:" + (runtime.totalMemory() -
			// runtime.freeMemory())/1024/1024);
			// System.out.println("AVAIL:" +runtime.freeMemory());
			int conditionsToCheck=0;
			boolean checkOrder = true;
			if(select.getGroupByColumnReferences()!=null)
			{
				for(int i=0; i<select.getOrderByElements().size();i++)
				{
					for(int j=0; j<select.getGroupByColumnReferences().size();j++)
					{
						if(select.getGroupByColumnReferences().get(j).getColumnName().contains(select.getOrderByElements().get(i).toString()))
						{
							conditionsToCheck++;
							break;
						}
					}
				}
				if(conditionsToCheck==select.getOrderByElements().size())
					checkOrder=false;
			}
			if (select.getOrderByElements() != null && checkOrder) {
				//System.out.println("ordering");
				executeOrderOnDisk(select);
			}		
		}

		private static void executeGroupByOnDisk(PlainSelect select) throws IOException, SQLException, java.text.ParseException {
			// TODO Auto-generated method stub
			List<Column> groupList = select.getGroupByColumnReferences();
		
			boolean isOnlyDates = true;
			for(Column c: groupList)
			{
				if(!(colDataTypes.get(tableName).get((int) colNames.get(tableName).get(c.getColumnName().toUpperCase())).equalsIgnoreCase("Date")))
					isOnlyDates=false;
			}
			if(isOnlyDates){
				Date key;
				List<String[]> temp;
				Map<Date, Integer> groupMap= new TreeMap<Date, Integer>();
				List<Function> functions = new ArrayList<Function>();
				List<SelectItem> selist = select.getSelectItems();
		
				for (SelectItem si : selist) {
					if ((Expression) ((SelectExpressionItem) si).getExpression() instanceof Function)
						// System.out.println((Expression)((SelectExpressionItem)
						// select.getSelectItems().get(i)).getExpression());
						functions.add((Function) ((SelectExpressionItem) si).getExpression());
		
				}
		
				String row = "";
				while (scan.hasNextLine() && (row = scan.nextLine()) != null) {
					rows = row.split("\\|");
					key = null;
		
					for (Column l : groupList)
						key = sdf.parse(eval.eval(l).toRawString());
		
					String s = key.toString().replaceAll(" ", "").replaceAll(":", "");
					if (!groupMap.containsKey(key)) {
						PrintWriter group = new PrintWriter(s, "UTF-8");
						group.println(row);
						groupMap.put(key, 1);
						group.close();
					} else {
						FileWriter group = new FileWriter(s, true);
						group.write(row + "\n");
						group.close();
					}
				}
		
				int limitCounter=0;
				printResultsWriter = new PrintWriter(printResultsFile, "UTF-8");
				for (Date s : groupMap.keySet()) {
					resultsWrite = new PrintWriter(resultsFile, "UTF-8");
					Scanner groupScan = new Scanner(new File(s.toString().replaceAll(" ", "").replaceAll(":", "")));
					while (groupScan.hasNextLine() && (row = groupScan.nextLine()) != null && limitCounter<limit) {
						// System.out.println("Writing: "+row+" to results");
						limitCounter++;
						resultsWrite.println(row);
					}
					resultsWrite.close();
					groupScan.close();
					scan = new Scanner(resultsFile);
					//executeAggregateOnDisk(select, functions);
					StringBuilder resultRow = new StringBuilder("");
					int counter = 0;
					for (SelectItem si : selist) {
						if ((Expression) ((SelectExpressionItem) si).getExpression() instanceof Function)
							resultRow.append(func[counter++]).append("|");
						else
							resultRow.append((eval.eval(((SelectExpressionItem) si).getExpression())).toRawString())
							.append("|");
					}
					resultRow.setLength(resultRow.length() - 1);
					printResultsWriter.println(resultRow.toString());
					func = null;
					if(limitCounter>=limit)
						break;
		
				}
				printResultsWriter.close();
				groupMap = null;
				functions = null;
				groupList = null;
				selist = null;
		
			}
			else{
				String key;
				List<String[]> temp;
				Map<String, Integer> groupMap= new TreeMap<String, Integer>();
				List<Function> functions = new ArrayList<Function>();
				List<SelectItem> selist = select.getSelectItems();
		
				for (SelectItem si : selist) {
					if ((Expression) ((SelectExpressionItem) si).getExpression() instanceof Function)
						// System.out.println((Expression)((SelectExpressionItem)
						// select.getSelectItems().get(i)).getExpression());
						functions.add((Function) ((SelectExpressionItem) si).getExpression());
		
				}
		
				String row = "";
				while (scan.hasNextLine() && (row = scan.nextLine()) != null) {
					rows = row.split("\\|");
					key = "";
		
					for (Column l : groupList)
						key = key + eval.eval(l);
		
					if (!groupMap.containsKey(key)) {
						PrintWriter group = new PrintWriter(key + ".txt", "UTF-8");
						group.println(row);
						groupMap.put(key, 1);
						group.close();
					} else {
						FileWriter group = new FileWriter(key + ".txt", true);
						group.write(row + "\n");
						group.close();
					}
				}
		
				int limitCounter=0;
				printResultsWriter = new PrintWriter(printResultsFile, "UTF-8");
				for (String s : groupMap.keySet()) {
					resultsWrite = new PrintWriter(resultsFile, "UTF-8");
					Scanner groupScan = new Scanner(new File(s + ".txt"));
					while (groupScan.hasNextLine() && (row = groupScan.nextLine()) != null && limitCounter<limit) {
						// System.out.println("Writing: "+row+" to results");
						limitCounter++;
						resultsWrite.println(row);
					}
					resultsWrite.close();
					groupScan.close();
					scan = new Scanner(resultsFile);
					executeAggregateOnDisk(select, functions);
					StringBuilder resultRow = new StringBuilder("");
					int counter = 0;
					for (SelectItem si : selist) {
						if ((Expression) ((SelectExpressionItem) si).getExpression() instanceof Function)
							resultRow.append(func[counter++]).append("|");
						else
							resultRow.append((eval.eval(((SelectExpressionItem) si).getExpression())).toRawString())
							.append("|");
					}
					resultRow.setLength(resultRow.length() - 1);
					printResultsWriter.println(resultRow.toString());
					func = null;
					if(limitCounter>=limit)
						break;
		
				}
				printResultsWriter.close();
				groupMap = null;
				functions = null;
				groupList = null;
				selist = null;
			}
		}

		private static void executeAggregateOnDisk(PlainSelect select, List<Function> functions)
				throws InvalidPrimitive, SQLException {
			// TODO Auto-generated method stub
			func = new Double[functions.size()];
			for (int i = 0; i < functions.size(); i++) {
				String funname = functions.get(i).getName();
				switch (funname) {
				case "SUM":
					func[i] = 0.0;
					break;
				case "MAX":
					func[i] = Double.MIN_VALUE;
					break;
				case "MIN":
					func[i] = Double.MAX_VALUE;
					break;
				case "COUNT":
					func[i] = 0.0;
				case "AVG":
					func[i] = 0.0;
		
				}
			}
			int count = 0;
			String row = "";
			while (scan.hasNextLine() && (row = scan.nextLine()) != null) {
				// System.out.println("Row: "+row);
				rows = row.split("\\|");
				// System.out.println(rows[0]+" "+rows[1]+" "+rows[2]+" aggg");
				// System.out.println("size func"+functions.size());
				count++;
				for (int i = 0; i < functions.size(); i++) {
					String funname = functions.get(i).getName();
					ExpressionList params = functions.get(i).getParameters();
					Double temp = null;
					if (!(funname.equalsIgnoreCase("COUNT"))) {
		
						temp = eval.eval(params.getExpressions().get(0)).toDouble();
		
						switch (funname) {
						case "SUM": {
							func[i] += temp;
							break;
						}
						case "MIN": {
							if (temp < func[i])
								func[i] = temp;
							break;
						}
						case "MAX": {
							if (temp > func[i])
								func[i] = temp;
							break;
						}
						case "AVG": {
							func[i] += temp;
							break;
						}
						}
					}
				}
			}
		
			for (int i = 0; i < functions.size(); i++) {
				String funname = functions.get(i).getName();
				if (funname.equalsIgnoreCase("COUNT"))
					func[i] = (double) count;
				else if (funname.equalsIgnoreCase("AVG"))
					func[i] /= count;
			}
		}

		public static void executeprintOnDisk(PlainSelect plain)
				throws SQLException, FileNotFoundException, UnsupportedEncodingException {
		
			int noOfSelItems = plain.getSelectItems().size();
			int counter = 0;
		
			if (plain.getGroupByColumnReferences() == null && func == null) {
				String row = "";
				String otherRow = "";
				// scan=new Scanner(resultsFile);
				printResultsWriter = new PrintWriter(printResultsFile, "UTF-8");
				// System.out.println(scan.nextLine() +"row");
				while (scan.hasNextLine() && (row = scan.nextLine()) != null && counter < limit) {
					// System.out.println("row"+ row);
					rows = row.split("\\|");
					// System.out.println(rows.toString());
					if (plain.getSelectItems().get(0) instanceof AllColumns) {
						for (int i = 0; i < rows.length; i++) {
							if (i == rows.length - 1)
								resultRow = resultRow + rows[i];
							else
								resultRow = resultRow + rows[i] + "|";
						}
						printResultsWriter.println(resultRow);
						counter++;
						resultRow = "";
					} else {
						for (int i = 0; i < noOfSelItems; i++) {
							Expression e = ((SelectExpressionItem) plain.getSelectItems().get(i)).getExpression();
							// System.out.println(e);
							resultRow = resultRow + (eval.eval(e).toRawString());
							// System.out.println("result:"+ resultRow);
							if (i == noOfSelItems - 1) {
								printResultsWriter.println(resultRow);
								counter++;
								resultRow = "";
							} else
								resultRow = resultRow + "|";
						}
					}
				}
				// System.out.println("counter: "+counter);
		
				printResultsWriter.close();
				printResultsScanner = new Scanner(printResultsFile);
				while (printResultsScanner.hasNextLine() && (row = printResultsScanner.nextLine()) != null)
					System.out.println(row);
		
				printResultsScanner.close();
			}
		
			else if (func != null && plain.getGroupByColumnReferences() == null) {
				for (int i = 0; i < func.length; i++) {
					if (i != func.length - 1)
						System.out.print(func[i] + "|");
					else
						System.out.println(func[i]);
				}
				func = null;
			} else if (func == null && plain.getGroupByColumnReferences() != null) {
				printResultsScanner = new Scanner(printResultsFile);
				String row = "";
				while (printResultsScanner.hasNextLine() && (row = printResultsScanner.nextLine()) != null)
					System.out.println(row);
		
				printResultsScanner.close();
		
			}
		}

		private static void executeOrderOnDisk(PlainSelect select)
				throws FileNotFoundException, UnsupportedEncodingException, SQLException {
			// TODO Auto-generated method stub
		
			orderBy = select.getOrderByElements();
			orderDataTypes = new ArrayList<String>();
			order = new ArrayList<Integer>();
			asc = new ArrayList<String>();
			if (orderBy != null) {
				for (int i = 0; i < orderBy.size(); i++) {
					for (int j = 0; j < columns.size(); j++) {
						if ((columns.get(j).getColumnName()).equals(orderBy.get(i).getExpression().toString()
								.substring(orderBy.get(i).getExpression().toString().indexOf('.') + 1))) {
							order.add(j);
							orderDataTypes.add(colDataTypes.get(tableName).get(j));
							asc.add(orderBy.get(i).isAsc() == true ? "ASC" : "DESC");
							break;
						}
					}
				}
			}
			String row = "";
			results.clear();
			while (orderBy != null && scan.hasNextLine() && (row = scan.nextLine()) != null) {
				rows = row.split("\\|");
				results.add(rows);
			}
			scan.close();
			Collections.sort(results, sorting);
			resultsWrite = new PrintWriter(resultsFile, "UTF-8");
			StringBuilder lineToWrite;
			for (String[] r : results) {
				lineToWrite = new StringBuilder();
				for (int i = 0; i < r.length; i++)
					lineToWrite.append(r[i] + "|");
				resultsWrite.println(lineToWrite.toString());
			}
			results.clear();
			resultsWrite.close();
			System.gc();
			printResultsWriter.close();
			scan = new Scanner(resultsFile);
			order = null;
			asc = null;
			orderBy = null;
		}

		public void indexing() throws IOException {

			List<Index> indexes = create.getIndexes();
			List<String> indexedColumns = new ArrayList<String>();
			ArrayList<TreeMap<String, Integer>> mapArray = new ArrayList<TreeMap<String, Integer>>();
			indexValues = new TreeMap<String, TreeMap<String, Integer>>();
			for (Index i : indexes) {
				if (i.getType().toString().equalsIgnoreCase("PRIMARY KEY")) {
					for (String l : i.getColumnsNames()) {
						indexedColumns.add(l);
						mapArray.add(new TreeMap<String, Integer>());
					}
				}
			}
			indexes = null;

			FileWriter indexWriter;
			String key = "";
			String row = "";
			while (scan.hasNextLine() && (row = scan.nextLine()) != null) {
				rows = row.split("\\|");
				for (int i = 0; i < indexedColumns.size(); i++) {
					columnPos = (int) colNames.get(tableName).get(indexedColumns.get(i).toUpperCase());
					key = rows[columnPos];
					mapArray.get(i).put(key, 1);
					indexWriter = new FileWriter(indexedColumns.get(i) + key, true);
					indexWriter.write(row + "\n");
					indexWriter.close();
				}
			}
			scan.close();
			scan = new Scanner(scanFile);

			for (int i = 0; i < indexedColumns.size(); i++)
				indexValues.put(indexedColumns.get(i), mapArray.get(i));

			indexes = null;
			indexedColumns = null;
			mapArray = null;

			System.gc();


		}

		@Override
		public PrimitiveValue eval(Column c) throws SQLException {
			if(evalPart3){
				columnPos = (int) colNames.get(c.getTable().getName()).get(c.getColumnName().toUpperCase());
				switch (colDataTypes.get(c.getTable().getName()).get(columnPos)) {
				case "string":
				case "STRING":
				case "varchar":
				case "VARCHAR":
					return new StringValue(jointRows.get(c.getTable().getName())[columnPos]);
				case "char":
				case "CHAR":
					return new StringValue("'" + jointRows.get(c.getTable().getName())[columnPos] + "'");
				case "int":
				case "INT":
					return new LongValue(jointRows.get(c.getTable().getName())[columnPos]);
				case "decimal":
				case "DECIMAL":
					return new DoubleValue(jointRows.get(c.getTable().getName())[columnPos]);
				case "date":
				case "DATE":
					return new DateValue(jointRows.get(c.getTable().getName())[columnPos]);
				default:
					return null;
				}

			}
			else if(evalPart2){
				columnPos = (int) colNames.get(c.getTable().getName()).get(c.getColumnName().toUpperCase());
				switch (colDataTypes.get(c.getTable().getName()).get(columnPos)) {
				case "string":
				case "STRING":
				case "varchar":
				case "VARCHAR":
					return new StringValue(fileName);
				case "char":
				case "CHAR":
					return new StringValue("'" + fileName + "'");
				case "int":
				case "INT":
					return new LongValue(fileName);
				case "decimal":
				case "DECIMAL":
					return new DoubleValue(fileName);
				case "date":
				case "DATE":
					return new DateValue(fileName);
				default:
					return null;

				}
			}
			else {
				columnPos = (int) colNames.get(c.getTable().getName()).get(c.getColumnName().toUpperCase());
				switch (colDataTypes.get(c.getTable().getName()).get(columnPos)) {
				case "string":
				case "STRING":
				case "varchar":
				case "VARCHAR":
					return new StringValue(rows[columnPos]);
				case "char":
				case "CHAR":
					return new StringValue("'" + rows[columnPos] + "'");
				case "int":
				case "INT":
					return new LongValue(rows[columnPos]);
				case "decimal":
				case "DECIMAL":
					return new DoubleValue(rows[columnPos]);
				case "date":
				case "DATE":
					return new DateValue(rows[columnPos]);
				default:
					return null;
				}
			} 
		}

		public int compare(String[] x, String[] y) {
			int index = order.get(o);
			boolean isAsc = asc.get(o).equals("ASC");
			String format = orderDataTypes.get(o);
			switch (format) {
			case "string":
			case "STRING":
			case "varchar":
			case "VARCHAR":
			case "char":
			case "CHAR": {
				format = "String";
				break;
			}
			case "int":
			case "INT":
			case "decimal":
			case "DECIMAL": {
				format = "Double";
				break;
			}
			case "date":
			case "DATE": {
				format = "Date";
				break;
			}
			}

			double first, second;
			String firstString, secondString;

			if (format.equals("Double")) {
				first = Double.parseDouble(x[index]);
				second = Double.parseDouble(y[index]);
				if (first == second) {
					if (order.size() - 1 > o) {
						o++;
						int comparisonResult = compare(x, y);
						if (comparisonResult != 0)
							o = 0;
						return comparisonResult;
					} else {
						o = 0;
						return 0;
					}
				} else {
					if (isAsc) {
						if (first < second)
							return -1;
						else
							return 1;
					} else {
						if (first < second)
							return 1;
						else
							return -1;

					}
				}
			} else if (format.equals("String")) {
				firstString = x[index];
				secondString = y[index];

				if ((firstString).compareTo((secondString)) == 0) {
					if (order.size() - 1 > o) {
						o++;
						int comparisonResult = compare(x, y);
						if (comparisonResult != 0)
							o = 0;
						return comparisonResult;
					} else {
						o = 0;
						return 0;
					}
				} else {
					if (isAsc) {
						if ((firstString).compareTo((secondString)) < 0)
							return -1;
						else
							return 1;
					} else {
						if ((firstString).compareTo((secondString)) < 0)
							return 1;
						else
							return -1;

					}
				}

			} else if (format.equals("Date")) {
				Date date1 = null, date2 = null;
				try {
					date1 = sdf.parse(x[index]);
					date2 = sdf.parse(y[index]);
				} catch (Exception e) {
					System.out.println("Exception");
				}

				if (date1.compareTo(date2) == 0) {
					if (order.size() - 1 > o) {
						o++;
						int comparisonResult = compare(x, y);
						if (comparisonResult != 0)
							o = 0;
						return comparisonResult;
					} else {
						o = 0;
						return 0;
					}
				} else {
					if (isAsc) {
						if (date1.compareTo(date2) > 0)
							return 1;
						else
							return -1;
					} else {
						if (date1.compareTo(date2) > 0)
							return -1;
						else
							return 1;
					}
				}

			}
			return 0;
		}
}