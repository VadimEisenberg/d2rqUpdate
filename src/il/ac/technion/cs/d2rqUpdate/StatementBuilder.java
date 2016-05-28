/*
   Copyright 2010 Technion - Israel Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package il.ac.technion.cs.d2rqUpdate;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.fuberlin.wiwiss.d2rq.algebra.AliasMap;
import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;


/**
 * A base class for building SQL Statements (includig queries) 
 * 
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 */
public abstract class StatementBuilder {
	// adapted from 
	// http://ssdl-wiki.cs.technion.ac.il/wiki/index.php/Class_Separator
	final static class Separator {
	    boolean first = true;
	    String separatorString;
	    
	    Separator(String separatorString) {
	    	this.separatorString = separatorString;
	    }
	  
	    @Override
		public String toString() {
	        if (!first) 
	        {
	            return separatorString + " ";
	        }
	        first = false;
	        return "";
	    }
	}
	
	private final RelationName table;
	private final ConnectedDB database;
	
	
	protected ConnectedDB getDatabase() {
		return database;
	}

	public StatementBuilder(RelationName table,
			ConnectedDB database) {
		this.table = table;
		this.database = database;
	}
	
	protected String quoteRelationName(RelationName name){
		return database.quoteRelationName(name);
	}

	public RelationName getTable() {
		return table;
	}
	
	abstract public String getSQLStatement();


	protected void appendSeparatedEqualities(StringBuffer result, 
											 Map<Attribute, String> values,
											 String separatorString,
											 boolean qualified) {
		Set<Attribute> keys = values.keySet();
		Separator separator = new Separator(separatorString);
		for (Attribute key: keys) {
			appendSeparatedEquality(result, key, values.get(key), separator,
					qualified);
		}
	}
	
	protected void appendSeparatedAssignments(StringBuffer result,
		Map<Attribute, String> updateValues, String separatorString,
		boolean qualified) {
		Set<Attribute> keys = updateValues.keySet();
		Separator separator = new Separator(separatorString);
		for (Attribute key : keys) {
			appendSeparatedAssignment(result, toSQL(key,getDatabase(),qualified) 
					, updateValues.get(key), separator);
		}
	}
	
	private void appendSeparatedEquality(StringBuffer result, 
		Attribute attribute, String value, Separator separator, 
		boolean qualified) {
		String equality = value.equals("NULL") ? "IS" : "=";
		result.append(separator + toSQL(attribute, getDatabase(),qualified) 
				 + " " + equality + " " + value + " ");
	}
	
	private void appendSeparatedAssignment(StringBuffer result, String key,
		String value, Separator separator) {
		result.append(separator + key + " = " + value + " ");
	}
	
	protected void appendSeparatedEqualities(StringBuffer result, 
		Attribute attribute,
		List<String> values, String separatorString, boolean qualified) {
		
		Separator separator = new Separator(separatorString);
		for (String value : values) {
			appendSeparatedEquality(result, attribute, value, separator, 
					qualified);
		}
	}
	
	public static void appendSeparatedStrings(StringBuffer result,
		Set<Attribute> columns,String separatorString, ConnectedDB database,
		boolean qualified) {
		Set<String> columnNames = new HashSet<String>(columns.size());
		for (Attribute column : columns){
			columnNames.add(toSQL(column,database,qualified));
		}
		appendSeparatedStrings(result, columnNames.toArray(new String[]{}), 
				separatorString);
	}

	/**
	 * @param result
	 * @param values
	 * @param separatorString
	 */
	public static void appendSeparatedStrings(StringBuffer result,
		String[] values, String separatorString) {
		Separator separator = new Separator(separatorString);
		for (String value : values) {
			result.append(separator + value);
		}
		
	}

	/**
	 * @param tripleString
	 * @param triples
	 * @param separatorString
	 */
	public static void appendSeparatedStrings(StringBuffer result,
		List<String> triples, String separatorString) {
		appendSeparatedStrings(result, triples.toArray(new String[]{}), 
				separatorString);
		
	}
	
	public static String toSQL(Attribute attribute, ConnectedDB database, 
		boolean qualified){
		if (qualified) {
			return attribute.toSQL(database, AliasMap.NO_ALIASES);
		}
		// MySQL uses backticks
		if (database.dbTypeIs(ConnectedDB.MySQL)) {
			return database.backtickQuote(attribute.attributeName());
		}
		// PostgreSQL and Oracle (and SQL-92) use double quotes
		return database.doubleQuote(attribute.attributeName());
	}
}