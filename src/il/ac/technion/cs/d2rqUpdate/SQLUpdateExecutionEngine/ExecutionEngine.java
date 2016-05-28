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

 
package il.ac.technion.cs.d2rqUpdate.SQLUpdateExecutionEngine;

import il.ac.technion.cs.d2rqUpdate.D2RQUpdateException;
import il.ac.technion.cs.d2rqUpdate.SelectStatementBuilder;
import il.ac.technion.cs.d2rqUpdate.SQLUpdateExecutionEngine.UpdateStatement.Type;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import de.fuberlin.wiwiss.d2rq.D2RQException;
import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */
public class ExecutionEngine {
	static private Log log = LogFactory.getLog(ExecutionEngine.class);
	
	private final List<UpdateStatement> statements = 
		new ArrayList<UpdateStatement>();
	
	private static class ForeignKeyConstraint {
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result =
					prime
							* result
							+ ((referencedAttribute == null) ? 0
									: referencedAttribute.hashCode());
			result =
					prime
							* result
							+ ((referencingAttribute == null) ? 0
									: referencingAttribute.hashCode());
			return result;
		}
		public Attribute getReferencingAttribute() {
			return referencingAttribute;
		}
		public Attribute getReferencedAttribute() {
			return referencedAttribute;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			ForeignKeyConstraint other = (ForeignKeyConstraint) obj;
			if (referencedAttribute == null) {
				if (other.referencedAttribute != null) {
					return false;
				}
			} else if (!referencedAttribute.equals(other.referencedAttribute)) {
				return false;
			}
			if (referencingAttribute == null) {
				if (other.referencingAttribute != null) {
					return false;
				}
			} else if (!referencingAttribute.equals(other.referencingAttribute)) {
				return false;
			}
			return true;
		}
		
		public ForeignKeyConstraint(Attribute referencingAttribute,
				Attribute referencedAttribute) {
			this.referencingAttribute = referencingAttribute;
			this.referencedAttribute = referencedAttribute;
		}
		private final Attribute referencingAttribute;
		private final Attribute referencedAttribute;
	}
	
	public void add(UpdateStatement statement){
		statements.add(statement);
	}
	
	public void execute(){
		List<UpdateStatement> sortedStatements = getSortedStatements();
		execute(sortedStatements);
		statements.clear();
	}
	
	/**
	 * @param sortedStatements
	 */
	private void execute(List<UpdateStatement> sortedStatements) {
		
		
		if(sortedStatements.size() < 1) {
			return;
		}
		
		//TODO implement handling different databases
		Connection connection = 
			sortedStatements.get(0).getDatabase().connection();
		
		boolean executeInBatch = false;
		try {
			executeInBatch = 
				connection.getMetaData().supportsBatchUpdates();
		} catch (SQLException exception) {
			log.warn(exception);
			// do nothing - just do not execute in batch
		}
		
		if (executeInBatch){
			executeInBatch(sortedStatements, connection);
		}
		else {
			executeOneByOne(sortedStatements, connection);
		}
	}

	/**
	 * @param sortedStatements
	 * @param connection
	 */
	private void executeOneByOne(List<UpdateStatement> sortedStatements,
		Connection connection) {
		try {	   
			for (UpdateStatement updateStatement : sortedStatements) {
				log.debug("execute not in batch: " + 
						updateStatement.getSqlString());
				 Statement statement = 
						connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_READ_ONLY);
				int updateCounter = 
					statement.executeUpdate(updateStatement.getSqlString());
				checkUpdateCounter(updateCounter, updateStatement);
				statement.close();
			}		
			
		}
		catch (SQLException exception) {
			throw new D2RQException(exception);
		}
		
	}

	/**
	 * @param updateCounter
	 * @param updateStatement
	 */
	private void checkUpdateCounter(int updateCounter,
		UpdateStatement updateStatement) {
		if (updateCounter != Statement.SUCCESS_NO_INFO 
				&& updateCounter < 
					updateStatement.getMinimalNumberOfUpdatedRows()) {
			throw new D2RQUpdateException("SQL " + 
					updateStatement.getUpdateType() + 
			" failed", D2RQUpdateException.SQL_STATEMENT_FAILED);
		}
	}

	private void executeInBatch(List<UpdateStatement> sortedStatements,
		Connection connection) {
		try {
			boolean previousAutoCommit = connection.getAutoCommit();
		    connection.setAutoCommit(false);
		    Statement statement = 
				connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
		    
			for (UpdateStatement updateStatement : sortedStatements) {
				log.debug("add to batch: " + updateStatement.getSqlString());
				statement.addBatch(updateStatement.getSqlString());
			}
			
			int [] updateCounters = statement.executeBatch();
			connection.commit();
			connection.setAutoCommit(previousAutoCommit);
			statement.close();
			checkUpdateCounters(updateCounters, sortedStatements);
			
		}
		catch (SQLException exception) {
			throw new D2RQException(exception);
		}
	}

	/**
	 * @param updateCounters
	 */
	private void checkUpdateCounters(int[] updateCounters,
									 List<UpdateStatement> statements) {
		int updateCounterIndex = 0;
		for (UpdateStatement updateStatement : statements) {
			checkUpdateCounter(updateCounters[updateCounterIndex],
					updateStatement);
			updateCounterIndex++;
		}	
	}
	
	
	/**
	 * @return
	 */
	private List<UpdateStatement> getSortedStatements() {
		
		DirectedGraph<UpdateStatement, DefaultEdge> dependencyGraph =
				createDependciesGraph();
		
		handleCycles(dependencyGraph);		
		return getTopologicallySortedStatements(dependencyGraph);
	}

	/**
	 * @param dependencyGraph
	 * @return
	 */
	private List<UpdateStatement> getTopologicallySortedStatements(
		DirectedGraph<UpdateStatement, DefaultEdge> dependencyGraph) {
		
		List<UpdateStatement> sortedStatements = 
			new ArrayList<UpdateStatement>(statements.size());
		
		Iterator<UpdateStatement> iterator =
            new TopologicalOrderIterator<UpdateStatement, DefaultEdge>(
            		dependencyGraph);
		
		while(iterator.hasNext()){
			sortedStatements.add(iterator.next());
		}
	
		return sortedStatements;
	}

	/**
	 * @param dependencyGraph
	 */
	private void handleCycles(
		DirectedGraph<UpdateStatement, DefaultEdge> dependencyGraph) {
		CycleDetector<UpdateStatement, DefaultEdge> detector =
            new CycleDetector<UpdateStatement, DefaultEdge>(dependencyGraph);
		Set<UpdateStatement> statementsInCycle = detector.findCycles();
		
		if (!statementsInCycle.isEmpty()) {
			StringBuffer statementsString = new StringBuffer();
			for(UpdateStatement statement : statementsInCycle){
				statementsString.append(statement.getSqlString() + ";\n");
			}
		
			throw new D2RQException("Update failed: there are cyclic " + 
					"dependencies among the following statements:\n" + 
					statementsString);
		}		
	}

	private DirectedGraph<UpdateStatement, DefaultEdge> createDependciesGraph() {
		
		DirectedGraph<UpdateStatement, DefaultEdge> dependencyGraph =
			createGraphWithVerticesOnly();
		
		addDependencyEdges(dependencyGraph);
		return dependencyGraph;
	}

	private void addDependencyEdges(
		DirectedGraph<UpdateStatement, DefaultEdge> dependencyGraph) {
		if(statements.size() < 1) {
			return;
		}
		
		//TODO implement handling different databases
		ConnectedDB database = statements.get(0).getDatabase();
		
		Map<RelationName, Set<ForeignKeyConstraint>> foreignKeys =
				getForeignKeys(statements, database);
		
		Map<Attribute, Set<UpdateStatement>> attributes2StatementsMap =
				getAttributes2StatementsMap(statements);
		
		for (Set<ForeignKeyConstraint> foreignKeysPerTable : 
			foreignKeys.values()) {
			for ( ForeignKeyConstraint foreignKey : foreignKeysPerTable) {
				addDependencyEdges(dependencyGraph, database,
						attributes2StatementsMap, foreignKey);
			}
		}
	}

	private void addDependencyEdges(
		DirectedGraph<UpdateStatement, DefaultEdge> dependencyGraph,
		ConnectedDB database,
		Map<Attribute, Set<UpdateStatement>> attributes2StatementsMap,
		ForeignKeyConstraint foreignKey) {
		Attribute referencingAttribute = 
			foreignKey.getReferencingAttribute();
		
		if (!attributes2StatementsMap.containsKey(referencingAttribute)
				) {
			return;
		}
		Set<UpdateStatement> referencingStatements = 
			attributes2StatementsMap.get(referencingAttribute);
		
		Attribute referencedAttribute = 
			foreignKey.getReferencedAttribute();
		
		if (!attributes2StatementsMap.containsKey(referencedAttribute)){
			return;
		}
		
		Set<UpdateStatement> referencedStatements = 
			attributes2StatementsMap.get(referencedAttribute);
	
		addDependencyEdges(dependencyGraph, database,
				referencingAttribute, referencedAttribute, referencingStatements,
				referencedStatements);
	}

	private void addDependencyEdges(
		DirectedGraph<UpdateStatement, DefaultEdge> dependencyGraph,
		ConnectedDB database, Attribute referencingAttribute,
		Attribute referencedAttribute, 
		Set<UpdateStatement> referencingStatements,
		Set<UpdateStatement> referencedStatements) {
		
		
		for(UpdateStatement referencingStatement : referencingStatements){
			Type referencingType = referencingStatement.getUpdateType();
			String newReferencingValue =  
				referencingStatement.getUpdatedAttributes().
				get(referencingAttribute);
			
			for(UpdateStatement referencedStatement : referencedStatements){
				Type referencedType = referencedStatement.getUpdateType();
				String newReferencedValue =  
					referencedStatement.getUpdatedAttributes().
					get(referencedAttribute);
				
				if((referencingType == Type.UPDATE_TO_NON_NULL_VALUE ||
				    referencingType == Type.INSERT) &&
				   referencedType == Type.INSERT) {
					if (newReferencingValue.equals(newReferencedValue)) {
						dependencyGraph.addEdge(referencedStatement,
								referencingStatement);
					}
				}
				
				if( referencedType == Type.DELETE &&
					(referencingType == Type.UPDATE_TO_NULL ||
				     referencingType == Type.DELETE)) {
					// delete of the primary key must happen after the 
					// update to NULL or delete of the foreign key
					
					if (!containsNULL(referencingAttribute,referencingStatement, 
							database)) {
						Set<String>  deletedReferencingValues = 
							getValue(referencingAttribute,referencingStatement, 
									database);
						
						Set<String> deletedReferencedValues = 
							getValue(referencedAttribute,referencedStatement, 
									database);
						
						deletedReferencingValues.retainAll(
									deletedReferencedValues);
								
						if (!deletedReferencingValues.isEmpty()) {
							dependencyGraph.addEdge(referencingStatement,
									referencedStatement);
						}
					}
				}
				
			}	
		}
	}

	/**
	 * @param referencingAttribute
	 * @param referencingStatement
	 * @param database
	 * @return
	 */
	private Set<String> getValue(Attribute referencingAttribute,
		UpdateStatement referencingStatement, ConnectedDB database) {
		
		
		Map<Attribute,String> conditionValues = new HashMap<Attribute,String>();
		conditionValues.putAll(referencingStatement.getSubjectValues());
		
		String selectSQLString = new SelectStatementBuilder(
				referencingAttribute.relationName(), database,
				Collections.singleton(referencingAttribute),
				conditionValues
				).getSQLStatement();
				
			try {
				Connection connection = database.connection();
				Statement selectStatement = connection.createStatement();
				ResultSet resultSet = selectStatement.executeQuery(selectSQLString);
				if (!resultSet.next()) {
					return Collections.singleton("NULL");
				}
				Set<String> values = new HashSet<String>();
				do {
					values.add(resultSet.getString(1));
				} while (resultSet.next());
				return values;
			}
			catch (SQLException exceptionFromQuery) {
				throw new D2RQException(exceptionFromQuery.getMessage() + ": " 
										+ selectSQLString);
			}
	}

	/**
	 * @param referencingAttribute
	 * @param referencingStatement 
	 * @return
	 */
	private boolean containsNULL(Attribute referencingAttribute, 
		UpdateStatement referencingStatement, ConnectedDB database) {
		
		Map<Attribute,String> conditionValues = new HashMap<Attribute,String>();
		conditionValues.putAll(referencingStatement.getSubjectValues());
		conditionValues.put(referencingAttribute,"NULL");
		
		String selectSQLString = new SelectStatementBuilder(
				referencingAttribute.relationName(), database,
				Collections.singleton(referencingAttribute),
				conditionValues
				).getSQLStatement() + 
				" LIMIT 1";
					
			try {
				Connection connection = database.connection();
				Statement selectStatement = connection.createStatement();
				ResultSet resultSet = selectStatement.executeQuery(selectSQLString);
				return resultSet.next();
			}
			catch (SQLException exceptionFromQuery) {
				throw new D2RQException(exceptionFromQuery.getMessage() + ": " 
										+ selectSQLString);
			}
	}

	private DirectedGraph<UpdateStatement, DefaultEdge> 
		createGraphWithVerticesOnly() {
		DirectedGraph<UpdateStatement,DefaultEdge> dependencyGraph = 
			new  DefaultDirectedGraph<UpdateStatement,DefaultEdge>(
					DefaultEdge.class);
		for (UpdateStatement statement : statements) {
			dependencyGraph.addVertex(statement);
		}
		return dependencyGraph;
	}

	private Map<Attribute, Set<UpdateStatement>> getAttributes2StatementsMap(
		List<UpdateStatement> statements) {
		Map<Attribute, Set<UpdateStatement>> attributes2UpdatedStatementsMap =
			new HashMap<Attribute, Set<UpdateStatement>>();
		
		for (UpdateStatement statement : statements){
			for (Attribute attribute : 
				statement.getUpdatedAttributes().keySet()) {
				if (!attributes2UpdatedStatementsMap.containsKey(attribute)) {
					attributes2UpdatedStatementsMap.put(attribute,
							new HashSet<UpdateStatement>());
				}
				Set<UpdateStatement> statementsPerAttribute = 
					attributes2UpdatedStatementsMap.get(attribute);
				statementsPerAttribute.add(statement);
			}
		}
		return attributes2UpdatedStatementsMap;
	}

	private Map<RelationName, Set<ForeignKeyConstraint>> getForeignKeys(
		List<UpdateStatement> statements, ConnectedDB database) {
		
		DatabaseMetaData metaData = null;
		try {
			metaData = database.connection().getMetaData();
		} catch (SQLException exception) {
			throw new D2RQException(exception);
		}
		
		Map<RelationName, Set<ForeignKeyConstraint>> foreignKeys = 
			new HashMap<RelationName, Set<ForeignKeyConstraint>>();
		
		for(UpdateStatement statement : statements) {
			RelationName table = statement.getTable();
			if (!foreignKeys.containsKey(table)) {
				foreignKeys.put(table, 
						getForeignKeys(metaData, database, table));
			}
		}
		return foreignKeys;
	}

	/**
     * @see de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector#foreignKeys(RelationName, int)
	 */
	private Set<ForeignKeyConstraint> getForeignKeys(DatabaseMetaData metaData,
		ConnectedDB database, RelationName table) {
		Set<ForeignKeyConstraint> foreignKeys = 
			new HashSet<ForeignKeyConstraint>();
		
		try {
			ResultSet resultSet = 
				metaData.getImportedKeys(null, getSchemaName(database, table), 
										       getTableName(table));
			while (resultSet.next()) {
				RelationName referencedTable = getRelationName(database,
						resultSet.getString("PKTABLE_SCHEM"), 
						resultSet.getString("PKTABLE_NAME"));
				Attribute referencedColumn = new Attribute(referencedTable, 
						resultSet.getString("PKCOLUMN_NAME"));
				
				RelationName referencingTable = getRelationName(database,
						resultSet.getString("FKTABLE_SCHEM"), 
						resultSet.getString("FKTABLE_NAME"));
				
				Attribute referencingColumn = new Attribute(referencingTable, 
						resultSet.getString("FKCOLUMN_NAME"));
				foreignKeys.add(new ForeignKeyConstraint(referencingColumn, 
						referencedColumn));
			}
			resultSet.close();
		} catch (SQLException exception) {
			new D2RQException(exception);
		}
		return foreignKeys;
	}

	
	/**
	 * @see de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector#toRelationName
	 */
	private RelationName getRelationName(ConnectedDB database, String schema, 
		String table) {
		
		if (database.dbTypeIs(ConnectedDB.PostgreSQL) && 
				"public".equals(schema)) {
			// Table in PostgreSQL default schema -- call the table "foo", 
			// not "public.foo"
			return new RelationName(null, table, 
					database.lowerCaseTableNames());
		}
		return new RelationName(schema, table, database.lowerCaseTableNames());
	}

	/**
	 * @see de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector#tableName
	 */
	private String getTableName(RelationName table) {
		return table.tableName();
	}

	/**
	 * @see de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector#schemaName
	 */
	private String getSchemaName(ConnectedDB database, RelationName table) {
		if (database.dbTypeIs(ConnectedDB.PostgreSQL) && 
				table.schemaName() == null) {
			// The default schema is known as "public" in PostgreSQL 
			return "public";
		}
		return table.schemaName();
	}

	public void clear(){
		statements.clear();
	}
}
