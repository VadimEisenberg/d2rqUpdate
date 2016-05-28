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

import il.ac.technion.cs.d2rqUpdate.SQLUpdateExecutionEngine.ExecutionEngine;
import il.ac.technion.cs.d2rqUpdate.SQLUpdateExecutionEngine.UpdateStatement;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.Token;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import de.fuberlin.wiwiss.d2rq.D2RQException;
import de.fuberlin.wiwiss.d2rq.algebra.AliasMap;
import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.Join;
import de.fuberlin.wiwiss.d2rq.algebra.MutableRelation;
import de.fuberlin.wiwiss.d2rq.algebra.Relation;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.algebra.TripleRelation;
import de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector;
import de.fuberlin.wiwiss.d2rq.expr.Expression;
import de.fuberlin.wiwiss.d2rq.nodes.NodeMaker;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */
public abstract class AbstractTripleUpdater {
	static Log log = LogFactory.getLog(AbstractTripleUpdater.class);
	
	private static final int RELATION_TOKEN_INDEX = 0;
	private static final int ATTRIBUTE_TOKEN_NUMBER = 2;
	
	// this class is used to return multiple values from the method 
		// 'extractValues'
		/**
		 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
		 *
		 */
		protected static class ExtractValuesInfo {
			public static final ExtractValuesInfo FAILED = new
				ExtractValuesInfo();
			@Override
			public String toString() {
				return "ExtractValuesInfo [aliases=" + aliases + ", database=" + 
						database + ", joins="
						+ joins + ", objectValues=" + objectValues
						+ ", projections=" + projections + ", subjectValues="
						+ subjectValues + ", succeded=" + succeded + "]";
			}

			@Override
			public int hashCode() {
				final int prime = 31;
				int result = 1;
				result =
						prime * result
								+ ((aliases == null) ? 0 : aliases.hashCode());
				result =
						prime
								* result
								+ ((database == null) ? 0 : database.hashCode());
				result =
						prime * result
								+ ((joins == null) ? 0 : joins.hashCode());
				result =
						prime
								* result
								+ ((objectValues == null) ? 0 : objectValues
										.hashCode());
				result =
						prime
								* result
								+ ((projections == null) ? 0 : projections
										.hashCode());
				result =
						prime
								* result
								+ ((subjectValues == null) ? 0 : subjectValues
										.hashCode());
				result = prime * result + (succeded ? 1231 : 1237);
				return result;
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
				ExtractValuesInfo other = (ExtractValuesInfo) obj;
				if (aliases == null) {
					if (other.aliases != null) {
						return false;
					}
				} else if (!aliases.equals(other.aliases)) {
					return false;
				}
			
				if (database == null) {
					if (other.database != null) {
						return false;
					}
				} else if (!database.equals(other.database)) {
					return false;
				}
				if (joins == null) {
					if (other.joins != null) {
						return false;
					}
				} else if (!joins.equals(other.joins)) {
					return false;
				}
				if (objectValues == null) {
					if (other.objectValues != null) {
						return false;
					}
				} else if (!objectValues.equals(other.objectValues)) {
					return false;
				}
				if (projections == null) {
					if (other.projections != null) {
						return false;
					}
				} else if (!projections.equals(other.projections)) {
					return false;
				}
				if (subjectValues == null) {
					if (other.subjectValues != null) {
						return false;
					}
				} else if (!subjectValues.equals(other.subjectValues)) {
					return false;
				}
				if (succeded != other.succeded) {
					return false;
				}
				return true;
			}

			private final boolean succeded;
			private final ConnectedDB database;
			private final Set<Join> joins;
			private final Map<Attribute, String> subjectValues;
			private final Map<Attribute, String> objectValues;
			private final Set<Attribute> projections;
			private final AliasMap aliases;
			
			public Set<Attribute> getProjections() {
				return projections;
			}

			public Map<Attribute, String> getSubjectValues() {
				return subjectValues;
			}
	
			public Map<Attribute, String> getObjectValues() {
				return objectValues;
			}
		
			private ExtractValuesInfo() {
				this.succeded = false;
				this.database = null;
				this.subjectValues = Collections.emptyMap();
				this.objectValues = Collections.emptyMap();
				this.joins = Collections.emptySet();
				this.projections = Collections.emptySet();
				this.aliases = AliasMap.NO_ALIASES;
			}
			
			public AliasMap getAliases() {
				return aliases;
			}

			public ExtractValuesInfo(ConnectedDB database,
					Expression condition,
					Map<Attribute, String> subjectValues,
					Map<Attribute, String> objectValues,
					Set<Join> joins, Set<Attribute> projections,
					AliasMap aliases) {
				this.succeded = true;
				this.database = database;
				this.subjectValues = subjectValues;
				this.objectValues = objectValues;
				this.joins = joins;
				this.projections = projections;
				this.aliases = aliases;
			}
			
			public boolean isSucceded() {
				return succeded;
			}
			public ConnectedDB getDatabase() {
				return database;
			}


			public Set<Join> getJoins() {
				return joins;
			}	
		}

	protected static class SubjectValuesTablePair {
			@Override
			public int hashCode() { // automatically generated
				final int prime = 31;
				int result = 1;
				result = prime * result + 
					((subjectValues == null) ? 0 : subjectValues.hashCode());
				result = prime * result + 
					((table == null) ? 0 : table.hashCode());
				return result;
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
				SubjectValuesTablePair other = (SubjectValuesTablePair) obj;
				if (subjectValues == null) {
					if (other.subjectValues != null) {
						return false;
					}
				} else if (!subjectValues.equals(other.subjectValues)) {
					return false;
				}
				if (table == null) {
					if (other.table != null) {
						return false;
					}
				} else if (!table.equals(other.table)) {
					return false;
				}
				return true;
			}
		
			public Map<Attribute, String> getSubjectValues() {
				return subjectValues;
			}
		
			public RelationName getTable() {
				return table;
			}
			public SubjectValuesTablePair(Map<Attribute, String> map, 
				RelationName table) {
				this.subjectValues = map;
				this.table = table;
			}
		
			private final Map<Attribute, String> subjectValues;
			private final RelationName table;
		}

	protected final Collection<TripleRelation> propertyBridges;
	
	public AbstractTripleUpdater(Collection<TripleRelation> propertyBridges) {
		this.propertyBridges = propertyBridges;
	}
	
	protected static class ObjectValuesProjectionsPair {
		public ObjectValuesProjectionsPair(Map<Attribute, String> objectValues,
				Set<Attribute> projections) {
			this.objectValues = objectValues;
			this.projections = projections;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;

			result =
					prime
							* result
							+ ((objectValues == null) ? 0 : objectValues
									.hashCode());
			result =
					prime
							* result
							+ ((projections == null) ? 0 : projections
									.hashCode());
			return result;
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
			ObjectValuesProjectionsPair other =
					(ObjectValuesProjectionsPair) obj;
		
			if (objectValues == null) {
				if (other.objectValues != null) {
					return false;
				}
			} else if (!objectValues.equals(other.objectValues)) {
				return false;
			}
			if (projections == null) {
				if (other.projections != null) {
					return false;
				}
			} else if (!projections.equals(other.projections)) {
				return false;
			}
			return true;
		}
		public Map<Attribute, String> getObjectValues() {
			return objectValues;
		}
		public Set<Attribute> getProjections() {
			return projections;
		}
		private final Map<Attribute, String> objectValues;
		private final Set<Attribute> projections;
	}
	
	/**
	 * @param map2Scalar
	 * @return
	 */
	protected static <T,U> Map<T, List<U>> 
		convertMap2Scalar_2_Map2List(Map<T, U> map2Scalar) {
		Map<T, List<U>> map2List = 
			new HashMap<T, List<U>>(map2Scalar.size());
		for(T key : map2Scalar.keySet()) {
			map2List.put(key, 
					java.util.Collections.singletonList(map2Scalar.get(key)));
		}
		return map2List;
	}

	public abstract void execute();

	@SuppressWarnings("unchecked")
	protected boolean doesCorrespondingRowAlreadyExist(ConnectedDB database,
		RelationName table, Map<Attribute, String> subjectValues, 
		Connection connection) {
		DatabaseSchemaInspector inspector =
			new DatabaseSchemaInspector(database);
		
		Set<Attribute> primaryKeys = 
			new HashSet<Attribute>(inspector.primaryKeyColumns(table));
		
		log.debug("the primary keys of table " + table + " are " + primaryKeys);
		
		Map<Attribute, String> valuesForPrimaryKeys = 
			new HashMap<Attribute, String>(subjectValues.size());
		
		for(Attribute primaryKey : primaryKeys) {
			if (subjectValues.containsKey(primaryKey)) {
				valuesForPrimaryKeys.put(primaryKey,
						subjectValues.get(primaryKey));
			}
		}
		
		log.debug("the subjectValues are " + subjectValues);
		log.debug("the valuesForPrimaryKeys are " + valuesForPrimaryKeys);
		
			String selectSQLString =
					new SelectStatementBuilder(table, database, primaryKeys,
							valuesForPrimaryKeys).getSQLStatement()
							+ " LIMIT 1";
			log.debug("select sqlString = " + selectSQLString);
		
			try {
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(selectSQLString);
				return resultSet.next();
			} catch (SQLException exception) {
				throw new D2RQException(exception.getMessage() + ": "
						+ selectSQLString);
			}
		}

	protected void insertNewRow(ExecutionEngine engine, ConnectedDB database, 
		RelationName table, Map<Attribute, String> values, 
		Map<Attribute, String> subjectValues, Connection connection) {
		
			String sqlString =
					new InsertStatementBuilder(table, database, values)
							.getSQLStatement();
			
			log.debug("insert sqlString = " + sqlString);
			engine.add(new UpdateStatement(database, table, 
					values, sqlString, 1,
					UpdateStatement.Type.INSERT, subjectValues));
		}

	/**
	 * @param valuesAttributesThatAreNotInCondition
	 * @return
	 */
	protected Set<RelationName> getMentionedTables(Set<String> attributes) {
		Set<RelationName> relations = new HashSet<RelationName>();
		
		for (String attribute : attributes) {
			relations
					.add(getRelationNameFromQuotedQualifiedAttribute(attribute));
		}
	
		return relations;
	}

	/**
	 * @param subject 
	 * @param object 
	 * @param info
	 * @param extractedValuesToFix
	 */
	protected Set<ExtractValuesInfo> 
		splitMultipleTables(ExtractValuesInfo info) {
		
		Set<RelationName> tables = getMentionedTables(info);
		if (tables.size() < 2) {
			return Collections.singleton(info);
		}
		
		Set<ExtractValuesInfo> infosPerTables = new HashSet<ExtractValuesInfo>();
		
		
		// 1. Group the subject, object and condition values per table, 
		// taking joins into account
		// 2. For a table, that has subject values empty - make the objects 
		// values be the subject ones (that means that the object values will be
		// inserted as subjects
		
		
		for (RelationName table : tables) {
			
			ExtractValuesInfo infoPerTable = filterInfoByTable(info, table);
			if (infoPerTable.isSucceded()) {
				infosPerTables.add(infoPerTable);
			}
		}
		
		return infosPerTables;
	}

	/**
	 * @param info 
	 * @param table
	 * @return
	 */
	private ExtractValuesInfo filterInfoByTable(ExtractValuesInfo info, 
		RelationName table) {
			
		Map<Attribute, String> subjectValues = 
			getValuesByTable(info.getSubjectValues(), info, table);
		
		Map<Attribute, String> objectValues = 
			getValuesByTable(info.getObjectValues(), info, table);
	
		boolean emptySubjectsOrObjectsAllowed = 
			allowEmptySubjectsOrObjectsWhileHandlingJoins(); 
		
		if (!emptySubjectsOrObjectsAllowed) {
			// check if the objects or subjects will become empty after
			// filtering by common condition
			Map<Attribute, String> subjectValuesToFilter = 
				new HashMap<Attribute, String>(subjectValues);
			Map<Attribute, String> objectValuesToFilter = 
				new HashMap<Attribute, String>(objectValues);
			
			filterOutByConditionAttributes(info, subjectValuesToFilter, 
					objectValuesToFilter);
			
			if (subjectValuesToFilter.isEmpty() ||  
					objectValuesToFilter.isEmpty()) {
				return ExtractValuesInfo.FAILED;
			}
		}
		
		if (subjectValues.isEmpty()) {
			subjectValues = new HashMap<Attribute, String>(objectValues);
		}
		
		if (objectValues.isEmpty()) {
			objectValues = new HashMap<Attribute, String>(subjectValues);
		}
		
		if (subjectValues.isEmpty() && objectValues.isEmpty()) {
			return ExtractValuesInfo.FAILED;
		}
		
		Set<Attribute> projections = 
			getValuesByTable(info.getProjections(), info, table);
	
		
		ExtractValuesInfo infoForTheTable =
			new ExtractValuesInfo(info.getDatabase(), 
					Expression.TRUE, 
					subjectValues, objectValues, Collections.<Join>emptySet(),
					projections,info.getAliases());
		log.debug("info for the table " + table + " :" + infoForTheTable);
		
		return infoForTheTable;
	}

	/**
	 * @return
	 */
	abstract protected boolean allowEmptySubjectsOrObjectsWhileHandlingJoins();

	private Map<Attribute, String> getValuesByTable(Map<Attribute, String> map, 
			ExtractValuesInfo info,
		RelationName table) {
		Map<Attribute, String> valuesByTable = 
			new HashMap<Attribute, String>();
		
		for (Attribute attribute : map.keySet()) {
			
			Set<Attribute> equivalenceClassOfTheAttribute = 
				getEquivalenceClassOfTheAttribute(attribute,info.getJoins(),
						info.getDatabase());
			log.debug("equivalence class of the attribute " + attribute + " is "
					+ equivalenceClassOfTheAttribute);
			
			for(Attribute attributeInTheClass : equivalenceClassOfTheAttribute){
				if (belongsToTable(attributeInTheClass,table)) {
					log.debug("adding attribute " + attributeInTheClass + 
							" with value " + map.get(attribute));
					valuesByTable.put(attributeInTheClass, 
						map.get(attribute));
				}
			}
		}
		return valuesByTable;
	}
	
	private Set<Attribute> getValuesByTable(Set<Attribute> values, 
			ExtractValuesInfo info, RelationName table) {
		Set<Attribute> valuesByTable = new HashSet<Attribute>(values.size());
		
		for (Attribute attribute : values) {
			Set<Attribute> equivalenceClassOfTheAttribute = 
				getEquivalenceClassOfTheAttribute(attribute,info.getJoins(),
						info.getDatabase());
			
			for(Attribute attributeInTheClass : equivalenceClassOfTheAttribute){
				if (belongsToTable(attributeInTheClass,table)) {
					valuesByTable.add(attributeInTheClass); 
				}
			}
		}
		return valuesByTable;
	}
	/**
	 * @param attributeInTheClass
	 * @param table
	 * @return
	 */
	private boolean belongsToTable(Attribute attribute, RelationName table) {
		return attribute.relationName().equals(table);
	}

	/**
	 * @param attribute
	 * @param database 
	 * @param set 
	 * @return
	 */
	private Set<Attribute> getEquivalenceClassOfTheAttribute(Attribute attribute, 
			Set<Join> joins, ConnectedDB database) {
		Set<Attribute> equivalenceClass = new HashSet<Attribute>();
		equivalenceClass.add(attribute);
		
		while(true) { // break at the end of the loop if no element was added
			boolean newElementAdded = false;
			
			for (Join join : joins) {
				if (join.attributes1().size() != 1 || 
						join.attributes2().size() != 1){
					log.error("joins with several attributes are currently " + 
							"unsupported");
					continue;
				}
				Attribute attribute1 = (Attribute)join.attributes1().get(0);  
				Attribute attribute2 = (Attribute)join.attributes2().get(0);
					
				if (equivalenceClass.contains(attribute1) && 
						!equivalenceClass.contains(attribute2)) {
					equivalenceClass.add(attribute2);
					newElementAdded = true;
				} else if (equivalenceClass.contains(attribute2) && 
						!equivalenceClass.contains(attribute1)) {
					equivalenceClass.add(attribute1);
					newElementAdded = true;
				}
			}
			if (!newElementAdded) {
				break;
			}
		}
				
		return equivalenceClass;
	}

	protected void filterOutByConditionAttributes(ExtractValuesInfo info,
		Map<Attribute, String> subjectValuesToFilter, 
		Map<Attribute, String> objectValuesToFilter) {
			
			// get the attribute of the condition - it appears in both subject 
			// and object values
			Set<Attribute> conditionAttributes = 
				getConditionAttributes(subjectValuesToFilter, 
						objectValuesToFilter);
			
			filterOutByConditionAttributes(subjectValuesToFilter, 
					conditionAttributes);
			filterOutByConditionAttributes(objectValuesToFilter, 
					conditionAttributes);
		}

	/**
	 * remove the attributes from valuesToFilter, that are in condition and do
	 * not pertain to the table of the valuesToFilter without condition
	 * @param filteredSubjectValues
	 * @param filteredObjectValues
	 * @param conditionAttributes 
	 */
	private void filterOutByConditionAttributes(Map<Attribute, String> 
		subjectValuesToFilter, 
		Set<Attribute> conditionAttributes) {
		
		Set<Attribute> valuesAttributesThatAreNotInCondition =
			new HashSet<Attribute>(subjectValuesToFilter.keySet());
		valuesAttributesThatAreNotInCondition.removeAll(conditionAttributes);
		
	
		for (Attribute conditionAttribute : conditionAttributes) { 		
			subjectValuesToFilter.remove(conditionAttribute);
		}
	}

	/**
	 * @param subjectValuesToFilter
	 * @param objectValuesToFilter
	 * @return
	 */
	private Set<Attribute> 
		getConditionAttributes(Map<Attribute, String> subjectValuesToFilter, 
			Map<Attribute, String> objectValuesToFilter) {
		
		Set<Attribute> conditionAttributes = 
			new HashSet<Attribute>(subjectValuesToFilter.keySet());
		conditionAttributes.retainAll(objectValuesToFilter.keySet());
		return conditionAttributes;
	}




	

	/**
	 * @param extractedValues
	 * @return
	 */
	protected Set<ExtractValuesInfo> 
	splitMultipleTables(Set<ExtractValuesInfo> extractedValues) {
		
		Set<ExtractValuesInfo> fixedExtractedValues = 
			new HashSet<ExtractValuesInfo>(extractedValues.size());
		
		for (ExtractValuesInfo info : extractedValues) {
			fixedExtractedValues.addAll(splitMultipleTables(info));
		}
		return fixedExtractedValues;
	}

	/**
	 * @param quotedQualifiedAttribute
	 * @param aliases
	 * @return
	 */
	protected String renameAttribute(String quotedQualifiedAttribute,
		AliasMap aliases, ConnectedDB database) {
		RelationName table = 
			getRelationNameFromQuotedQualifiedAttribute(
					quotedQualifiedAttribute);
		String attributeName = 
			getAttributeNameFromQuotedQualifiedAttribute(
					quotedQualifiedAttribute);
		
		Attribute renamedAttribute = 
			new Attribute(renameTable(table, aliases), attributeName);
		
		return renamedAttribute.toSQL(database, AliasMap.NO_ALIASES);
	}

	protected RelationName renameTable(RelationName table, AliasMap aliases) {
		if( aliases.isAlias(table)) {
			table = aliases.originalOf(table);
		}
		return table;
	}

	/**
	 * @param projections
	 * @param aliases
	 * @param database 
	 * @return
	 */
	protected Set<Attribute> renameAttributes(Set<Attribute> attributes, 
			AliasMap aliases, ConnectedDB database) {
		Set<Attribute> renamedAttributes = new HashSet<Attribute>();
		
		for (Attribute attribute : attributes) {
			Attribute renamedAttribute = renameAttribute(aliases, attribute);
			renamedAttributes.add(renamedAttribute);
		}
		return renamedAttributes;
	}

	private Attribute renameAttribute(AliasMap aliases, Attribute attribute) {
		RelationName renamedTable = 
			renameTable(attribute.relationName(), aliases);
		Attribute renamedAttribute = 
			new Attribute(renamedTable, attribute.attributeName());
		return renamedAttribute;
	}

	/**
	 * @param subjectValues
	 * @param aliases
	 * @return
	 */
	protected Map<Attribute, String> renameValues(Map<Attribute, String> map, 
			AliasMap aliases) {
		Map<Attribute, String> renamedValues = new HashMap<Attribute, String>();
		
		for (Attribute attribute : map.keySet()) {
			Attribute renamedAttribute = renameAttribute(aliases, attribute);
			renamedValues.put(renamedAttribute, map.get(attribute));
		}
		return renamedValues;
	}

	/**
	 * @param objectValuesProjectionsPairs
	 * @return
	 */
	protected Map<Attribute, String> 
		getObjectValues(Set<ObjectValuesProjectionsPair> 
			objectValuesProjectionsPairs) {
		Map<Attribute, String> objectValues = new HashMap<Attribute, String>();
		for (ObjectValuesProjectionsPair pair : objectValuesProjectionsPairs) {
			objectValues.putAll(pair.getObjectValues());
		}
		
		return objectValues;
	}

	protected Map<Attribute, String> 
		collectObjectValues(Set<ExtractValuesInfo> extractValuesInfos) {
		Map<Attribute, String> objectValues = 
			new HashMap<Attribute,String>();
		
		for(ExtractValuesInfo info : extractValuesInfos) {
			objectValues.putAll(info.getObjectValues());
		}
		return objectValues;
	}

	protected static Attribute makeAttributeFromQuotedQualifiedName(
		String qualifiedAttributeName) {
		return new Attribute(
				getRelationNameFromQuotedQualifiedAttribute(
						qualifiedAttributeName),
				getAttributeNameFromQuotedQualifiedAttribute(
						qualifiedAttributeName));
	}

	private static String getAttributeNameFromQuotedQualifiedAttribute(
		String qualifiedAttributeName) {
		return getUnquotedTokenFromQuotedQualifiedAttribute(
				qualifiedAttributeName, ATTRIBUTE_TOKEN_NUMBER);
	}


	
	public static Set<ExtractValuesInfo> 
		extractValues(Collection<Triple> triples, 
			Collection<TripleRelation> tripleRelations) {
		Set<ExtractValuesInfo> extractedValues = 
			new HashSet<ExtractValuesInfo>();
		
		Map<Node, HashMap<Attribute,String>> blankNodeSubjectValues = 
			new HashMap<Node, HashMap<Attribute,String>> ();
		
		Map<Node, HashMap<Attribute,String>> blankNodeObjectValues = 
			new HashMap<Node, HashMap<Attribute,String>> ();
		
		Map<Node, Relation> blankNodeSideEffectsForSubject = new 
			HashMap<Node, Relation> ();
		
		for (Triple triple : triples) {
			if (triple.getSubject().isBlank() || triple.getObject().isBlank()) {
				fillMapsForBlankNodes(triple, tripleRelations,
						extractedValues,blankNodeSubjectValues,
						blankNodeObjectValues, blankNodeSideEffectsForSubject);
			} else {
				extractValuesForNonBlankNodes(triple, tripleRelations,
						extractedValues);
			}
		}
		
		extractValuesFromBlankNodeMaps(blankNodeSubjectValues,
				blankNodeObjectValues,blankNodeSideEffectsForSubject,
				extractedValues);
		log.debug("extractedValues = " + extractedValues);
		return extractedValues;
	}
	
	/**
	 * @param blankNodeSubjectValues
	 * @param blankNodeObjectValues
	 * @param blankNodeSideEffectsForSubject
	 * @param extractedValues 
	 */
	@SuppressWarnings("unchecked")
	private static void extractValuesFromBlankNodeMaps(
		Map<Node, HashMap<Attribute, String>> blankNodeSubjectValues,
		Map<Node, HashMap<Attribute, String>> blankNodeObjectValues,
		Map<Node, Relation> blankNodeSideEffectsForSubject,
		Set<ExtractValuesInfo> extractedValues) {
		
		for (Node blankNode : blankNodeSubjectValues.keySet()) {
			HashMap<Attribute, String> subjectValues = 
				blankNodeSubjectValues.get(blankNode);
			HashMap<Attribute, String> objectValues = 
				blankNodeObjectValues.get(blankNode);
			Relation sideEffectsForSubject = 
				blankNodeSideEffectsForSubject.get(blankNode);
			
			log.debug("Exctracted values for blank node" + blankNode +
					" subjectValues = " + subjectValues +
					" objectValues = " + objectValues);
			
			extractedValues.add(new ExtractValuesInfo(	
					sideEffectsForSubject.database(),
					sideEffectsForSubject.condition(),
					subjectValues,objectValues,
					sideEffectsForSubject.joinConditions(),
					sideEffectsForSubject.projections(),
					sideEffectsForSubject.aliases()));
		}
		
	}
	
	/**
	 * @param triple
	 * @param tripleRelations
	 * @param extractedValues
	 * @param blankNodeSideEffectsForSubject 
	 * @param blankNodeObjectValues 
	 * @param blankNodeSubjectValues 
	 */
	private static void fillMapsForBlankNodes(Triple triple,
		Collection<TripleRelation> tripleRelations,
		Set<ExtractValuesInfo> extractedValues, 
		Map<Node, HashMap<Attribute, String>> blankNodeSubjectValues,
		Map<Node, HashMap<Attribute, String>> blankNodeObjectValues, 
		Map<Node, Relation> blankNodeSideEffectsForSubject) {
				
		HashMap<Attribute,String> subjectValues = 
			new HashMap<Attribute,String>(1);
		HashMap<Attribute,String> objectValues = 
			new HashMap<Attribute,String>(1);
		
		if (triple.getSubject().isBlank() && triple.getObject().isBlank()) {
			log.error("Unable to handle both subject and object blank nodes " +
					"in a triple");
			return;
		}
		
		Node blankNode = triple.getSubject().isBlank() ? triple.getSubject() :
			(triple.getObject().isBlank() ? triple.getObject() : null);
		
		if (blankNode == null){
			return;
		}

		log.debug("blank node = " + blankNode + " blanknodeid "
				+ blankNode.getBlankNodeId() + " class "
				+ blankNode.getClass().getCanonicalName());

		for (TripleRelation tripleRelation : tripleRelations) {
			
			MutableRelation sideEffectsForPredicate =
				getSideEffects(tripleRelation, TripleRelation.PREDICATE,
						triple.getPredicate());

			if (sideEffectsForPredicate == null) {
				continue;
			}
		
			MutableRelation sideEffectsForSubject =
					getSideEffects(tripleRelation, TripleRelation.SUBJECT,
							triple.getSubject());

			if (sideEffectsForSubject != null) {
				extractValues(sideEffectsForSubject, tripleRelation
						.baseRelation().database(), subjectValues);

				log.debug("subjectValues = " + subjectValues);
				blankNodeSubjectValues.put(blankNode, subjectValues);
				blankNodeSideEffectsForSubject.put(blankNode,
						sideEffectsForSubject.immutableSnapshot());
			}
			
			MutableRelation sideEffectsForObject =
					getSideEffects(tripleRelation, TripleRelation.OBJECT,
							triple.getObject());
			if (sideEffectsForObject != null) {
				extractValues(sideEffectsForObject, tripleRelation
						.baseRelation().database(), objectValues);
				log.debug("objectValues = " + objectValues);
				blankNodeObjectValues.put(blankNode, objectValues);
			}

		}
	}
	

	private static void extractValuesForNonBlankNodes(Triple triple,
		Collection<TripleRelation> tripleRelations,
		Set<ExtractValuesInfo> extractedValues) {
		for (TripleRelation tripleRelation : tripleRelations) {
			ExtractValuesInfo info = extractValues(triple, tripleRelation);
			if (info.isSucceded()) {
				extractedValues.add(info);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static Set<RelationName> getMentionedTables(ExtractValuesInfo info) {
	
		Set<RelationName> relations = new HashSet<RelationName>();
		
		Set<Attribute> attributesToCheck = new HashSet<Attribute>(); 
		
		for (Join join : info.getJoins()){
			attributesToCheck.addAll(join.attributes1());
			attributesToCheck.addAll(join.attributes2());
		}
		
		for(Attribute attribute : attributesToCheck){
			relations.add(attribute.relationName());
		}
		
		for(Attribute attribute : info.getSubjectValues().keySet()) {
			relations.add(attribute.relationName());
		}
		for(Attribute attribute : info.getObjectValues().keySet()) {
			relations.add(attribute.relationName());
		}
		
		return relations;
	}
	
	/**
	 * @param expression
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected
	static Set<RelationName> getMentionedTables(Expression expression) {
		
		Set<RelationName> relations = new HashSet<RelationName>();
		for(Attribute attribute : (Set<Attribute>)expression.attributes()){
			relations.add(attribute.relationName());
		}
		return relations;
	}

	/**
	 * @param attributeString
	 * @return
	 */
	protected static RelationName 
		getRelationNameFromQuotedQualifiedAttribute(String qualifiedAttribute) {
		return new RelationName(null,
				getUnquotedTokenFromQuotedQualifiedAttribute(qualifiedAttribute,
						RELATION_TOKEN_INDEX));
	}
	
	private static String 
		getUnquotedTokenFromQuotedQualifiedAttribute(
			String quotedQualifiedAttribute,
			int tokenIndex){ 
			Token[] tokens = getTokens(quotedQualifiedAttribute);
			if (tokens.length != 3 || !tokens[1].toString().equals(".")) {
				throw new D2RQException("Attribute \"" + 
						quotedQualifiedAttribute + 
						"\" is not in \"table.column\" notation",
						D2RQException.SQL_INVALID_ATTRIBUTENAME);
			}
			String tokenString = tokens[tokenIndex].toString();
			// unquote
			return tokenString.substring(1, tokenString.length() - 1);
	}


	
	@SuppressWarnings("unchecked")
	static ExtractValuesInfo extractValues(Triple triple, 
		TripleRelation tripleRelation) {
		
		log.debug("extracting values for triple " + triple + 
				" tripleRelation = " + tripleRelation);
		Map<Attribute,String> subjectValues = new HashMap<Attribute,String>(1);
		Map<Attribute,String> objectValues = new HashMap<Attribute,String>(1);
		
		MutableRelation sideEffectsForPredicate = 
			getSideEffects(tripleRelation, TripleRelation.PREDICATE,
						   triple.getPredicate());
		
		if (sideEffectsForPredicate == null){
			return ExtractValuesInfo.FAILED;
		}
		MutableRelation sideEffectsForSubject = 
			getSideEffects(tripleRelation, TripleRelation.SUBJECT,
						   triple.getSubject());
		
		if (sideEffectsForSubject == null){
			return ExtractValuesInfo.FAILED;
		}
	
		MutableRelation sideEffectsForObject = 
			getSideEffects(tripleRelation, TripleRelation.OBJECT,
						   triple.getObject());
		
		if (sideEffectsForObject == null){
			log.debug("sideEffectsForObject is empty");
			return ExtractValuesInfo.FAILED;
		}
		
		extractValues(sideEffectsForSubject, 
					  tripleRelation.baseRelation().database(), 
					  subjectValues);
		
		extractValues(sideEffectsForObject, 
				  tripleRelation.baseRelation().database(), 
				  objectValues);
		
		Relation immutableSnapshotOfSideEffectsForSubject =
			sideEffectsForSubject.immutableSnapshot();
		
		return new ExtractValuesInfo(	
				immutableSnapshotOfSideEffectsForSubject.database(),
				immutableSnapshotOfSideEffectsForSubject.condition(),
				subjectValues,objectValues,
				immutableSnapshotOfSideEffectsForSubject.joinConditions(),
				immutableSnapshotOfSideEffectsForSubject.projections(),
				immutableSnapshotOfSideEffectsForSubject.aliases());
	}

	static MutableRelation getSideEffects(TripleRelation tripleRelation, 
		String partOfATriple, Node nodeOfATriple) {
		MutableRelation sideEffects = 
			new MutableRelation(tripleRelation.baseRelation());
		
		NodeMaker nodeMaker = 
			tripleRelation.nodeMaker(partOfATriple).selectNode(nodeOfATriple, 
		                		    					   	   sideEffects);
		if(nodeMaker.equals(NodeMaker.EMPTY)) {
			return null;
		}
		
		return sideEffects;
	}

	static void extractValues(MutableRelation relation, ConnectedDB database, 
		Map<Attribute, String> values) {
		// Currently the extraction is done by parsing the SQL string.
		// Once D2RQ relation`s interface is changed to provide more accessor 
		// methods to the condition of the relation, a more reasonable approach
		// could be applied.
		
		Relation immutableSnapshotOfRelation = relation.immutableSnapshot();
		String sqlString = 
			immutableSnapshotOfRelation.condition().toSQL(database, 
					immutableSnapshotOfRelation.aliases());
		
		log.debug("sqlString = " + sqlString);
		
		Token[] tokens = getTokens(sqlString);
		for (int tokenIndex = 0; tokenIndex < tokens.length; tokenIndex++) {
			Token token = tokens[tokenIndex];
			if (token != null && token.toString().equals("=")) {
				String attributeString = 
					getLeftPartOfEquality(tokens, tokenIndex);
				Attribute attribute = 
					makeAttributeFromQuotedQualifiedName(attributeString);
				values.put(attribute, 
						getRightPartOfEquality(tokens, tokenIndex));
			}
		}
	}

	private static String getRightPartOfEquality(Token[] tokens,
		int equalityTokenIndex) {
		String theRightPartOfTheEquality = 
			equalityTokenIndex < tokens.length - 1 ?
				tokens[equalityTokenIndex+1].toString() : "";
		if (equalityTokenIndex < tokens.length-3) {
			Token oneAfterNextToken = tokens[equalityTokenIndex + 2];
			if (oneAfterNextToken.toString().equals(".")) {
				theRightPartOfTheEquality = theRightPartOfTheEquality + 
				tokens[equalityTokenIndex + 2].toString()
				+ tokens[equalityTokenIndex + 3].toString();
			}
		}
		if ((theRightPartOfTheEquality.equals("TIMESTAMP") || 
			 theRightPartOfTheEquality.equals("DATE")) &&
			 (equalityTokenIndex < tokens.length-2)) {
			theRightPartOfTheEquality += tokens[equalityTokenIndex + 2];
		}
		return theRightPartOfTheEquality;
		
	}

	private static String getLeftPartOfEquality(Token[] tokens,
		int equalityTokenIndex) {
		String theLeftPartOfTheEquality = equalityTokenIndex >= 1 ? 
				tokens[equalityTokenIndex-1].toString() : "";

		if (equalityTokenIndex >= 3) {
			Token oneBeforePreviousToken = tokens[equalityTokenIndex-2];
			if (oneBeforePreviousToken.toString() == ".") {
				theLeftPartOfTheEquality = 
					tokens[equalityTokenIndex-3].toString() + 
				tokens[equalityTokenIndex-2].toString() + 
				theLeftPartOfTheEquality;
			}
		}
		return theLeftPartOfTheEquality;
	}

	private static Token[] getTokens(String sqlString) {
		CCJSqlParser parser = 
			new CCJSqlParser(new StringReader(sqlString));
		
		List<Token> tokensList = new ArrayList<Token>();
		while (true){
			Token token = parser.getNextToken();
			if (token != null) {
				tokensList.add(token);
			}
			
			if (token == null || token.endColumn == sqlString.length()) {
				break;
			}
		}
		
		return tokensList.toArray(new Token[0]);
	}

	protected static RelationName 
	getSingleTable(Set<RelationName> relations) {
		
		if(relations.size() != 1) {
			throw new RuntimeException("the number of relations mentioned in " +
					"the expression is not 1: " + relations);	
		}
		
		Iterator<RelationName> iterator = relations.iterator();
		return iterator.next();
	}

	/**
	 * @param joins
	 * @return
	 */
	protected static Set<RelationName> 
		getMentionedTables(Map<String,String>... attributeValueMaps) {
		Set<RelationName> relations = new HashSet<RelationName>();
		for (Map<String, String> attributeValueMap : attributeValueMaps) {
			for (String attribute : attributeValueMap.keySet()){
				relations.add(getRelationNameFromQuotedQualifiedAttribute(
						attribute));
			}
		}
		return relations;
		
	}

}