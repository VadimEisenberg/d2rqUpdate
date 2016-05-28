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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import de.fuberlin.wiwiss.d2rq.D2RQException;
import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.algebra.TripleRelation;
import de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 *
 */
public class TripleDeleteAddUpdater extends AbstractTripleUpdater {
	private static class SubjectPropertyPair {
		@Override
		public String toString() {
			return "SubjectPropertyPair [property=" + property + ", subject="
					+ subject + "]";
		}
		
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result =
					prime * result
							+ ((property == null) ? 0 : property.hashCode());
			result =
					prime * result
							+ ((subject == null) ? 0 : subject.hashCode());
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
			SubjectPropertyPair other = (SubjectPropertyPair) obj;
			if (property == null) {
				if (other.property != null) {
					return false;
				}
			} else if (!property.equals(other.property)) {
				return false;
			}
			if (subject == null) {
				if (other.subject != null) {
					return false;
				}
			} else if (!subject.equals(other.subject)) {
				return false;
			}
			
			return true;
		}
		public SubjectPropertyPair(Node subject, Node property) {
			this.subject = subject;
			this.property = property;
			
		}
		private final Node subject;
		private final Node property;
	
	}
	
	static private Log log = LogFactory.getLog(TripleDeleteAddUpdater.class);
	private final Collection<Triple> triplesToDelete;
	private final Collection<Triple> triplesToAdd;
	private final Set<Attribute> allMappedAttributes;
		
	public TripleDeleteAddUpdater(Collection<Triple> triplesToDelete,
		                          Collection<Triple> triplesToAdd,
			                      Collection<TripleRelation> propertyBridges,
			                      Set<Attribute> allMappedAttributes) {
		super(propertyBridges);
		this.triplesToAdd = triplesToAdd;
		this.triplesToDelete = triplesToDelete;
		this.allMappedAttributes = allMappedAttributes;
		
	}
	
	@Override
	public void execute(){
		Map<SubjectPropertyPair, Collection<Triple>> 
			subjectProperty2TriplesToDeleteMap = 
				getSubjectProperty2TriplesMap(triplesToDelete);
		
		Map<SubjectPropertyPair, Collection<Triple>> 
			subjectProperty2TriplesToAddMap = 
				getSubjectProperty2TriplesMap(triplesToAdd);
		
		
		
		// "paired" in the names of variables means that a deleted triple
		// is paired by an inserted triple with the same subject and property
		List<Triple> nonPairedTriplesToDelete = new ArrayList<Triple>();  
		List<Triple> nonPairedTriplesToAdd = new ArrayList<Triple>();
		
		sortOutPairedAndNonPairedTriples(nonPairedTriplesToDelete, 
				nonPairedTriplesToAdd,
				subjectProperty2TriplesToDeleteMap,
				subjectProperty2TriplesToAddMap);
		
		new TripleDeleter(nonPairedTriplesToDelete, 
				propertyBridges,allMappedAttributes).execute();
		
		new TripleAdder(nonPairedTriplesToAdd, 
				propertyBridges).execute();
		
		executePairedTriples(subjectProperty2TriplesToDeleteMap,
							 subjectProperty2TriplesToAddMap);
	}

	/**
	 * @param subjectProperty2TriplesToDeleteMap
	 * @param subjectProperty2TriplesToAddMap
	 */
	private void executePairedTriples(
		Map<SubjectPropertyPair, Collection<Triple>> 
			subjectProperty2TriplesToDeleteMap,
		Map<SubjectPropertyPair, Collection<Triple>> 
			subjectProperty2TriplesToAddMap) {
		
		Set<SubjectPropertyPair> pairedSubjectPropertyPairs = 
			subjectProperty2TriplesToDeleteMap.keySet();
		
		if ( ! pairedSubjectPropertyPairs.equals(
				subjectProperty2TriplesToAddMap.keySet())) {
			throw new D2RQException("keys of maps to add and to delete differ" +
					" after sort out");
		}
		
		ExecutionEngine engine = new ExecutionEngine();
		
		for(SubjectPropertyPair subjectProperty : pairedSubjectPropertyPairs) {
			
			Set<ExtractValuesInfo> extractValuesInfoToDelete =
				getExctractedValuesInfoQueue(
					subjectProperty2TriplesToDeleteMap, subjectProperty);
			
			Set<ExtractValuesInfo> extractValuesInfoToAdd =
				getExctractedValuesInfoQueue(
					subjectProperty2TriplesToAddMap, subjectProperty);
			
			if(extractValuesInfoToDelete.size() < 1) {
				return;
			}
			
			ExtractValuesInfo firstInfo = 
				extractValuesInfoToDelete.iterator().next();
			
			RelationName table = getSingleTable(getMentionedTables(firstInfo));
			
			ConnectedDB database = firstInfo.getDatabase();
			
			// should be the same since we have the same subject
			Map<Attribute, String> subjectValues = firstInfo.getSubjectValues();
			
			Map<Attribute, String> objectValuesToDelete =
					collectObjectValues(extractValuesInfoToDelete);
			Map<Attribute, String> objectValuesToAdd =
				collectObjectValues(extractValuesInfoToAdd);
			
			execute(engine, database, table, subjectValues, 
					objectValuesToDelete, objectValuesToAdd);
		}
		
		engine.execute();
	}

	/**
	 * @param engine
	 * @param database
	 * @param table
	 * @param subjectValues
	 * @param objectValuesToDelete
	 * @param objectValuesToAdd
	 */
	private void execute(ExecutionEngine engine, ConnectedDB database,
		RelationName table, Map<Attribute, String> subjectValues,
		Map<Attribute, String> objectValuesToDelete,
		Map<Attribute, String> objectValuesToAdd) {
		
		DatabaseSchemaInspector inspector =
				new DatabaseSchemaInspector(database);

		boolean tableHasPrimaryKeys =
				!inspector.primaryKeyColumns(table).isEmpty();

		if (!tableHasPrimaryKeys) {
			throw new D2RQException(table.toString() + " has no primary keys "
					+ "update not yet implemented");
		}
		
		Connection connection = database.connection();
		
		if (!doesCorrespondingRowAlreadyExist(database, table, subjectValues,
				connection)) {
			Map<Attribute, String> values = 
				new HashMap<Attribute, String>(subjectValues);
			values.putAll(objectValuesToAdd);
			insertNewRow(engine, database, table, values, subjectValues,
					connection);
			return;
		}
		
		updateExistingRow(engine, database, table, subjectValues, 
				objectValuesToDelete, objectValuesToAdd, connection);
		
	}
	
	private void updateExistingRow(ExecutionEngine engine, ConnectedDB database, 
		RelationName table, Map<Attribute, String> subjectValues,
		Map<Attribute, String> objectValuesToDelete,
		Map<Attribute, String> objectValuesToAdd,  Connection connection) {
		Map<Attribute, List<String>> whereValues =
				convertMap2Scalar_2_Map2List(subjectValues);

		for (Attribute attribute : objectValuesToAdd.keySet()) {
			whereValues.put(attribute, Arrays.asList(new String[] { "NULL",
					objectValuesToDelete.get(attribute), 
					objectValuesToAdd.get(attribute) }));
		}

		String sqlString =
				new UpdateStatementBuilder(table, database, objectValuesToAdd,
						whereValues).getSQLStatement();
		log.debug("update sqlString = " + sqlString);

		engine.add(new UpdateStatement(database, table,
				objectValuesToAdd, sqlString, 1,
				UpdateStatement.Type.UPDATE_TO_NON_NULL_VALUE, subjectValues));
	}
	
	private Set<ExtractValuesInfo> getExctractedValuesInfoQueue(
		Map<SubjectPropertyPair, Collection<Triple>> subjectProperty2TriplesMap,
		SubjectPropertyPair subjectProperty) {
		
		Collection<Triple> triplesToDelete = 
			subjectProperty2TriplesMap.get(subjectProperty);
		
		
			Set<ExtractValuesInfo> extractValues = 
				TripleUpdater.extractValues(triplesToDelete, propertyBridges);
			extractValues =
				splitMultipleTables(extractValues);
			return extractValues;
		}
	


	/**
	 * @param nonPairedTriplesToDelete
	 * @param nonPairedTriplesToAdd
	 * @param subjectProperty2TriplesToDeleteMap
	 * @param subjectProperty2TriplesToAddMap
	 */
	private void sortOutPairedAndNonPairedTriples(
		List<Triple> nonPairedTriplesToDelete,
		List<Triple> nonPairedTriplesToAdd,
		Map<SubjectPropertyPair, Collection<Triple>> 
			subjectProperty2TriplesToDeleteMap,
		Map<SubjectPropertyPair, Collection<Triple>> 
			subjectProperty2TriplesToAddMap) {

		sortOutNonPairedTriples(nonPairedTriplesToDelete,
				subjectProperty2TriplesToDeleteMap,
				subjectProperty2TriplesToAddMap);
		
		sortOutNonPairedTriples(nonPairedTriplesToAdd,
				subjectProperty2TriplesToAddMap,
				subjectProperty2TriplesToDeleteMap);
			
	}

	private void sortOutNonPairedTriples(List<Triple> nonPairedTriples,
		Map<SubjectPropertyPair, Collection<Triple>> triplesMap,
		Map<SubjectPropertyPair, Collection<Triple>> triplesPairedMap) {
		
		Set<SubjectPropertyPair> nonPairedSubjectPropertyPairs = 
			new HashSet<SubjectPropertyPair> 
				(triplesMap.keySet());
		
		nonPairedSubjectPropertyPairs.removeAll(triplesPairedMap.keySet());
		for (SubjectPropertyPair subjectPropertyPair : 
			nonPairedSubjectPropertyPairs) {
			nonPairedTriples.addAll(triplesMap.get(subjectPropertyPair));
			triplesMap.remove(subjectPropertyPair);
		}
	}

	private Map<SubjectPropertyPair, Collection<Triple>> 
		getSubjectProperty2TriplesMap(Collection<Triple> triples) {
		
		Map<SubjectPropertyPair, Collection<Triple>> 
			subjectProperty2TriplesMap =
				new HashMap<SubjectPropertyPair, 
							Collection<Triple>>();

		for (Triple triple : triples) {
			SubjectPropertyPair key = 
				new SubjectPropertyPair(triple.getSubject() , 
						triple.getPredicate());
			
			if (!subjectProperty2TriplesMap.containsKey(key)) {
				subjectProperty2TriplesMap.put(key, new HashSet<Triple>());
			}
			Collection<Triple> triplesPerSubjectAndProperty =
					subjectProperty2TriplesMap.get(key);
			triplesPerSubjectAndProperty.add(triple);

		}
		return subjectProperty2TriplesMap;
	}

	/* (non-Javadoc)
	 * @see il.ac.technion.cs.d2rqUpdate.AbstractTripleUpdater#allowEmptySubjectsOrObjectsWhileHandlingJoins()
	 */
	@Override
	protected boolean allowEmptySubjectsOrObjectsWhileHandlingJoins() {
		return false;
	}
}
