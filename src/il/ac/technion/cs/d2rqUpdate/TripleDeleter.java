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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Triple;

import de.fuberlin.wiwiss.d2rq.D2RQException;
import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.algebra.TripleRelation;
import de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * An auxiliary class for adding triples
 * 
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 */
public class TripleDeleter extends TripleUpdater {
	static private Log log = LogFactory.getLog(TripleDeleter.class);
	private final Set<Attribute> allMappedAttributes;

	public TripleDeleter(Collection<Triple> triples,
			Collection<TripleRelation> propertyBridges,
			Set<Attribute> allMappedAttributes) {
		super(triples, propertyBridges);
		this.allMappedAttributes = allMappedAttributes;
	}

	@Override
	protected void execute(ExecutionEngine engine,
		ConnectedDB database, RelationName table,
		Map<Attribute, String> subjectValues, 
		Set<ObjectValuesProjectionsPair> objectValuesProjectionsPairs) {
		
		log.debug("subjectValues = " + subjectValues);
		for (ObjectValuesProjectionsPair pair : objectValuesProjectionsPairs) {
			log.debug("object values = " + pair.getObjectValues() + 
					" projections = " + pair.getProjections());
		}
		DatabaseSchemaInspector inspector = 
			new DatabaseSchemaInspector(database);
		
		boolean tableHasPrimaryKeys = 
			!inspector.primaryKeyColumns(table).isEmpty();
		
		if (!tableHasPrimaryKeys) {
			throw new D2RQException(table.toString() + " has no primary keys " + 
					"update not yet implemented");
		}
		
		if (multipleValuesPerAttributeExist(objectValuesProjectionsPairs)) {

			for (ObjectValuesProjectionsPair pair : 
					objectValuesProjectionsPairs) {
				log.debug("object values = " + pair.getObjectValues()
						+ " projections = " + pair.getProjections());

				Map<Attribute, String> objectValues =
						getObjectValuesToDelete(pair);

				executePerSubjectsAndObjects(engine, database, table,
						subjectValues, inspector, objectValues);
			}
		} else {
			Map<Attribute, String> objectValues =
				getObjectValuesToDelete(objectValuesProjectionsPairs);
			log.debug("object values to delete = " + objectValues);
			executePerSubjectsAndObjects(engine, database, table,
				subjectValues, inspector, objectValues);
		}
		
		
		
	}


	/**
	 * @param objectValuesProjectionsPairs
	 * @param database
	 * @return
	 */
	private Map<Attribute, String> getObjectValuesToDelete(
		Set<ObjectValuesProjectionsPair> objectValuesProjectionsPairs) {
		
		Map<Attribute, String> objectValuesToDelete =
			new HashMap<Attribute, String>();
		
		for (ObjectValuesProjectionsPair pair : objectValuesProjectionsPairs){
			objectValuesToDelete.putAll(getObjectValuesToDelete(pair));
		}
		return objectValuesToDelete;
	}

	private void executePerSubjectsAndObjects(ExecutionEngine engine,
		ConnectedDB database, RelationName table,
		Map<Attribute, String> subjectValues, DatabaseSchemaInspector inspector,
		Map<Attribute, String> objectValues) {
		
		if (objectValues.size() == 0) {
			return;
		}
		
		if (shouldDeleteTheWholeRow(database, table, subjectValues,
				objectValues.keySet(), inspector)) {
		
			Map<Attribute, String> allValues =
					new HashMap<Attribute, String>(subjectValues);
			allValues.putAll(objectValues);

			executeDeleteRow(engine, database, table, allValues, subjectValues,
					inspector);
			return;
		}

		verifyNonNullablesAreNotDeleted(database, table, subjectValues,
				objectValues, inspector);
		executeUpdatesToNULL(engine, database, table, subjectValues,
				objectValues);
	}

	/**
	 * @param objectValuesProjectionsPairs
	 * @param database
	 * @return
	 */
	protected Map<Attribute, String> getObjectValuesToDelete(
		ObjectValuesProjectionsPair pair) {
		Map<Attribute, String> objectValuesToDelete =
				new HashMap<Attribute, String>();

		Map<Attribute, String> objectValues = pair.getObjectValues();

		Set<Attribute> candidateObjectsToDelete = objectValues.keySet();

		// if there are more object values to delete - delete only those
		// that participate in the projections. this will not delete
		// the column values in conditions (when the column alone does
		// not determine the value of the triple)

		// if there is only one object value to delete - delete it, since
		// (when the column alone determines the value of the triple)
		if (candidateObjectsToDelete.size() > 1) {
			candidateObjectsToDelete.retainAll(pair.getProjections());
		}

		for (Attribute object : candidateObjectsToDelete) {
			objectValuesToDelete.put(object, objectValues.get(object));
		}

		return objectValuesToDelete;
	}

	private void executeUpdatesToNULL(ExecutionEngine engine,
		ConnectedDB database, RelationName table,
		Map<Attribute, String> subjectValues, 
		Map<Attribute, String> objectValues) {
		
		Map<Attribute, List<String>> whereValues =
				convertMap2Scalar_2_Map2List(subjectValues);

		for (Attribute attribute : objectValues.keySet()) {
			Map<Attribute, List<String>> whereValuesPerDeletedAttribute =
					new HashMap<Attribute, List<String>>(whereValues);

			whereValuesPerDeletedAttribute.put(attribute, java.util.Collections
					.singletonList(objectValues.get(attribute)));

			String sqlString =
					new UpdateStatementBuilder(table, database,
							java.util.Collections.singletonMap(attribute,
									"NULL"), whereValuesPerDeletedAttribute)
							.getSQLStatement();
			log.debug("update sqlString = " + sqlString);

			
			engine.add(new UpdateStatement(database, table,		
						   java.util.Collections.singletonMap(attribute,"NULL"),
					sqlString, 0, UpdateStatement.Type.UPDATE_TO_NULL,
					subjectValues));
		}

	}

	private void executeDeleteRow(ExecutionEngine engine, ConnectedDB database,
		RelationName table, Map<Attribute, String> allValues,
		Map<Attribute, String> subjectValues,
		DatabaseSchemaInspector inspector) {
		String sqlString = 
			new DeleteStatementBuilder(table, database, 
					allValues).getSQLStatement();

		engine.add(new UpdateStatement(database, table, allValues, sqlString, 0,
				UpdateStatement.Type.DELETE, subjectValues));
	}

	/**
	 * @param database
	 * @param table
	 * @param objectValues
	 * @param objectValues2
	 * @param inspector
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean shouldDeleteTheWholeRow(ConnectedDB database,
		RelationName table, Map<Attribute, String> subjectValues,
		Set<Attribute> objectAttributes, DatabaseSchemaInspector inspector) {

		log.debug("attributesDelete = " + objectAttributes);

		// check that all the columns that are not being deleted contain
		// a NULL value or are not mapped to any property
		Map<Attribute, String> selectConditionValues =
				new HashMap<Attribute, String>(subjectValues);

		boolean allTheColumnsAreBeingDeleted = true;
		List<Attribute> columnsToCheck = inspector.listColumns(table);
		// check only columns that are mapped to some property
		columnsToCheck.retainAll(allMappedAttributes);

		for (Attribute column : columnsToCheck) {
			if (!objectAttributes.contains(column)) {
				log.debug(column + " is not deleted");
				allTheColumnsAreBeingDeleted = false;
				selectConditionValues.put(column, "NULL");
			}
		}

		if (allTheColumnsAreBeingDeleted) {
			return true;
		}

		log.debug("selectConditionValues = " + selectConditionValues);

		String selectSQLString =
				new SelectStatementBuilder(table, database, subjectValues
						.keySet(), selectConditionValues).getSQLStatement()
						+ " LIMIT 1";

		log.debug("select sqlString = " + selectSQLString);

		try {
			Connection connection = database.connection();
			Statement selectStatement = connection.createStatement();
			ResultSet resultSet = selectStatement.executeQuery(selectSQLString);
			return resultSet.next();

		} catch (SQLException exceptionFromQuery) {
			throw new D2RQException(exceptionFromQuery.getMessage() + ": "
					+ selectSQLString);
		}
	}

	private void verifyNonNullablesAreNotDeleted(ConnectedDB database,
		RelationName table, Map<Attribute, String> subjectValues,
		Map<Attribute, String> objectValues, 
		DatabaseSchemaInspector inspector) {
		
		for (Attribute attribute : objectValues.keySet()) {
			if (!inspector.isNullable(attribute)) {
				if (doesTheColumnContainsTheValueToBeDeleted(database, table,
						subjectValues, attribute, objectValues
								.get(attribute))) {
					throw new D2RQUpdateException(
							"Unable to set a non nullable" + " attribute "
									+ attribute + " to NULL",
							D2RQUpdateException.DELETE_NOT_NULLABLE_ATTRIBUTE);
				}
			}
		}
	}

	private boolean doesTheColumnContainsTheValueToBeDeleted(
		ConnectedDB database, RelationName table,
		Map<Attribute, String> subjectValues, Attribute attribute,
		String valueToBeDeleted) {
		boolean theAttributeContainsTheValueToBeDeleted = false;

		Map<Attribute, String> selectConditionValues =
				new HashMap<Attribute, String>(subjectValues);
		selectConditionValues.put(attribute, valueToBeDeleted);
		String selectSQLString =
				new SelectStatementBuilder(table, database, subjectValues
						.keySet(), selectConditionValues).getSQLStatement()
						+ " LIMIT 1";

		log.debug("select sqlString = " + selectSQLString);

		try {
			Connection connection = database.connection();
			Statement selectStatement = connection.createStatement();
			ResultSet resultSet = selectStatement.executeQuery(selectSQLString);
			theAttributeContainsTheValueToBeDeleted = resultSet.next();
		} catch (SQLException exceptionFromQuery) {
			throw new D2RQException(exceptionFromQuery.getMessage() + ": "
					+ selectSQLString);
		}
		return theAttributeContainsTheValueToBeDeleted;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeil.ac.technion.cs.d2rqUpdate.AbstractTripleUpdater#
	 * allowEmptySubjectsOrObjectsWhileHandlingJoins()
	 */
	@Override
	protected boolean allowEmptySubjectsOrObjectsWhileHandlingJoins() {
		return false;
	}

}
