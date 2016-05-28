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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Triple;

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
public class TripleAdder extends TripleUpdater {
	static Log log = LogFactory.getLog(TripleAdder.class);

	public TripleAdder(Collection<Triple> triples,
			Collection<TripleRelation> propertyBrdidges) {
		super(triples, propertyBrdidges);
	}

	@Override
	protected void execute(ExecutionEngine engine, 
		ConnectedDB database, RelationName table,
		Map<Attribute, String> subjectValues, 
		Set<ObjectValuesProjectionsPair> objectValuesProjectionsPairs) {
		
		log.debug("subjectValues for table = " + table + " : " + subjectValues);
		log.debug("objectValuesProjectionsPairs = " + objectValuesProjectionsPairs);
		DatabaseSchemaInspector inspector =
				new DatabaseSchemaInspector(database);

		boolean tableHasPrimaryKeys =
				!inspector.primaryKeyColumns(table).isEmpty();
		
		log.debug("the primary keys of table " + table + " are " + 
				inspector.primaryKeyColumns(table));
		
		log.debug("the table " + table + " is link? " + 
				inspector.isLinkTable(table));
		
		if (!tableHasPrimaryKeys) {
			throw new D2RQUpdateException(table.toString() + 
					" has no primary keys - update not yet implemented", 
					D2RQUpdateException.NOT_IMPLEMENTED);
		}

		Connection connection = database.connection();
		
		
		if (multipleValuesPerAttributeExist(objectValuesProjectionsPairs)) {
			for (ObjectValuesProjectionsPair pair : 
				objectValuesProjectionsPairs) {
				Map<Attribute, String> objectValues = pair.getObjectValues();
				executePerSubjectAndObjectValues(engine, database, table,
						subjectValues, connection, objectValues);
			}
		}
		else {
			executePerSubjectAndObjectValues(engine, database, table,
					subjectValues, connection, 
					getObjectValues(objectValuesProjectionsPairs));
		}
	}

	private void executePerSubjectAndObjectValues(ExecutionEngine engine,
		ConnectedDB database, RelationName table,
		Map<Attribute, String> subjectValues, Connection connection,
		Map<Attribute, String> objectValues) {
		
		Map<Attribute, String> values = 
			new HashMap<Attribute, String>(subjectValues);
		values.putAll(objectValues);

		if (!doesCorrespondingRowAlreadyExist(database, table, values,
				connection)) {
			insertNewRow(engine, database, table, values, subjectValues,
					connection);
			return;
		}

		updateExistingRow(engine, database, table, subjectValues, objectValues,
				connection);
	}

	private void updateExistingRow(ExecutionEngine engine, ConnectedDB database, 
		RelationName table, Map<Attribute, String> subjectValues, 
		Map<Attribute, String> objectValues, Connection connection) {
		Map<Attribute, List<String>> whereValues =
				convertMap2Scalar_2_Map2List(subjectValues);

		for (Attribute attribute : objectValues.keySet()) {
			whereValues.put(attribute, Arrays.asList(new String[] { "NULL",
					objectValues.get(attribute) }));
		}

		String sqlString =
				new UpdateStatementBuilder(table, database, objectValues,
						whereValues).getSQLStatement();
		log.debug("update sqlString = " + sqlString);

		engine.add(new UpdateStatement(database, table,
				objectValues, sqlString, 1,
				UpdateStatement.Type.UPDATE_TO_NON_NULL_VALUE,subjectValues));
	}
	

	

	/* (non-Javadoc)
	 * @see il.ac.technion.cs.d2rqUpdate.AbstractTripleUpdater#allowEmptySubjectsOrObjectsWhileHandlingJoins()
	 */
	@Override
	protected boolean allowEmptySubjectsOrObjectsWhileHandlingJoins() {
		return true;
	}
}
