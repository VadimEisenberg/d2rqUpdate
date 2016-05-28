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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Triple;

import de.fuberlin.wiwiss.d2rq.algebra.AliasMap;
import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.algebra.TripleRelation;
import de.fuberlin.wiwiss.d2rq.dbschema.DatabaseSchemaInspector;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * The base class for auxiliary classes for adding/deleting triples
 *
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 */
public abstract class TripleUpdater extends AbstractTripleUpdater {
	static Log log = LogFactory.getLog(TripleUpdater.class);
	private final Collection<Triple> triples;
	public TripleUpdater(Collection<Triple> triples, 
			Collection<TripleRelation> propertyBridges) {
		super(propertyBridges);
		this.triples = triples;
		log.debug("triples to update = " + triples);
	}
	
	@Override
	public void execute() {
		Map<SubjectValuesTablePair, Collection<ExtractValuesInfo>> 
			subjectTablePairToTriplesMap = 
				getSubjectTablePair2ExctractedValuesMap();
		
		ExecutionEngine engine = new ExecutionEngine();
		
		Collection<SubjectValuesTablePair> subjectTablePairs =
			subjectTablePairToTriplesMap.keySet();
		
		for (SubjectValuesTablePair subjectTablePair :
			subjectTablePairs) {
			Collection<ExtractValuesInfo> extractedValuesPerSubjectAndTable = 
				subjectTablePairToTriplesMap.get(subjectTablePair);
			executePerSubjectAndTable(engine, 
					extractedValuesPerSubjectAndTable);
		}
		
		engine.execute();
	}
	
	
	Map<SubjectValuesTablePair, Collection<ExtractValuesInfo>> 
		getSubjectTablePair2ExctractedValuesMap() {
		Map<SubjectValuesTablePair, Collection<ExtractValuesInfo>> 
		subjectToTriplesMap =
				new HashMap<SubjectValuesTablePair, 
							Collection<ExtractValuesInfo>>();

		Set<ExtractValuesInfo> extractedValues =
				extractValues(triples, propertyBridges);
		
		extractedValues = splitMultipleTables(extractedValues);

		for (ExtractValuesInfo info : extractedValues) {
			RelationName table = getSingleTable(getMentionedTables(info));
			filterTheSubjectValuesByThePrimaryKeys(info, table);
			SubjectValuesTablePair key =
					new SubjectValuesTablePair(info.getSubjectValues(), table);
			if (!subjectToTriplesMap.containsKey(key)) {
				subjectToTriplesMap.put(key, new HashSet<ExtractValuesInfo>());
			}
			Collection<ExtractValuesInfo> extractedPerSubjectAndTable =
					subjectToTriplesMap.get(key);
			extractedPerSubjectAndTable.add(info);
		}

		return subjectToTriplesMap;
	}

	@SuppressWarnings("unchecked")
	private void filterTheSubjectValuesByThePrimaryKeys(ExtractValuesInfo info,
		RelationName table) {
		DatabaseSchemaInspector inspector =
			new DatabaseSchemaInspector(info.getDatabase());
		// handle aliases
		AliasMap aliases = info.getAliases(); 
		RelationName theAliasedtable = renameTable(table, aliases);
		
		List<Attribute> primaryKeys = 
			inspector.primaryKeyColumns(theAliasedtable);
		
		Set<Attribute> attributes = 
			new HashSet<Attribute>(info.getSubjectValues().keySet());
		
		for (Attribute attribute : attributes) {	
			if (!primaryKeys.contains(attribute)) {
				info.getSubjectValues().remove(attribute);
			}
		}
	}
	

	

	public void executePerSubjectAndTable(ExecutionEngine engine, 
		Collection<ExtractValuesInfo> extractedValuesPerSubjectAndTable) {
		
		if(extractedValuesPerSubjectAndTable.size() < 1) {
			return;
		}
		
		ExtractValuesInfo firstInfo = 
			extractedValuesPerSubjectAndTable.iterator().next();
		
		
		RelationName table = 
			getSingleTable(getMentionedTables(firstInfo));
		
		// handle aliases
		AliasMap aliases = firstInfo.getAliases(); 
		table = renameTable(table, aliases);
		
		ConnectedDB database = firstInfo.getDatabase();
		
		// should be the same since we have the same subject
		log.debug("subjectValues before rename for table = " + table + " : " + 
				firstInfo.getSubjectValues());
		Map<Attribute, String> subjectValues = 
			renameValues(firstInfo.getSubjectValues(),aliases);
	
		Set<ObjectValuesProjectionsPair> objectValuesProjectionsPairs = new
			HashSet<ObjectValuesProjectionsPair>(
					extractedValuesPerSubjectAndTable.size());
		
		for(ExtractValuesInfo info : extractedValuesPerSubjectAndTable) {
			ObjectValuesProjectionsPair pair = 
				new ObjectValuesProjectionsPair(
					renameValues(info.getObjectValues(),aliases),
					renameAttributes(info.getProjections(),aliases, database));
			objectValuesProjectionsPairs.add(pair);
		}
		
		execute(engine, database, table, subjectValues, 
				objectValuesProjectionsPairs);
	}

	/**
	 * @param engine 
	 * @param database
	 * @param table
	 * @param subjectValues
	 * @param objectValuesProjectionsPairs
	 */
	abstract protected void execute(ExecutionEngine engine, 
		ConnectedDB database, RelationName table, 
		Map<Attribute, String> subjectValues, 
		Set<ObjectValuesProjectionsPair> objectValuesProjectionsPairs);

	/**
	 * @param objectValuesProjectionsPairs
	 * @return
	 */
	protected boolean multipleValuesPerAttributeExist(
		Set<ObjectValuesProjectionsPair> objectValuesProjectionsPairs) {
		
		Map<Attribute, String> objectAttributes = 
			new HashMap<Attribute, String>();
		
		for (ObjectValuesProjectionsPair pair : objectValuesProjectionsPairs) {
			Map<Attribute, String> objectValues = pair.getObjectValues();
			for (Map.Entry<Attribute,String> attributeValue : 
				objectValues.entrySet()) {
				Attribute attribute = attributeValue.getKey();
				String value = attributeValue.getValue();
				if (objectAttributes.containsKey(attribute)) {
					if (!objectAttributes.get(attribute).equals(value)) {
						return true;
					}
					continue;
				}
				objectAttributes.put(attribute, value);
				
			}
		}
			
		return false;
	}

}