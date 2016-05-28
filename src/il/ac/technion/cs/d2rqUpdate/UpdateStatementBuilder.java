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
import java.util.List;
import java.util.Map;

import de.fuberlin.wiwiss.d2rq.algebra.Attribute;
import de.fuberlin.wiwiss.d2rq.algebra.RelationName;
import de.fuberlin.wiwiss.d2rq.sql.ConnectedDB;

/**
 * Builds UPDATE SQL statement a given relation, database, update values and
 * condition values
 * 
 * @author Vadim Eisenberg <Vadim.Eisenberg@gmail.com>
 */
public class UpdateStatementBuilder extends StatementBuilder {
	private final Map<Attribute, String> updateValues;
	private final Map<Attribute, List<String>> conditionValues;
	
	
	public UpdateStatementBuilder(RelationName table,
								  ConnectedDB database, 
								  Map<Attribute, String> objectValuesToAdd, 
								  Map<Attribute, List<String>> whereValues) {
		super(table,database);
		this.updateValues = objectValuesToAdd;
		this.conditionValues = whereValues;
		
	}
	
	@Override
	public String getSQLStatement() {		
		StringBuffer result = new StringBuffer("UPDATE ");
		
		result.append(quoteRelationName(getTable()));
		
		result.append(" SET ");
		appendSeparatedAssignments(result, updateValues, ",", false);
		
		result.append(" WHERE ");
		Separator separator = new Separator("AND");
		
		for(Attribute attribute : conditionValues.keySet()) {
			result.append(separator);
			result.append(" ( ");
			appendSeparatedEqualities(result, attribute, 
					conditionValues.get(attribute), "OR", true);
			result.append(" ) ");
		}
		 
		return result.toString();
	}
}
